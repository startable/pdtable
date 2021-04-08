from pdtable.table_origin import LocationFolder
from pdtable.store import BlockIterator, BlockType
from typing import (
    Protocol,
    Iterator,
    Any,
    NamedTuple,
    Iterable,
    Optional,
    Set,
    Tuple,
    Callable,
    List,
)
from abc import abstractmethod, abstractstaticmethod
import logging, time
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
import sys, os, subprocess, logging, re
import datetime

from ..table_origin import InputIssueTracker, LocationFile, LocationBlock, LoadItem, NullInputIssueTracker, TableOrigin, InputError

logger = logging.getLogger(__name__)


class LoadError(Exception):
    pass


# Todo: integrate properly with origin
class LoadOrchestrator(Protocol):
    @abstractmethod
    def add_load_item(self, w: LoadItem):
        pass

    @property
    @abstractmethod
    def issue_tracker(self) -> InputIssueTracker:
        pass

class Loader(Protocol):
    @abstractmethod
    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        pass


### FilesystemLoder et al.


class FilesystemLocationFile(LocationFile):
    def __init__(
        self, local_path: Path, load_specification: Optional[LoadItem] = None, stat_result=None
    ) -> None:
        self._local_path = local_path
        self._load_specification = load_specification or LoadItem(
            specification=str(local_path), source=None
        )
        self._stat_result = stat_result

    @property
    def local_path(self):
        return self._local_path

    @property
    def load_specification(self) -> LoadItem:
        return self._load_specification

    def get_stat_result(self, cached=True):
        if (not cached) or self._stat_result is None:
            self._stat_result = self.local_path.stat()
        return self._stat_result

    def get_mtime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.get_stat_result().st_mtime)

    @property
    def load_identifier(self) -> str:
        name_part = str(self.local_path.absolute())
        mtime = self.get_mtime()
        if mtime:
            return name_part + "@" + mtime.isoformat(timespec="seconds")
        return name_part

    @property
    def interactive_identifier(self) -> str:
        return str(self.local_path)

    def get_interactive_identifier(
        self, sheet: Optional[str] = None, row: Optional[int] = None
    ) -> str:
        if sheet is None:
            loc = f"Row {row}"
        else:
            loc = f"'{sheet}'!A{row}"
        return f"{loc} of '{self.interactive_identifier}'"

    def interactive_uri(
        self,
        sheet: Optional[str] = None,
        row: Optional[int] = None,
        read_only: Optional[bool] = False,
    ) -> str:
        file_uri = self.local_path.as_uri()
        if sheet is None and row is None:
            return file_uri
        if sheet is None:
            sheet = "Sheet1"
        row_mark = f"!A{row}" if row is not None else ""
        return file_uri + f"#'{sheet}'{row_mark}"


class LoadResolver(Protocol):
    @abstractmethod
    def resolve(spec_without_protocol) -> Tuple[bool, LocationFile]:
        pass


Reader = Callable[[LocationFile, LoadOrchestrator], BlockIterator]

_ABSOLUTE_PATH = re.compile(r"/|\\")


@dataclass(frozen=True)
class FilesystemLoader(Loader):
    """
    A Loader implementation for local filesystem loads

    Implementation of ``***include``-directive
    ------------------------------------------
    Each row in an include directive correspond to a load item. 
    Paths are resolved as follows:
    - relative paths are resolved relative to the file they are specified in
    - absolute paths are resolved relative to the root folder



    ``file_name_pattern``
    ---------------------
    When a directory is a directory, files matching this pattern will be loaded.


    ``ignore_protocol``
    -------------------
    ``specification`` field in ``LoadItem`` instances may optionally include protocol-specification.
    This protocol should match value of ``ignore_protocol``, which defaults to ``file:``. Set value
    of of ``ignore_protocol`` to ``None`` to not accept any protocol in specificaton string.
    """

    file_reader: Reader
    root_folder: Path
    file_name_pattern: re.Pattern = field(
        default_factory=lambda: re.compile(r"^(input|setup)_.*\.(csv|xls|xlsx)$")
    )
    ignore_protocol: Optional[str] = "file:"

    def resolve_load_item_path(self, load_item: LoadItem) -> Path:
        spec = load_item.specification
        if self.ignore_protocol and spec.startswith(self.ignore_protocol):
            spec = spec[len(self.ignore_protocol) :]
        is_absolute = _ABSOLUTE_PATH.match(spec) is not None
        if is_absolute:
            resolved = self.root_folder / spec[1:]
        else:
            if load_item.source is None or load_item.source.local_folder_path is None:
                raise LoadError("Cannot load location relative to source with no local folder path")
            resolved = load_item.source.local_folder_path / spec
        resolved = resolved.resolve()
        try:
            resolved_relative = resolved.relative_to(self.root_folder)
        except ValueError as e:
            raise LoadError(f"Load item {resolved} is outside load root folder: {self.root_folder}")

        return resolved

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        try:
            full_path = self.resolve_load_item_path(load_item)
        except LoadError as e:
            # tracker may raise new exception if so configured
            orchestrator.issue_tracker.add_error(e, load_item=load_item)
            return

        if full_path.is_dir():
            src = LocationFolder(local_folder_path=full_path, load_specification=load_item)
            yield from self.load_folder(src, orchestrator)
        else:
            src = FilesystemLocationFile(local_path=full_path, load_specification=load_item)
            yield from self.load_file(src, orchestrator)

    def load_folder(
        self, location: LocationFolder, orchestrator: LoadOrchestrator
    ) -> BlockIterator:
        for p in location.local_folder_path.iterdir():
            if not self.file_name_pattern.match(p.name):
                continue
            logger.debug(f"Including file '{p}' from '{location}'")
            orchestrator.add_load_item(LoadItem(specification=p.name, source=location))
        return
        yield  # to ensure this is a generator

    def load_file(self, location: LocationFile, orchestrator: LoadOrchestrator) -> BlockIterator:
        if location.local_path is None:
            raise ValueError("FilesystemLoader only supports local files")
        for block_type, value in self.file_reader(location, orchestrator):
            if block_type == BlockType.DIRECTIVE and value.name == "include":
                source = value.origin.input_location
                for line in value.lines:
                    logger.debug(f"Adding include '{line}' from {source.interactive_identifier}")
                    orchestrator.add_load_item(LoadItem(specification=line, source=source))
            else:
                yield block_type, value


### SimpleOrchestrator


def load_all(roots: List[LoadItem], loader: Loader, issue_tracker: InputIssueTracker = None):
    class Orchestrator:
        def __init__(self, roots, issue_tracker):
            self.load_items = roots
            self.issue_tracker = issue_tracker

        def add_load_item(self, item):
            self.load_items.append(item)

    orch = Orchestrator(
        roots, 
        issue_tracker if issue_tracker is not None else NullInputIssueTracker
    )
    while orch.load_items:
        yield from loader.load(orch.load_items.pop(), orch)

    if not orch.issue_tracker.is_ok:
        raise InputError(f"Load issues: {orch.issue_tracker}")

