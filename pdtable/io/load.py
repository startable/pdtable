from abc import abstractmethod, abstractstaticmethod
import logging, time
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
import sys, os, subprocess, logging, re
import datetime
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
    Union,
)


from .csv import read_csv
from pdtable.store import BlockIterator, BlockType
from pdtable.table_origin import (
    LocationFolder,
    InputIssueTracker,
    LocationFile,
    LocationBlock,
    LoadItem,
    NullInputIssueTracker,
    TableOrigin,
    InputError,
    FilesystemLocationFile,
)

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


Reader = Callable[[LocationFile, LoadOrchestrator], BlockIterator]

### FilesystemLoder et al.

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
    file_name_pattern: re.Pattern
    root_folder: Optional[Path] = None
    ignore_protocol: Optional[str] = "file:"

    def resolve_load_item_path(self, load_item: LoadItem) -> Path:
        spec = load_item.specification
        if self.ignore_protocol and spec.startswith(self.ignore_protocol):
            spec = spec[len(self.ignore_protocol) :]
        leading_slash = _ABSOLUTE_PATH.match(spec) is not None
        if leading_slash:
            if self.root_folder is None:
                raise LoadError("Absolute include not allowed since root folder is not specified")
            resolved = self.root_folder / spec[1:]
        else:
            if load_item.source is None or load_item.source.local_folder_path is None:
                raise LoadError("Cannot load location relative to source with no local folder path")
            resolved = load_item.source.local_folder_path / spec
        resolved = resolved.resolve()

        if self.root_folder is not None:
            # Check that resolved folder is inside root
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


def loader_load_all(roots: List[LoadItem], loader: Loader, issue_tracker: InputIssueTracker = None):
    """
    Load `LoadItem`-objects listed in `roots`, together with any items added by the loader due to ``***include`` directives or similar

    This loader is single threaded. For higher performance, a multi-threaded loader should be used.
    """
    class Orchestrator:
        def __init__(self, roots, issue_tracker):
            self.load_items = roots
            self.issue_tracker = issue_tracker

        def add_load_item(self, item):
            self.load_items.append(item)

    orch = Orchestrator(
        roots, issue_tracker if issue_tracker is not None else NullInputIssueTracker
    )
    while orch.load_items:
        yield from loader.load(orch.load_items.pop(), orch)

    if not orch.issue_tracker.is_ok:
        raise InputError(f"Load issues: {orch.issue_tracker}")


class FileReader:
    csv_sep: Optional[str]

    def __init__(self, csv_sep: Optional[str]=None):
        self.csv_sep = csv_sep

    def read(self, location_file: LocationFile, orchestrator: LoadOrchestrator) -> BlockIterator:
        path = location_file.get_local_path()
        ext = path.suffix.lower()
        if ext=='.csv':
            location_sheet = location_file.make_location_sheet()
            yield from read_csv(
                path, sep=self.csv_sep, 
                location_sheet = location_sheet,
                issue_tracker = orchestrator.issue_tracker,
            )
        elif ext=='.xlsx':
            # TODO: make read_excel `location_file`-aware and call here
            raise NotImplementedError
        else:
            raise ValueError(f"Unsupported file extension: {ext}")


def load_files(
    files: Iterable[str],
    csv_sep: Optional[str]=None,
    root_folder: Optional[Path]=None,
    issue_tracker: Optional[InputIssueTracker]=None,
    file_name_pattern: re.Pattern=None
):
    """
    Load one or more files, respecting include directive

    Example: load all files matching `input_*`, `setup_*` in `input_folder`::

        load_files(['/'], root_folder=input_folder, csv_sep=';')


    args:
        csv_sep: 
            Optional; Separator for csv files. Defaults to pdtable.CSV_SEP
        root_folder:
            Optional; Root folder for resolving absolute imports.
            If defined, no loads outside root folder are allowed.
        file_name_pattern:
            Compiled regexp to match filenames against when reading a whole 
            directory. Defaults to `^(input|setup)_.*\.(csv|xlsx)$`
    
    """    
    if file_name_pattern is None:
        file_name_pattern = re.compile(r"^(input|setup)_.*\.(csv|xlsx)$")
    reader = FileReader(csv_sep=csv_sep)
    yield from loader_load_all(
        roots=[LoadItem(str(f), source=None) for f in files], 
        loader=FilesystemLoader(
            file_reader=reader.read, 
            root_folder=root_folder,
            file_name_pattern=file_name_pattern,
            ),
        issue_tracker=issue_tracker
    )