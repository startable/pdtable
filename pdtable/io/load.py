"""
The ``load``-module is responsible for reading input sets

Compared to the single-file reader functions, the load functionality adds:

  - Support of `***include` directives
  - Support for multiple input sources (records systems, blobs, etc)
  - Support of tracking input origin 


Example of loading all files in a given folder::

    inputs = load_files(['/'], root_folder=root_folder, csv_sep=';')
    bundle =  TableBundle(inputs)

The Table objects returned by load will have an `origin` attribute describing their load herritage. 
An example of how this can be used is the `make_location_trees`-function, which builds a tree-representation of the load process::

    location_trees = make_location_trees(iter(bundle))
    print('\n'.join(str(n) for n in location_trees))
"""

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
    Dict,
)


from .csv import read_csv
from .excel import read_excel
from pdtable import Table
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
    LoadLocation,
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
                raise LoadError(
                    f"Load item {resolved} is outside load root folder: {self.root_folder}"
                )

        return resolved

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        try:
            full_path = self.resolve_load_item_path(load_item)
        except LoadError as e:
            # tracker may raise new exception if so configured
            orchestrator.issue_tracker.add_error(e, load_item=load_item)
            return

        if full_path.is_dir():
            src = LocationFolder(
                local_folder_path=full_path,
                load_specification=load_item,
                root_folder=self.root_folder,
            )
            yield from self.load_folder(src, orchestrator)
        else:
            src = FilesystemLocationFile(
                local_path=full_path, load_specification=load_item, root_folder=self.root_folder
            )
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
    sheet_name_pattern: re.Pattern

    def __init__(self, sheet_name_pattern: re.Pattern, csv_sep: Optional[str] = None):
        self.csv_sep = csv_sep
        self.sheet_name_pattern = sheet_name_pattern

    def read(self, location_file: LocationFile, orchestrator: LoadOrchestrator) -> BlockIterator:
        path = location_file.get_local_path()
        ext = path.suffix.lower()
        if ext == ".csv":
            location_sheet = location_file.make_location_sheet()
            yield from read_csv(
                path,
                sep=self.csv_sep,
                location_sheet=location_sheet,
                issue_tracker=orchestrator.issue_tracker,
            )
        elif ext == ".xlsx":
            yield from read_excel(
                path,
                sheet_name_pattern=self.sheet_name_pattern,
                location_file=location_file,
                issue_tracker=orchestrator.issue_tracker,
            )
        else:
            raise ValueError(f"Unsupported file extension: {ext}")


def load_files(
    files: Iterable[str],
    csv_sep: Optional[str] = None,
    root_folder: Optional[Path] = None,
    issue_tracker: Optional[InputIssueTracker] = None,
    file_name_pattern: re.Pattern = None,
    sheet_name_pattern: re.Pattern = None,
):
    """
    Load one or more files, respecting include directive

    Example: load all files matching `input_*`, `setup_*` in `input_folder`::

        load_files(['/'], root_folder=input_folder, csv_sep=';')


    This function is a thin wrapper around the current best-practice loader
    and the backing implementation will be updated when best practice changes.

    args:
        csv_sep: 
            Optional; Separator for csv files. Defaults to pdtable.CSV_SEP
        root_folder:
            Optional; Root folder for resolving absolute imports.
            If defined, no loads outside root folder are allowed.
        file_name_pattern:
            Compiled regexp to match filenames against when reading a whole 
            directory. Defaults to `^(input|setup)_.*\\.(csv|xlsx)$`
        sheet_name_pattern:
            Compiled regexp to match sheet names against when reading a workbook
            directory. Defaults to `^(input|setup)`

    yields:
        
    
    """
    if file_name_pattern is None:
        file_name_pattern = re.compile(r"^(input|setup)_.*\.(csv|xlsx)$")
    if sheet_name_pattern is None:
        sheet_name_pattern = re.compile(r"^(input|setup)")
    reader = FileReader(csv_sep=csv_sep, sheet_name_pattern=sheet_name_pattern)
    yield from loader_load_all(
        roots=[LoadItem(str(f), source=None) for f in files],
        loader=FilesystemLoader(
            file_reader=reader.read, root_folder=root_folder, file_name_pattern=file_name_pattern,
        ),
        issue_tracker=issue_tracker,
    )


@dataclass
class LocationTreeNode:
    """
    A tree structure with each node mapping to a LoadLocation

    Leaf nodes will correspond to LocationBlock objects with the corresponding
    table available as ``.table``.
    """

    location: LoadLocation
    table: Optional[Table] = None
    parent: Optional["LocationTreeNode"] = None
    children: List["LocationTreeNode"] = field(default_factory=list)

    def add_child(self, child: "LocationTreeNode"):
        self.children.append(child)
        child.parent = self

    def visit_all(self, visitor, level: int = 0) -> Iterable:
        yield visitor(level, self)
        for child in self.children:
            yield from child.visit_all(level=level + 1, visitor=visitor)

    def __str__(self) -> str:
        def str_visitor(level, node):
            if node.table is not None:
                return f"{'  '*level}**{node.table.name}"
            else:
                return (
                    f"{'  '*level}{node.location.interactive_identifier if node.location else ''}"
                )

        return "\n".join(self.visit_all(visitor=str_visitor))


def make_location_trees(tables: Iterable[Table]):
    """
    Return a graph representation of the origins for given tables

    Graph is a collection of trees:
        B LocationFile
          L LocationBlock # table
          B LocationBlock # include
            B LocationFile
                    ....
                L LocationBlock  # table
        B LocationFolder
        B LocationFile
            L LocationBlock

    The implementation relies on ``load_identifier`` to be unique for a LocationFile object.

    Return:
        List of root origins
    """
    # Implementation note:
    # To extend to non-compliant readers that do not implement unique load identifiers,
    # a check for object identify could be added.

    # Create leaf nodes and immediate parents, track by load_identifier
    # Since load_identifier for LocationBlock just refers to parent file,
    # individual tables are not registered
    buf: Dict[str, LocationTreeNode] = dict()

    def register_node(location: LoadLocation, child: LocationTreeNode):
        """
        Add location and ancestors to buf by load_identifier

        Should not be called for individual LocationBlock
        """
        if location.load_identifier in buf:
            buf[location.load_identifier].add_child(child)
            return
        new_node = LocationTreeNode(location=location)
        new_node.add_child(child)
        buf[location.load_identifier] = new_node
        source = location.load_specification.source
        if source is not None:
            register_node(source, child=new_node)

    for t in tables:
        if t.metadata.origin is None:
            raise ValueError("Table object without origin not supported for `make_origin_trees`", t)
        location = t.metadata.origin.input_location
        if location is None:
            if t.metadata.origin.parents:
                raise NotImplementedError("Support for non-loaded tables not implemented")
            else:
                raise ValueError("Missing input_location for table", t)
        leaf = LocationTreeNode(location=location, table=t)
        register_node(location.file, child=leaf)

    # return nodes without parent as roots:
    return [v for v in buf.values() if v.parent == None]

