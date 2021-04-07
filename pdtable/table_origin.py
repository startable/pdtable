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
    Dict,
)
from abc import abstractmethod, abstractproperty, abstractstaticmethod
import logging, time
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
import sys, os, subprocess, datetime, html, random, base64


class LoadLocation(Protocol):
    @property
    @abstractmethod
    def local_folder_path(self) -> Optional[Path]:
        """
        Used for resolving relative imports
        """
        pass

    @property
    @abstractmethod
    def load_specification(self) -> "LoadItem":
        pass

    @property
    @abstractmethod
    def load_identifier(self) -> str:
        pass

    @abstractmethod
    def interactive_open(self, read_only: bool = False):
        pass


class LoadItem(NamedTuple):
    specification: str
    source: Optional[LoadLocation]

    @property
    def source_identifier(self) -> str:
        return "<root>" if self.source is None else self.source.load_identifier

    def load_history(self) -> Iterator["LoadItem"]:
        """
        Return the load tree leading up to this load item

        Typical use:
        ```
        '\n'.join(f'included as "{li.spec} from "{li.source_identifier}"' for li in load_specification.load_history())
        ```

        Example text representation
        ```
        included as "doreco:input_foo_DOR12345" from "/mp/input_include.csv@2021-01-02T1233" row 27
        included as "input_include.csv" from "/mp"
        included as "mp/" from "/input_all.csv@123123123" row 12
        included as "input_all.csv" from "/"
        included as "/" from "<root>"
        ```
        """
        yield self
        if self.source:
            yield from self.source.load_specification.load_history()

    def __str__(self) -> str:
        return ";".join(
            f'included as "{li.specification} from "{li.source_identifier}"'
            for li in self.load_history()
        )


def interactive_open_uri(uri):
    """
    A cross-platform functionality for launching tool by URI
    """
    if sys.platform == "win32":
        os.startfile(uri)
    else:
        opener = "open" if sys.platform == "darwin" else "xdg-open"
        subprocess.call([opener, uri])


class LocationFile(Protocol):
    """
    Represents a traceable load entity

    The load entity could be a file, a blob, an http response. The LocationFile instance
    should hold enough information to uniquely identify (and if possible allow recreation)
    of the block stream resulting of loading this entity.
    """

    @property
    @abstractmethod
    def load_specification(self) -> LoadItem:
        """
        The specification passed to Loader instance as LoadItem to retrieve this file

        The specification may not uniquely identify the file, as i may include partial
        record specifications (e.g. "use latest") resolved at load time.
        """
        pass

    @property
    @abstractmethod
    def load_identifier(self) -> str:
        """
        Unique identifier of loaded item
        
        Must be unique to allow import loop detection and input caching.
        For local files, this could be absolute path and modification time.
        """
        pass

    @property
    @abstractmethod
    def local_path(self) -> Optional[Path]:
        pass

    @property
    def local_folder_path(self) -> Optional[Path]:
        return None if self.local_path is None else self.local_path.parent

    @abstractmethod
    def interactive_uri(
        self, sheet: Optional[str] = None, row: Optional[int] = None, read_only=True
    ) -> Optional[str]:
        pass

    def interactive_open(
        self, sheet: Optional[str] = None, row: Optional[int] = None, read_only=True
    ):
        uri = self.interactive_uri(sheet, row, read_only)
        interactive_open_uri(uri)

    def get_interactive_identifier(
        self, sheet: Optional[str] = None, row: Optional[int] = None
    ) -> str:
        """
        Defaults to load identifier, but may be replaced with replacement better suited for interactive use
        """
        s_loc = "" if sheet is None else f" Sheet '{sheet}'"
        r_loc = "" if row is None else f" Row {row}"

        return self.load_identifier + s_loc + r_loc

    @property
    def interactive_identifier(self) -> str:
        return self.get_interactive_identifier()

    def get_local_path(self) -> Path:
        """
        Always return path to local instance

        Will write contents to local storage if not available.
        """
        if self.local_path is not None:
            return self.local_path
        raise NotImplementedError("Automatic download not implemented")

    def make_location_sheet(self, sheet_name: Optional[str] = None):
        return LocationSheet(file=self, sheet_name=sheet_name)


def _random_id() -> str:
    return base64.b32encode(os.urandom(5*2)).decode('ascii')

@dataclass(frozen=True)
class NullLocationFile:
    """
    Null-implementation of LocationFile
    """
    load_specification: LoadItem = field(default_factory = lambda: LoadItem("Unknown", source=None))
    load_identifier: str = field(default_factory = lambda: f"<Random load identifier - {_random_id()}>")
    local_path: Optional[Path] = None

    def interactive_uri(
        self, sheet: Optional[str] = None, row: Optional[int] = None, read_only=True
    ) -> Optional[str]:
        return None
        

class LocationFolder(NamedTuple):
    local_folder_path: Path
    load_specification: LoadItem

    @property
    def load_identifier(self) -> str:
        return str(self.local_folder_path)

    @property
    def interactive_identifier(self) -> str:
        return self.load_identifier

    def interactive_uri(self, read_only=False) -> str:
        return self.local_folder_path.as_uri()

    def interactive_open(self):
        interactive_open_uri(self.interactive_uri())

    @classmethod
    def make_location_folder(
        cls, local_folder_path: Path, load_specification: LoadItem = None
    ) -> "LocationFolder":
        if load_specification is None:
            load_specification = LoadItem(str(local_folder_path), source=None)
        return cls(local_folder_path=local_folder_path, load_specification=load_specification)


@dataclass(frozen=True)  # to allow empty dict default
class LocationSheet:
    file: LocationFile
    sheet_name: Optional[str]
    sheet_metadata: Dict[str, str] = field(default_factory=dict)

    def make_location_block(self, row: int):
        return LocationBlock(sheet=self, row=row)

class LocationBlock(NamedTuple):
    sheet: LocationSheet
    row: int

    @property
    def file(self) -> LocationFile:
        return self.sheet.file

    @property
    def sheet_name(self) -> Optional[str]:
        return self.sheet.sheet_name

    @property
    def local_folder_path(self) -> Optional[Path]:
        return self.file.local_folder_path

    @property
    def load_identifier(self) -> str:
        return self.file.load_identifier

    @property
    def interactive_identifier(self) -> str:
        return self.file.get_interactive_identifier(sheet=self.sheet_name, row=self.row)

    def interactive_uri(self, read_only: bool = False):
        return self.file.interactive_uri(sheet=self.sheet_name, row=self.row, read_only=read_only)

    def interactive_open(self, read_only: bool = False):
        interactive_open_uri(self.interactive_uri(read_only=read_only))


@dataclass(frozen=True)
class TableOrigin:
    """
    A TableOrigin instance defines the source of a Table instance.

    The source may either be a loaded input table or an operation combining multiple
    parents (represented by TableOrigin instances) into a derived table, so that each
    ``TableOrigin``-instance is the root of a tree of ``TableOrigin`` instances.

    Each node in the tree is either a _leaf_, corresponding to a loaded input, in which 
    case only ``input_location`` is defined, or a _branch_, corresponding to a derived
    table, in which case only ``parents`` and ``operation`` are defined.
    
    For an integrated representation of this information, an application should traverse
    the tree and provide a representation in the most convenient form. For an example, 
    see ``table_origin_as_html`` or ``table_origin_as_str``.
    """

    input_location: Optional[LocationBlock] = None
    parents: Iterable["TableOrigin"] = ()
    operation: Optional[str] = None

    def _post_init_(self):
        if self.operation is None:
            # leaf
            if self.parents or self.input_location is None:
                raise ValueError("For TableOrigin leaf node, only input_location must be defined")
        else:
            # branch
            if self.input_location is not None or not self.parents:
                raise ValueError(
                    "For TableOrigin branch node, only operation and parents should be defined"
                )

    @property
    def is_leaf(self):
        return self.operation is None

    def get_input_ancestors(self) -> Iterator[LocationBlock]:
        """
        Return iterator over the input-location of all non-derived ancestors
        """
        if self.is_leaf:
            if self.input_location:
                yield self.input_location
            else:
                raise ValueError("Inconsistent state of TableOrigin")
        else:
            for p in self.parents:
                yield from p.get_input_ancestors()

    def __str__(self):
        return table_origin_as_str(self)

    def _repr_html_(self) -> str:
        return table_origin_as_html(self)


def table_origin_as_html(tt: TableOrigin):
    def visit(tt):
        return visit_branch(tt) if tt.input_location is None else visit_leaf(tt)

    def visit_leaf(tt):
        loc = tt.input_location
        return (
            f"""<a href="{loc.interactive_uri()}" class="input-table-origin">"""
            f"""{html.escape(loc.interactive_identifier)}</a>"""
        )

    def visit_branch(tt):
        return (
            f"""<div class="derived-table-origin"><span>{html.escape(tt.operation)}</span><ul>"""
            + "\n".join(f"<li>{visit(p)}</li>" for p in tt.parents)
            + """</ul></div>"""
        )

    return visit(tt)


def table_origin_as_str(tt: TableOrigin):
    buf: List[Tuple[int, str]] = []

    def visit(tt, lev):
        return visit_branch(tt, lev) if tt.input_location is None else visit_leaf(tt, lev)

    def visit_leaf(tt, lev):
        buf.append((lev, tt.input_location.interactive_identifier))

    def visit_branch(tt, lev):
        buf.append((lev, f"Derived via {tt.operation} from:"))
        for p in tt.parents:
            visit(p, lev + 1)

    visit(tt, 0)
    return "\n".join("  " * lev + s for lev, s in buf)

