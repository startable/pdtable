from pdtable.store import BlockIterator, BlockType
from typing import Protocol, Iterator, Any, NamedTuple, Iterable, Optional, Set, Tuple, Callable
from abc import abstractmethod, abstractstaticmethod
import logging, time
from dataclasses import dataclass, field
from pathlib import Path, PosixPath
import sys, os, subprocess

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
    def load_specification(self) -> LoadItem:
        pass

    @property
    @abstractmethod
    def load_identifier(self) -> str:
        pass


    @abstractmethod
    def open_interactive(self, read_only: bool = False):
        pass


class LoadItem(NamedTuple):
    specification: str
    source: LoadLocation

    @property
    @abstractmethod
    def source_identifier(self) -> str:
        return self.source.load_identifier

    def load_history(self) -> Iterator["LoadItem"]:
        """
        Return the load tree leading up to this load item

        Typical use:
        ```
        '\n'.join(f'included as "{li.spec} from "{li.source_identifier}"' for li in load_specification.load_history())
        ```

        Example text representation
        ```
        included as "doreco:input_foo_DOR12345" from "/mpmp/input_include.csv@2021-01-02T1233" row 27
        included as "input_include.csv" from "/mpmp"
        included as "mpmp/" from "/input_all.csv@123123123" row 12
        included as "input_all.csv" from "/"
        ```
        """
        yield self
        if self.source:
            yield from self.source.load_specification.load_history()

    def __str__(self) -> str:
        return ';'.join(f'included as "{li.specification} from "{li.source_identifier}"' for li in self.load_history())


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
    def file_identifier(self) -> str:
        """
        Unique identifier of loaded item
        
        Must be unique to allow import loop detection and input caching.
        For local files, this could be absolute path and modification time.
        """
        pass

    # TODO: Should this field just be left out?
    @property
    @abstractmethod
    def file_metadata(self) -> Any:
        """
        For use by loader framework
        """
        pass

    @property
    @abstractmethod
    def local_path(self) -> Optional[Path]:
        pass

    @abstractmethod
    def open_interactive(self, sheet: Optional[str]=None, row: Optional[int]=None, read_only=True):
        pass

    @abstractmethod
    def get_local_path(self) -> Path:
        """
        Always return path to local instance

        Will write contents to local storage if not available.
        """
        pass


class LocationFolder(NamedTuple):
    local_folder_path: Path

    @property
    def load_identifier(self) -> str:
        return self.local_folder_path
        
    def open_interactive(self):
        if sys.platform == "win32":
            os.startfile(self.local_folder_path)
        else:
            opener = "open" if sys.platform == "darwin" else "xdg-open"
            subprocess.call([opener, self.local_folder_path])


@dataclass(frozen=True)  # to allow empty dict default
class LocationSheet:
    file: LocationFile
    sheet_name: Optional[str] 
    sheet_metadata: Dict[str, str] = field(default_factory=dict)


class LocationBlock(NamedTuple):
    sheet: LocationSheet
    row: int

    @property
    def sheet_name(self) -> Optional[str]:
        return self.sheet.sheet_name

    @property
    def local_folder_path(self) -> Path:
        return self.file.local_folder_path

    @property
    def load_identifier(self) -> str:
        return self.file.load_identifier()
