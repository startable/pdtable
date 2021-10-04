from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
import logging
import re
from typing import (
    NamedTuple,
)

from pdtable.store import BlockIterator, BlockType
from pdtable.table_origin import (
    FilesystemLocationFile,
    LoadItem,
    LocationFile,
    interactive_open_uri,
)

# from ..reader import FileReader
from ..csv import read_csv
from ..excel import read_excel
from ._protocol import (
    Loader,
    LoadError,
    LoadOrchestrator,
)


logger = logging.getLogger(__name__)


class FileReader:
    """
    `FileReader` is a wrapper that integrates the single-file loaders with the `load`-module
    """

    csv_sep: None | str
    sheet_name_pattern: re.Pattern

    def __init__(self, sheet_name_pattern: re.Pattern, csv_sep: None | str = None):
        self.csv_sep = csv_sep
        self.sheet_name_pattern = sheet_name_pattern

    @property
    def supported_extensions(self) -> list[str]:
        return ["csv", "xlsx"]

    @property
    def supported_filename_pattern(self) -> re.Pattern:
        """
        A regex pattern matching supported file extensions

        Example value: ``re.compile(r".*\\.(xlsx|csv)$")``
        """
        return re.compile(r".*\.({})$".format("|".join(self.supported_extensions)), re.IGNORECASE)

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


class LocationFolder(NamedTuple):
    """
    An implementation of the LoadLocation interface to describe a folder
    """

    local_folder_path: Path
    load_specification: LoadItem
    root_folder: None | Path = None

    @property
    def load_identifier(self) -> str:
        return str(self.local_folder_path)

    @property
    def interactive_identifier(self) -> str:
        if self.root_folder is None:
            return self.load_identifier
        rel_path = self.local_folder_path.relative_to(self.root_folder)
        if rel_path == Path("."):
            return f"<root_folder: {self.root_folder}>"
        else:
            return str(rel_path)

    def interactive_uri(self, read_only=False) -> str:
        return self.local_folder_path.as_uri()

    def interactive_open(self):
        interactive_open_uri(self.interactive_uri())

    @classmethod
    def make_location_folder(
        cls,
        local_folder_path: Path,
        load_specification: LoadItem = None,
        root_folder: None | Path = None,
    ) -> "LocationFolder":
        if load_specification is None:
            load_specification = LoadItem(str(local_folder_path), source=None)
        return cls(
            local_folder_path=local_folder_path,
            load_specification=load_specification,
            root_folder=root_folder,
        )


@dataclass(frozen=True)
class ProtocolLoader(Loader):
    """
    A composable Loader implementation that allows multi-protocol support

    ``default_protocol``
    -------------------
    If the ``specification`` in a ``LoadItem`` instance does not include a known protocol,
    the protocol is assumed to be ``default_protocol``.
    """

    protocol_handlers: dict[str, Loader]
    default_protocol: str = "file"

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        # Do not try to parse protocol, as this may cause issues on windows where file paths may
        # look like protocols (e.g. C:...)
        spec = load_item.specification.lower()
        handler = next(
            (h for p, h in self.protocol_handlers.items() if spec.startswith(p + ":")),
            self.protocol_handlers[self.default_protocol],
        )
        yield from handler.load(load_item=load_item, orchestrator=orchestrator)


@dataclass(frozen=True)
class IncludeLoader(Loader):
    """
    A composable Loader implementation that implements an ``include``-directive

    Each row in an include directive correspond to a load item.
    The ``FileSystemLoader`` will resolve relative file paths relative to the
    folder containing the file with the include-directive.
    """

    loader: Loader

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        for block_type, value in self.loader.load(load_item, orchestrator):
            if block_type == BlockType.DIRECTIVE and value.name == "include":
                source = value.origin.input_location
                for line in value.lines:
                    logger.debug(f"Adding include '{line}' from {source.interactive_identifier}")
                    orchestrator.add_load_item(LoadItem(specification=line, source=source))
            else:
                yield block_type, value


_LEADING_SLASH = re.compile(r"/|\\")


@dataclass
class FileSystemLoader:
    """
    A loader protocol for local filesystem loads

    Paths are resolved as follows:
    - relative paths are resolved relative to the file/folder they originate in
    - absolute paths are resolved relative to the root folder

    ``file_name_pattern``
    ---------------------
    When a directory is a directory, files matching this pattern will be loaded.

    ``root_folder``
    ---------------
    If a ``root_folder`` is specified, all path specifications with a leading ``/`` or ``\\`` are
    resolved relative to the root folder, and all paths must be inside the root folder.

    ``ignore_protocol``
    -------------------
    When used together with `ProtocolLoader`, specification strings may include protocols.
    The ``ignore_protocol`` specifies a protocol value that should be ignored.
    All other protocol values in specification will assumed to be part of the filesystem path,
    likely leading to errors.
    Value of ``ignore_protcol`` must be lower case and include trailing ``:``.
    Default value is ``file:``.
    """

    file_reader: FileReader
    file_name_pattern: re.Pattern
    root_folder: None | Path = None
    ignore_protocol: str = "file:"

    def _resolve_load_item_path(self, load_item: LoadItem) -> Path:
        spec = load_item.specification

        if self.ignore_protocol and spec.lower().startswith(self.ignore_protocol):
            spec = spec[len(self.ignore_protocol) :]

        leading_slash = _LEADING_SLASH.match(spec) is not None
        if leading_slash:
            if self.root_folder is None:
                resolved = Path(spec)
                if not resolved.is_absolute():
                    raise LoadError(
                        "Include with leading slash must be an absolute path when root_folder "
                        "not defined"
                    )
            else:
                resolved = self.root_folder / spec[1:]
        else:
            if load_item.source is None or load_item.source.local_folder_path is None:
                raise LoadError("Cannot load location relative to source with no local folder path")
            resolved = load_item.source.local_folder_path / spec
        resolved = resolved.resolve()

        if self.root_folder is not None:
            # Check that resolved folder is inside root
            try:
                resolved.relative_to(self.root_folder)
            except ValueError:
                raise LoadError(
                    f"Load item {resolved} is outside load root folder: {self.root_folder}"
                )
        return resolved

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        try:
            full_path = self._resolve_load_item_path(load_item)
        except LoadError as e:
            # tracker may raise new exception if so configured
            orchestrator.issue_tracker.add_error(e, load_item=load_item)
            return

        if full_path.is_dir():
            self._load_folder(
                LocationFolder(
                    local_folder_path=full_path,
                    load_specification=load_item,
                    root_folder=self.root_folder,
                ),
                orchestrator,
            )
        else:
            yield from self.file_reader.read(
                FilesystemLocationFile(
                    local_path=full_path, load_specification=load_item, root_folder=self.root_folder
                ),
                orchestrator,
            )

    def _load_folder(
        self, location: LocationFolder, orchestrator: LoadOrchestrator
    ) -> BlockIterator:
        for p in location.local_folder_path.iterdir():
            if not self.file_name_pattern.match(p.name):
                continue
            logger.debug(f"Including file '{p}' from '{location}'")
            orchestrator.add_load_item(LoadItem(specification=p.name, source=location))


def make_loader(
    *,
    csv_sep: None | str = None,
    sheet_name_pattern: re.Pattern = None,
    file_reader: FileReader = None,
    root_folder: None | str | Path = None,
    file_name_pattern: re.Pattern = None,
    file_name_start_pattern: str = None,
    additional_protocol_loaders: dict[str, Loader] = None,
    allow_include: bool = True,
) -> Loader:
    """
    Make a composite loader

    args:
        csv_sep:
            Optional; Separator for csv files. Defaults to pdtable.CSV_SEP
        sheet_name_pattern:
            Optional; Compiled regexp to match sheet names against when reading a workbook
            directory. Defaults to `.*`. Typical values include `^(input|setup)`
        file_reader:
            Optional; If not supplied, file_reader is instantiated based on `csv_sep` and
            `sheet_name_pattern`. These arguments cannot be provided together with ``file_reader``.
            Reasons to include external reader could include low-level filtering to improve
            performance.
        root_folder:
            Optional; Root folder for resolving absolute imports. If defined, no loads outside
            root folder are allowed. See also `FileSystemLoader`.
        file_name_start_pattern:
            Optional; If provided ``file_name_pattern`` is computed as ``file_name_start_pattern``
            combined with the extension pattern defined by the file reader.
            If not provided a negative lookahead for excel temp files is used `(?!~\\$)`
            Example value `"^(input|setup)_"`.
        file_name_pattern:
            Compiled regexp to match filenames against when reading a whole directory.
            Defaults to extension pattern defined by the file reader.
        allow_include:
            Optional; Default is True; If True, honor the ``include`` directive.
        additional_protocol_loaders:
            Optional; A dict mapping protocol names (lower case, no colon) to ``Loader``-objects
            to handle that protocol. Will be supplemented with a default file protocol.
    """

    # file reader
    if file_reader is None:
        file_reader = FileReader(csv_sep=csv_sep, sheet_name_pattern=sheet_name_pattern)
    elif csv_sep is not None or sheet_name_pattern is not None:
        raise ValueError("csv_sep and sheet_name_pattern cannot be used with file_reader")

    # file loader
    if file_name_pattern is None:
        if file_name_start_pattern is None:
            # negative lookahead to avoid excel tempfiles
            file_name_start_pattern = r"(?!~\$)"
        sfp = file_reader.supported_filename_pattern
        file_name_pattern = re.compile(file_name_start_pattern + sfp.pattern, sfp.flags)
    elif file_name_start_pattern is not None:
        raise ValueError("file_name_start_pattern cannot be used with file_name_pattern")
    if root_folder is not None:
        root_folder = Path(root_folder)
    file_loader = FileSystemLoader(
        file_reader=file_reader, root_folder=root_folder, file_name_pattern=file_name_pattern
    )

    # protocols
    loader = file_loader
    if additional_protocol_loaders is not None:
        loader = ProtocolLoader(
            handlers={
                kv
                for ll in [[("file", file_reader)], additional_protocol_loaders.items()]
                for kv in ll
            },
            default_protocol="file",
        )

    # include directive
    if allow_include:
        loader = IncludeLoader(loader)

    return loader
