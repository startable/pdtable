"""Interface to read/write Tables from/to an Excel workbook.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others readers such as 'xlrd' and writers such as 'xlsxwriter'.

openpyxl (and eventually other engines) are not required at install time; only when the functions
requiring them (read_excel() or write_excel()) are called for the first time.

"""
import os
from enum import Enum, auto
from os import PathLike
from pathlib import Path
from typing import Union, Callable, Iterable, BinaryIO, Dict, Optional
import re
import warnings
import logging

from .parsers.blocks import parse_blocks
from .parsers.fixer import ParseFixer
from .. import BlockType, Table
from ..store import BlockIterator
from ..table_origin import FilesystemLocationFile, InputIssueTracker, LocationFile, NullLocationFile

logger = logging.getLogger(__name__)


def read_excel(
    source: Union[str, PathLike, Path],
    *,
    origin: str = None,
    location_file: LocationFile = None,
    sheet_name_pattern: Optional[re.Pattern] = None,
    fixer: ParseFixer = None,
    to: str = "pdtable",
    filter: Callable[[BlockType, str], bool] = None,
    issue_tracker: InputIssueTracker = None,
) -> BlockIterator:
    """Reads StarTable blocks from an Excel workbook.
    # TODO copy most of read_csv() docstring over

    Reads StarTable blocks from an Excel workbook file at the specified path.
    Yields them one at a time as a tuple: (block type, block content)

    Args:
        source:
            Path of workbook to read.

        origin:
            Optional; File origin description/file name as str. May be shadowed by `location_file`.

        location_file:
            Optional; Origin of file as a LocationFile object.
            `location_sheet` takes precedence over `origin`. For file input default
            if input path with `origin` as optional input specification, for stream input
            default is a null context with `origin` as description.
        sheet_name_pattern:
            Optional[re.Pattern] = None;
            If specified, only sheets with name matching pattern will be loaded.
            Matching is done with ``match``, i.e. must match from start of string.


    Yields:
        Tuples of the form (block type, block content)
    """

    source_is_stream = hasattr(source, "read")
    if not source_is_stream:
        source = Path(source)

    # resolve location
    if location_file is None:
        if not source_is_stream:
            location_file = FilesystemLocationFile(local_path=source, load_specification=origin)
        else:
            location_file = NullLocationFile()
    elif origin is not None:
        warnings.warn(
            f"Input 'origin': {origin} is shadowed by input 'location_file': {location_file}."
        )

    try:
        from ._excel_openpyxl import read_sheets
    except ImportError as err:
        raise ImportError(
            "Unable to find a usable Excel engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

    def name_matches(name) -> bool:
        if sheet_name_pattern is None:
            return True
        return sheet_name_pattern.match(name) is not None

    for name, row_cell_iter in read_sheets(source):
        if not name_matches(name):
            logger.debug(f"Skipping sheet '{name}'")
            continue
        location_sheet = location_file.make_location_sheet(name)
        yield from parse_blocks(
            row_cell_iter,
            location_sheet=location_sheet,
            fixer=fixer,
            to=to,
            filter=filter,
            issue_tracker=issue_tracker,
        )


class ExcelWriteBackend(Enum):
    OPENPYXL = auto()
    XLSXWRITER = auto()


def write_excel(
    tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
    to: Union[str, os.PathLike, Path, BinaryIO],
    na_rep: str = "-",
    sep_lines: int = 1,
    styles: Union[bool, Dict] = False,
    backend: ExcelWriteBackend = ExcelWriteBackend.OPENPYXL,
    engine_kwargs: Union[None, Dict] = None,
):
    """Writes one or more tables to an Excel workbook.

    Writes table blocks to an Excel workbook file.
    Values are formatted to comply with the StarTable standard where necessary and possible.

    This is a thin wrapper around parse_blocks(). The only thing it does is to present the contents
    of an Excel workbook as a Iterable of cell rows, where each row is a sequence of values.

    Args:
        tables:
            Table(s) to write.
            * If a single Table or an iterable of Tables, writes to one sheet with default name.
            * If a dict of {sheet_name: Table} or {sheet_name: Iterable[Table]}, writes tables to
              sheets with specified names.

        to:
            File path or binary stream to which to write.
            If a file path, then this file gets created/overwritten and then closed after writing.
            If a stream, then it is left open after writing; the caller is responsible for managing
            the stream.

        na_rep:
            Optional; String representation of missing values (NaN, None, NaT).
            If overriding the default '-', it is recommended to use another value compliant with
            the StarTable standard.

        sep_lines:
            Optional; Number of blank separator lines between tables.
            Default is 1.

        styles:
            Optional. Determines whether styles are applied to table blocks in the output workbook.
            * If bool(styles) is False (default), no styles are applied.
            * If True, default pdtable styles are applied (neutral shades of grey and dark blue).
            * Custom styles can be specified by passing a JSON-like structure of dictionaries.
              The top-level keys represent the parts of a table block and are any number of:
              {'table_name', 'destinations', 'column_names', 'units', 'values'}
              For each given table part, the value is a dictionary with any number of the following
              style element keys: {'font', 'fill', 'alignment'}
              The value for each of these keys is in turn a dictionary of valid {property: value}
              pairs.
              Example:
                {
                    'table_name': {
                        'font': {'color': '1F4E78', 'bold': True,},
                        'fill': {'color': 'D9D9D9',},  # RGB color code
                    },
                    'destinations': {
                        'font': {'color': '808080', 'italic': True,},
                        'fill': {'color': 'D9D9D9',},
                    },
                    'column_names': {
                        'fill': {'color': 'F2F2F2',},
                        'font': {'bold': True,},
                    },
                    'units': {'fill': {'color': 'F2F2F2',}},
                    'values': {'alignment': {'horizontal': 'center'}},
                }

             Colors are given as RGB hex codes of the form 'RRGGBB' e.g. 'FF0000' is red.
             Leading transparency digits are accepted but unused e.g. '42FF0000' will still be red.

             If a table part key is omitted, then no style is applied to that table part.
             Similarly, if a style element key is omitted, then no style is explicitly applied to
             that style element.
             In such cases, the default style is determined by the Excel writer engine (the default
             engine is openpyxl).
        backend:
            Optional; backend used to write .xlsx file. Supported options: items in ExcelWriteBackend
        engine_kwargs:
            Optional; Arguments to be passed to the engine "Workbook" class. To write large (> 4GB) files with
            xlsxwriter, set engine_kwargs={'use_zip64': True}
    """
    try:
        if backend == ExcelWriteBackend.OPENPYXL:
            from ._excel_openpyxl import write_excel_openpyxl as write_excel_func
        elif backend == ExcelWriteBackend.XLSXWRITER:
            from ._excel_xlsxwriter import write_excel_xlsxwriter as write_excel_func
        else:
            raise ValueError(f"Invalid backend: {backend}. Valid values are items in ExcelWriteBackend")
    except ImportError as err:
        raise ImportError(
            "Unable to find a usable spreadsheet engine. "
            f"Tried using: '{backend.name.lower()}'.\n"
            f"Please install {backend.name.lower()} for Excel I/O support."
        ) from err
    engine_kwargs = {} if engine_kwargs is None else engine_kwargs
    write_excel_func(tables, to, na_rep, styles, sep_lines, engine_kwargs)
