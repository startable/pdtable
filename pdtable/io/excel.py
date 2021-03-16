"""Interface to read/write Tables from/to an Excel workbook.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others readers such as 'xlrd' and writers such as 'xlsxwriter'.

openpyxl (and eventually other engines) are not required at install time; only when the functions
requiring them (read_excel() or write_excel()) are called for the first time.

"""
import os
from os import PathLike
from pathlib import Path
from typing import Union, Callable, Iterable, BinaryIO, Dict

from .parsers.blocks import parse_blocks
from .parsers.fixer import ParseFixer
from .. import BlockType, Table
from ..store import BlockIterator


def read_excel(
    source: Union[str, PathLike, Path, BinaryIO],
    origin=None,
    fixer: ParseFixer = None,
    to: str = "pdtable",
    filter: Callable[[BlockType, str], bool] = None,
) -> BlockIterator:
    """Reads StarTable blocks from an Excel workbook.
    # TODO copy most of read_csv() docstring over

    Reads StarTable blocks from an Excel workbook file at the specified path.
    Yields them one at a time as a tuple: (block type, block content)

    Args:
        source:
            Path of workbook to read.



    Yields:
        Tuples of the form (block type, block content)
    """

    kwargs = {"origin": origin, "fixer": fixer, "to": to, "filter": filter}

    try:
        from ._excel_openpyxl import read_cell_rows_openpyxl as read_cell_rows

    except ImportError as err:
        raise ImportError(
            "Unable to find a usable Excel engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

    yield from parse_blocks(read_cell_rows(source), **kwargs)


def write_excel(
    tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
    to: Union[str, os.PathLike, Path, BinaryIO],
    na_rep: str = "-",
    sep_lines: int = 1,
    styles: Union[bool, Dict] = False
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
    """
    try:
        from ._excel_openpyxl import write_excel_openpyxl as write_excel_func

    except ImportError as err:
        raise ImportError(
            "Unable to find a usable spreadsheet engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

    write_excel_func(tables, to, na_rep, styles, sep_lines)
