"""Interface to read/write Tables from/to an Excel workbook.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others readers such as 'xlrd' and writers such as 'xlsxwriter'.

openpyxl (and eventually other engines) are not required at install time; only when the functions
requiring them (read_excel() or write_excel()) are called for the first time.

"""
import os
from os import PathLike
from typing import Union, Callable, Iterable, BinaryIO, Dict

from .parsers.blocks import parse_blocks
from .parsers.fixer import ParseFixer
from .. import BlockType, Table, TableBundle
from ..store import BlockIterator


def read_excel(
    source: Union[str, PathLike, BinaryIO],
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
    to: Union[str, os.PathLike, BinaryIO],
    na_rep: str = "-",
    num_blank_rows_between_tables: int = 1,
    style: bool = False
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
        num_blank_rows_between_tables:
            Optional; Number of blank rows between tables.
            Default is 1.
        style:
            Optional; Whether or not to apply standard StarTable style to Excel workbook file.
            Default is False.
    """
    try:
        from ._excel_openpyxl import write_excel_openpyxl as write_excel_func

    except ImportError as err:
        raise ImportError(
            "Unable to find a usable spreadsheet engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

    write_excel_func(tables, to, na_rep, style, num_blank_rows_between_tables)
