"""Interface to read StarTable data from an Excel workbook file.

This is a thin wrapper around parse_blocks(). The only thing it does is to present the contents of
an Excel workbook as a Iterable of cell rows, where each row is a sequence of values.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others.

openpyxl (and eventually other engines) are not required at install time;
only when read_excel() is called for the first time.
"""

from os import PathLike

from .parsers.blocks import parse_blocks
from ..store import BlockGenerator


def read_excel(path: PathLike) -> BlockGenerator:
    """Reads StarTable blocks from an Excel workbook.

    Reads StarTable blocks from an Excel workbook file at the specified path.
    Yields them one at a time as a tuple: (block type, block content)

    Args:
        path:
            Path of workbook to read.

    Yields:
        Tuples of the form (block type, block content)
    """
    try:
        import openpyxl
        # from ._read_excel_openpyxl import parse_blocks
        wb = openpyxl.load_workbook(path)
        for ws in wb.worksheets:
            cell_rows = ws.iter_rows(values_only=True)
            yield from parse_blocks(cell_rows)

    except ImportError as err:
        raise ImportError(
            "Unable to find a usable Excel engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

