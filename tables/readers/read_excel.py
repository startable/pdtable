"""Interface to read StarTable data from an Excel workbook file.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others.

openpyxl (and eventually other engines) are not required at install time;
only when write_excel() (or something else in this module) is called for the first time.
"""

from os import PathLike

from tables.store import BlockGenerator


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
        from ._read_excel_openpyxl import parse_blocks
    except ImportError as err:
        raise ImportError(
            "Unable to find a usable Excel engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err
    wb = openpyxl.load_workbook(path)
    for ws in wb.worksheets:
        cell_rows = ws.iter_rows(values_only=True)
        yield from parse_blocks(cell_rows)
