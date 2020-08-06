"""Machinery to write Tables to an Excel workbook. 

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others such as 'xlsxwriter'. 

openpyxl (and eventually other engines) are not required at install time; 
only when write_excel() is called for the first time. 
"""
try:
    import openpyxl

    try:
        from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
    except ImportError:
        # openpyxl < 2.6
        from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError as err:
    raise ImportError(
        "Unable to find a usable spreadsheet engine. "
        "Tried using: 'openpyxl'.\n"
        "Please install openpyxl for Excel I/O support."
    )

from tables.store import TableBundle
from typing import Iterable, Union
import os

from ..pdtable import Table
from ._formatting import _represent_row_elements


def write_excel(
    tables: Union[Table, Iterable[Table], TableBundle],
    out: Union[str, os.PathLike],
    na_rep: str = "-",
):
    """Writes one or more tables to an Excel workbook.

    Writes table blocks to an Excel workbook file. 
    Values are formatted to comply with the StarTable standard where necessary and possible. 

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        out:
            File path to which to write. 
        na_rep:
            Optional; String representation of missing values (NaN, None, NaT). If overriding the default '-', it is recommended to use another value compliant with the StarTable standard.
    """

    if isinstance(tables, Table):
        # For convenience, pack single table in an iterable
        tables = [tables]

    wb = openpyxl.Workbook()
    ws = wb.active
    for t in tables:
        _append_table_to_openpyxl_worksheet(t, ws, na_rep)
    wb.save(out)


def _append_table_to_openpyxl_worksheet(
    table: Table, ws: OpenpyxlWorksheet, na_rep: str = "-"
) -> None:
    units = table.units
    ws.append([f"**{table.name}"])
    ws.append([" ".join(str(x) for x in table.metadata.destinations)])
    ws.append(table.column_names)
    ws.append(units)
    for row in table.df.itertuples(index=False, name=None):
        # TODO: apply format string specified in ColumnMetadata
        ws.append(_represent_row_elements(row, units, na_rep))
    ws.append([])  # blank row marking table end
