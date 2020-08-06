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
import pandas as pd
from typing import Iterable, TextIO, Union
from pathlib import Path

from ..pdtable import Table
from ._formatting import _represent_row_elements

# def to_excel(self, path, header: str = '', header_sep: str = ';') -> None:
#     '''
#     :param path: Path to the location to save excel file to.
#     :param header: Text to be shown before the bundle of tables. If the text contains a newline (\n) and/or the
#             header_sep, the text will span over multiple rows and/or columns, respectively, in the excel
#             sheet.  Header will have one line of separation to the bundle tables.
#     :param header_sep: Separator to control header text to be split onto multiple columns
#     '''
#     wb = openpyxl.Workbook()
#     ws = wb.active

#     if header:
#         for row in header.rstrip().split('\n'):
#             ws.append(row.split(header_sep))
#         ws.append([])

#     for t in self._tables:
#         t.to_excel(ws)
#         ws.append([])  # blank line after table block
#     wb.save(path)


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
    ws.append([None])  # empty row marking table end
