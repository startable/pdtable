"""Machinery to write Tables to an Excel workbook using openpyxl as engine."""

import openpyxl

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet


from pdtable import Table
from pdtable.io._represent import _represent_row_elements


def write_excel_openpyxl(na_rep, out, tables):
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
