from pathlib import Path
from typing import Union, Iterable, Dict, BinaryIO
import os

import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from pdtable import Table
from pdtable.io._excel_write_helper import _pack_tables, _table_destinations, _table_header
from pdtable.io._represent import _represent_col_elements, _represent_row_elements


def write_excel_xlsxwriter(
        tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
        path: Union[str, os.PathLike, Path, BinaryIO],
        na_rep: str,
        styles: Union[bool, Dict],
        sep_lines: int
):
    tables = _pack_tables(tables)

    wb = xlsxwriter.Workbook(path, {"default_date_format": "yyyy-mm-dd hh:mm"})
    for sheet_name, tabs in tables.items():
        ws = wb.add_worksheet(name=sheet_name)
        row_index = 0

        if isinstance(tabs, Table):
            # For convenience, pack single table in an iterable
            tabs = [tabs]

        for t in tabs:
            row_index = _append_table_to_xlsxwriter_worksheet(t, ws, sep_lines, na_rep, row_index)

    wb.close()

def _append_table_to_xlsxwriter_worksheet(table: Table, ws: Worksheet, sep_lines: int, na_rep: str, row_index: int) -> int:
    ws.write(row_index, 0, _table_header(table))
    ws.write(row_index + 1, 0, _table_destinations(table))
    if table.metadata.transposed:
        for i, col in enumerate(table):
            row_i = row_index + 2 + i
            ws.write(row_i, 0, col.name)
            ws.write(row_i, 1, col.unit)
            ws.write_row(
                row_i, 2, _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True)
            )

    else:
        ws.write_row(row_index + 2, 0, table.column_names)
        ws.write_row(row_index + 3, 0, table.units)
        for i, row in enumerate(table.df.itertuples(index=False, name=None)):
            row_i = row_index + 4 + i
            if ws.write_row(
                    row_i, 0, _represent_row_elements(row, table.units, na_rep, convert_datetime=True)
            ) != 0:
                raise IOError("Could not write value to worksheet")

    return row_i + sep_lines + 1
