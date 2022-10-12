from pathlib import Path
from typing import Union, Iterable, Dict, BinaryIO
import os

import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from pdtable import Table
from pdtable.io._excel_write_helper import _pack_tables, _table_destinations, _table_header, DEFAULT_STYLE_SPEC
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

    if styles:
        styles = DEFAULT_STYLE_SPEC if styles is True else styles
    else:
        styles = {}
    formats = {cell_type: wb.add_format(_formatting_dict(d)) for cell_type, d in styles.items()}

    for sheet_name, tabs in tables.items():
        ws = wb.add_worksheet(name=sheet_name)
        row_index = 0

        if isinstance(tabs, Table):
            # For convenience, pack single table in an iterable
            tabs = [tabs]

        for t in tabs:
            row_index = _append_table_to_xlsxwriter_worksheet(t, ws, sep_lines, na_rep, row_index, formats)

    wb.close()

def _append_table_to_xlsxwriter_worksheet(table: Table, ws: Worksheet, sep_lines: int, na_rep: str,
                                          row_index: int, formats: Dict) -> int:
    ws.write(row_index, 0, _table_header(table), formats.get("table_name", None))
    ws.write(row_index + 1, 0, _table_destinations(table), formats.get("destinations", None))
    if table.metadata.transposed:
        for i, col in enumerate(table):
            row_i = row_index + 2 + i
            ws.write(row_i, 0, col.name, formats.get("column_names", None))
            ws.write(row_i, 1, col.unit, formats.get("units", None))
            ws.write_row(
                row_i, 2,
                _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True),
                None
            )

    else:
        ws.write_row(row_index + 2, 0, table.column_names, formats.get("column_names", None))
        ws.write_row(row_index + 3, 0, table.units, formats.get("units", None))
        for i, row in enumerate(table.df.itertuples(index=False, name=None)):
            row_i = row_index + 4 + i
            ws.write_row(
                row_i, 0,
                _represent_row_elements(row, table.units, na_rep, convert_datetime=True),
                None
            )

    return row_i + sep_lines + 1


def _formatting_dict(openpyxl_dict: Dict) -> Dict:
    xlsxwriter_dict = {}
    # font stuff
    font = openpyxl_dict.get("font", {})
    if "bold" in font:
        xlsxwriter_dict["bold"] = font["bold"]
    for prop in ["color", "name", "size"]:
        if prop in font:
            xlsxwriter_dict[f"font_{prop}"] = font[prop]

    # background stuff
    fill = openpyxl_dict.get("fill", {})
    if "color" in fill:
        xlsxwriter_dict["bg_color"] = fill["color"]
        xlsxwriter_dict["pattern"] = 1  # solid fill

    return xlsxwriter_dict
