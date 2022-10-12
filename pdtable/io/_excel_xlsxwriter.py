from pathlib import Path
from typing import Union, Iterable, Dict, BinaryIO
import os

import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from pdtable import Table
from pdtable.io._excel_write_helper import _pack_tables, _table_destinations, _table_header, DEFAULT_STYLE_SPEC
from pdtable.io._represent import _represent_col_elements, _represent_row_elements

DEFAULT_DATE_FORMAT = "yyyy-mm-dd hh:mm"

def write_excel_xlsxwriter(
        tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
        path: Union[str, os.PathLike, Path, BinaryIO],
        na_rep: str,
        styles: Union[bool, Dict],
        sep_lines: int
):
    tables = _pack_tables(tables)

    wb = xlsxwriter.Workbook(path, {"default_date_format": DEFAULT_DATE_FORMAT})
    formats = _create_formats(wb, styles)

    for sheet_name, tabs in tables.items():
        ws = wb.add_worksheet(name=sheet_name)
        row_index = 0

        if isinstance(tabs, Table):
            # For convenience, pack single table in an iterable
            tabs = [tabs]

        for t in tabs:
            row_index = _append_table_to_xlsxwriter_worksheet(t, ws, sep_lines, na_rep, row_index, formats)

    wb.close()


def _create_formats(wb, styles):
    if styles:
        styles = DEFAULT_STYLE_SPEC if styles is True else styles
    else:
        styles = {}

    formats = {cell_type: wb.add_format(_formatting_dict(d)) for cell_type, d in styles.items()}

    unit_transposed_dict = {"align": "center"}
    unit_transposed_dict.update(_formatting_dict(styles.get("units", {})))
    formats["units_transposed"] = wb.add_format(unit_transposed_dict)

    values_transposed_dict = {"align": "center"}
    values_transposed_dict.update(_formatting_dict(styles.get("values", {})))
    formats["values_transposed"] = wb.add_format(values_transposed_dict)

    datetime_value = {"num_format": DEFAULT_DATE_FORMAT}
    datetime_value.update(_formatting_dict(styles.get("values", {})))
    formats["values_datetime"] = wb.add_format(datetime_value)

    datetime_transposed_value = {"align": "center", "num_format": DEFAULT_DATE_FORMAT}
    datetime_transposed_value.update(_formatting_dict(styles.get("values", {})))
    formats["values_datetime_transposed"] = wb.add_format(datetime_transposed_value)

    return formats


def _append_table_to_xlsxwriter_worksheet(table: Table, ws: Worksheet, sep_lines: int, na_rep: str,
                                          row_index: int, formats: Dict) -> int:
    ws.write(row_index, 0, _table_header(table), formats.get("table_name", None))
    ws.write(row_index + 1, 0, _table_destinations(table), formats.get("destinations", None))
    if table.metadata.transposed:
        for i, col in enumerate(table):
            row_i = row_index + 2 + i
            ws.write(row_i, 0, col.name, formats.get("column_names", None))
            ws.write(row_i, 1, col.unit, formats.get("units_transposed", None))
            if col.unit == "datetime":
                ft = formats.get("values_datetime_transposed", None)
            else:
                ft = formats.get("values_transposed", None)
            ws.write_row(
                row_i, 2,
                _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True),
                ft
            )
        final_row = row_i + 1

    else:
        ws.write_row(row_index + 2, 0, table.column_names, formats.get("column_names", None))
        ws.write_row(row_index + 3, 0, table.units, formats.get("units", None))
        for i, col in enumerate(table):
            if col.unit == "datetime":
                ft = formats.get("values_datetime", None)
            else:
                ft = formats.get("values", None)
            ws.write_column(
                row_index + 4, i,
                _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True),
                ft
            )
        final_row = row_index + 4 + table.df.shape[0]

    return final_row + sep_lines


def _formatting_dict(openpyxl_dict: Dict) -> Dict:
    xlsxwriter_dict = {}
    # font stuff
    font = openpyxl_dict.get("font", {})
    for prop in ["bold", "italic"]:
        if prop in font:
            xlsxwriter_dict[prop] = font[prop]
    for prop in ["color", "name", "size"]:
        if prop in font:
            xlsxwriter_dict[f"font_{prop}"] = font[prop]

    # background stuff
    fill = openpyxl_dict.get("fill", {})
    if "color" in fill:
        xlsxwriter_dict["bg_color"] = fill["color"]
        xlsxwriter_dict["pattern"] = 1  # solid fill

    #alignment stuff
    alignment = openpyxl_dict.get("alignment", {})
    if "vertical" in alignment:
        xlsxwriter_dict["valign"] = alignment["vertical"]
    if "horizontal" in alignment:
        xlsxwriter_dict["align"] = alignment["horizontal"]

    return xlsxwriter_dict
