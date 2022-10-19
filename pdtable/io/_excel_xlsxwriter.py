from pathlib import Path
from typing import Union, Iterable, Dict, BinaryIO, Any
import os

import xlsxwriter
from xlsxwriter.worksheet import Worksheet

from pdtable import Table
from pdtable.io._excel_write_helper import _pack_tables, _table_destinations, _table_header, DEFAULT_STYLE_SPEC
from pdtable.io._represent import _represent_col_elements

DEFAULT_DATE_FORMAT = "yyyy-mm-dd hh:mm:ss"


def write_excel_xlsxwriter(
        tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
        path: Union[str, os.PathLike, Path, BinaryIO],
        na_rep: str,
        styles: Union[bool, Dict],
        sep_lines: int,
        engine_kwargs: Dict[str, Any]
):
    tables = _pack_tables(tables)

    wb = xlsxwriter.Workbook(path, engine_kwargs)
    formats = XlsxwriterCellFormats(wb, styles)

    for sheet_name, tabs in tables.items():
        ws = wb.add_worksheet(name=sheet_name)
        row_index = 0

        if isinstance(tabs, Table):
            # For convenience, pack single table in an iterable
            tabs = [tabs]

        for t in tabs:
            row_index = _append_table_to_xlsxwriter_worksheet(t, ws, sep_lines, na_rep, row_index, formats)

    wb.close()


def _formatting_dict(openpyxl_dict: Dict) -> Dict:
    """ Transforms an openpyxl style configuration to a xlsxwriter one """
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


def _with_default_format(wb: xlsxwriter.Workbook, default: Dict, style_def: Dict):
    default.update(_formatting_dict(style_def))
    return wb.add_format(default)


class XlsxwriterCellFormats:
    def __init__(self, wb: xlsxwriter.Workbook, styles: Dict[str, Any]):
        if styles:
            styles = DEFAULT_STYLE_SPEC if styles is True else styles
        else:
            styles = {}

        self.table_name = wb.add_format(_formatting_dict(styles.get("table_name", {})))
        self.destinations = wb.add_format(_formatting_dict(styles.get("destinations", {})))
        self.units = wb.add_format(_formatting_dict(styles.get("units", {})))
        self.column_names = wb.add_format(_formatting_dict(styles.get("column_names", {})))
        self.values = wb.add_format(_formatting_dict(styles.get("values", {})))

        self.units_transposed = _with_default_format(wb, {"align": "center"}, styles.get("units", {}))
        self.values_transposed = _with_default_format(wb, {"align": "center"}, styles.get("values", {}))
        self.values_datetime = _with_default_format(
            wb, {"num_format": DEFAULT_DATE_FORMAT}, styles.get("values", {})
        )
        self.values_datetime_transposed = _with_default_format(
            wb, {"num_format": DEFAULT_DATE_FORMAT, "align": "center"}, styles.get("values", {})
        )


def _append_table_to_xlsxwriter_worksheet(table: Table, ws: Worksheet, sep_lines: int, na_rep: str,
                                          row_index: int, formats: XlsxwriterCellFormats) -> int:
    ws.write(row_index, 0, _table_header(table), formats.table_name)
    ws.write(row_index + 1, 0, _table_destinations(table), formats.destinations)
    if table.metadata.transposed:
        for i, col in enumerate(table):
            row_i = row_index + 2 + i
            ws.write(row_i, 0, col.name, formats.column_names)
            ws.write(row_i, 1, col.unit, formats.units_transposed)
            if col.unit == "datetime":
                ft = formats.values_datetime_transposed
            else:
                ft = formats.values_transposed
            ws.write_row(
                row_i, 2,
                _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True),
                ft
            )
        final_row = row_i + 1

    else:
        ws.write_row(row_index + 2, 0, table.column_names, formats.column_names)
        ws.write_row(row_index + 3, 0, table.units, formats.units)
        for i, col in enumerate(table):
            if col.unit == "datetime":
                ft = formats.values_datetime
            else:
                ft = formats.values
            ws.write_column(
                row_index + 4, i,
                _represent_col_elements(col.values, col.unit, na_rep, convert_datetime=True),
                ft
            )
        final_row = row_index + 4 + table.df.shape[0]

    return final_row + sep_lines


