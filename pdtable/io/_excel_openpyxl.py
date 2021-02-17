"""Machinery to read/write Tables in an Excel workbook using openpyxl as engine."""
from os import PathLike
from typing import Union, Iterable, Sequence, Any, Dict

import openpyxl

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

from pdtable import Table
from pdtable.io._represent import _represent_row_elements, _represent_col_elements


DEFAULT_SHEET_NAME = "Sheet1"


def read_cell_rows_openpyxl(path: Union[str, PathLike]) -> Iterable[Sequence[Any]]:
    """Reads from an Excel workbook, yielding one row of cells at a time."""
    import openpyxl

    wb = openpyxl.load_workbook(path, read_only=True, data_only=True, keep_links=False)
    for ws in wb.worksheets:
        yield from ws.iter_rows(values_only=True)


def write_excel_openpyxl(tables, path, na_rep):
    """Write tables to an Excel workbook at the specified path."""

    if not isinstance(tables, Dict):
        # For convenience, pack it in a dict
        tables = {DEFAULT_SHEET_NAME: tables}

    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # Remove the one sheet that openpyxl creates by default

    for sheet_name in tables:

        tabs = tables[sheet_name]
        if isinstance(tabs, Table):
            # For convenience, pack single table in an iterable
            tabs = [tabs]

        ws = wb.create_sheet(title=sheet_name)
        for t in tabs:
            _append_table_to_openpyxl_worksheet(t, ws, na_rep)

    wb.save(path)


def _append_table_to_openpyxl_worksheet(
    table: Table, ws: OpenpyxlWorksheet, na_rep: str = "-"
) -> None:
    units = table.units
    if table.metadata.transposed:
        ws.append([f"**{table.name}*"])
        ws.append([" ".join(str(x) for x in table.metadata.destinations)])
        for col in table:
            ws.append(
                [str(col.name), str(col.unit)]
                + list(_represent_col_elements(col.values, col.unit, na_rep)),
            )
    else:
        ws.append([f"**{table.name}"])
        ws.append([" ".join(str(x) for x in table.metadata.destinations)])
        ws.append(table.column_names)
        ws.append(units)
        for row in table.df.itertuples(index=False, name=None):
            # TODO: apply format string specified in ColumnMetadata
            ws.append(_represent_row_elements(row, units, na_rep))

    ws.append([])  # blank row marking table end


def _format_tables_in_worksheet(ws: OpenpyxlWorksheet) -> None:
    # Define styles to be used
    # TODO: These should perhaps live somewhere else?
    font_name = 'Arial'
    font_size = 10
    header_font = Font(bold=True, color='1F4E78', name=font_name, size=font_size)
    destination_font = Font(bold=True, color='808080', name=font_name, size=font_size)
    name_font = Font(bold=True, name=font_name, size=font_size)
    values_font = Font(name=font_name, size=font_size)
    header_fill = PatternFill(start_color='D9D9D9', fill_type='solid')
    variable_fill = PatternFill(start_color='F2F2F2', fill_type='solid')

    # Identify placement of tables in sheet
    rows = [row for row in ws.iter_rows()]
    i_start = [i for i, row in enumerate(ws.iter_rows()) if row[0].value is not None and row[0].value.startswith('**')]
    i_end = [i - 1 for i in i_start if i > 0] + [len(rows)]

    # Loop through tables
    for i in range(len(i_start)):
        if rows[i_start[i]][0].value[-1] == '*':   # Transposed table
            header_row = rows[i_start[i]]
            destination_row = rows[i_start[i] + 1]
            table_cols = [col for col in ws.iter_cols(min_row=i_start[i] + 3, max_row=i_end[i])]
            name_cells = table_cols[0]
            unit_cells = table_cols[1]
            list_of_value_cells = table_cols[2:]
        else:
            header_row = rows[i_start[i]]
            destination_row = rows[i_start[i] + 1]
            table_rows = rows[i_start[i] + 2:i_end[i]]
            row_end = len([cell for cell in table_rows[0] if cell.value is not None])
            if row_end < len(table_rows[0]):    # Cut off rows outside table
                header_row = header_row[:row_end]
                destination_row = destination_row[:row_end]
                table_rows = [row[:row_end] for row in table_rows]
            name_cells = table_rows[0]
            unit_cells = table_rows[1]
            list_of_value_cells = table_rows[2:]

        # Format cells with defined styles
        _format_cells(header_row, font=header_font, fill=header_fill)
        _format_cells(destination_row, font=destination_font, fill=header_fill)
        _format_cells(name_cells, font=name_font, fill=variable_fill)
        _format_cells(unit_cells, font=values_font, fill=variable_fill)
        for value_cells in list_of_value_cells:
            _format_cells(value_cells, font=values_font)

    # Widen columns
    num_cols = len(rows[0])
    for i_column in [get_column_letter(i + 1) for i in range(num_cols + 1)]:
        ws.column_dimensions[i_column].width = 20


def _format_cells(cells, *, font: Font, fill: PatternFill = None) -> None:
    for cell in cells:
        cell.font = font
        if fill is None:
            continue
        cell.fill = fill
