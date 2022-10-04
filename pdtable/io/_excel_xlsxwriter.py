from pathlib import Path
from typing import Union, Iterable, Dict, BinaryIO
import os

import xlsxwriter

from pdtable import Table
from pdtable.io._excel_write_helper import pack_tables


def write_excel_xlsxwriter(
        tables: Union[Table, Iterable[Table], Dict[str, Table], Dict[str, Iterable[Table]]],
        path: Union[str, os.PathLike, Path, BinaryIO],
        na_rep: str,
        styles: Union[bool, Dict],
        sep_lines: int
):
    tables = pack_tables(tables)

    wb = xlsxwriter.Workbook()
    for sheet_name, tabs in tables.items():
        wb.add_worksheet(name=sheet_name)
    return NotImplemented()
