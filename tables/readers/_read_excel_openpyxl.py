"""Machinery to read Tables from an Excel workbook using openpyxl as engine."""

from typing import Optional

import tables.proxy
import tables.table_metadata
from .read_csv import make_block

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet

from tables.store import BlockType, BlockGenerator


def parse_blocks(ws: OpenpyxlWorksheet, origin: Optional[str] = None) -> BlockGenerator:
    """Parses blocks from a single Openpyxl worksheet"""
    block_lines = []
    block_type = BlockType.METADATA
    block_start_row = 0
    for irow_0based, row in enumerate(ws.iter_rows(values_only=True)):
        # TODO iterate on cells instead of rows? because all rows are as wide as the rightmost thing in the sheet
        next_block_type = None
        first_cell = row[0]
        first_cell_is_str = isinstance(first_cell, str)
        if first_cell_is_str:
            if first_cell.startswith("**"):
                if first_cell.startswith("***"):
                    next_block_type = BlockType.DIRECTIVE
                else:
                    next_block_type = BlockType.TABLE
            elif first_cell.startswith(":"):
                next_block_type = BlockType.TEMPLATE_ROW
        elif (
            first_cell is None or (first_cell_is_str and first_cell == "")
        ) and not block_type == BlockType.METADATA:
            next_block_type = BlockType.BLANK

        if next_block_type is not None:
            yield make_block(
                block_type, block_lines, tables.table_metadata.TableOriginCSV(origin, block_start_row)
            )
            # TODO replace TableOriginCSV with one tailored for Excel
            block_lines = []
            block_type = next_block_type
            block_start_row = irow_0based + 1
        block_lines.append(row)

    if block_lines:
        yield make_block(block_type, block_lines, tables.table_metadata.TableOriginCSV(origin, block_start_row))
        # TODO replace TableOriginCSV with one tailored for Excel
