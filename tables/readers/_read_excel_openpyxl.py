"""Machinery to read Tables from an Excel workbook using openpyxl as engine."""

from typing import Optional, Sequence, Iterable

import tables.proxy
import tables.table_metadata
from .read_csv import make_block

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet

from tables.store import BlockType, BlockGenerator


def parse_blocks(cell_rows: Iterable[Sequence], origin: Optional[str] = None) -> BlockGenerator:
    """Parses blocks from a single sheet.

    Takes an iterable of cell rows and parses it into blocks.

    Args:
        cell_rows: Iterable of cell rows, where each row is a sequence of cells.
        origin: A thing.

    Yields:
        Blocks.
    """
    # Loop seems clunky with repeated init and emit clauses -- could probably be cleaned up
    # but I haven't seen how.
    # Template data handling is half-hearted, mostly because of doubts on StarTable syntax
    # Must all template data have leading `:`?
    # In any case, avoiding row-wise emit for multi-line template data should be a priority.

    block_lines = []
    block_type = BlockType.METADATA
    block_start_row = 0
    for irow_0based, row in enumerate(cell_rows):
        next_block_type = None
        first_cell = row[0] if len(row) > 0 else None
        if isinstance(first_cell, str) and first_cell != "":
            # Check whether it's a block start marker
            if first_cell.startswith("**"):
                if first_cell.startswith("***"):
                    next_block_type = BlockType.DIRECTIVE
                else:
                    next_block_type = BlockType.TABLE
            elif first_cell.startswith(":"):
                next_block_type = BlockType.TEMPLATE_ROW
        elif (
                first_cell is None or first_cell == ""
        ) and not block_type == BlockType.METADATA:
            # Blank first cell marks end of block
            next_block_type = BlockType.BLANK

        if next_block_type is not None:
            yield make_block(
                block_type, block_lines,
                tables.table_metadata.TableOriginCSV(origin, block_start_row)
            )
            # TODO replace TableOriginCSV with one tailored for Excel
            block_lines = []
            block_type = next_block_type
            block_start_row = irow_0based + 1
        block_lines.append(row)

    if block_lines:
        yield make_block(block_type, block_lines,
                         tables.table_metadata.TableOriginCSV(origin, block_start_row))
        # TODO replace TableOriginCSV with one tailored for Excel
