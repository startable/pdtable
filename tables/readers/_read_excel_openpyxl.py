"""Machinery to read Tables from an Excel workbook using openpyxl as engine."""

from typing import List, Tuple, Any, Optional

import numpy as np
import pandas as pd

import tables.table_metadata
import tables.proxy
from .parsers.columns import normalize_if_str, is_missing_data_marker
from .read_csv import make_table

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet

from tables.store import BlockType, BlockGenerator

# ================================================================================
# ====== Parsers for the various column types (as determined by their unit) ======

_onoff_value_conversions = {0: False, 1: True, False: False, True: True, "0": False, "1": True}


def _parse_onoff_column(values) -> np.ndarray:
    try:
        as_bool = [_onoff_value_conversions[normalize_if_str(v)] for v in values]
    except KeyError:
        raise ValueError("Entries in onoff columns must be 0 (False) or 1 (True)")
    return np.array(as_bool, dtype=np.bool)


def _parse_float_column(values) -> np.ndarray:
    try:
        values = [np.nan if is_missing_data_marker(v) else float(v) for v in values]
    except (ValueError, TypeError):
        raise ValueError(
            "Entries in numerical columns must be numbers or missing-value markers ('-', 'NaN', 'nan')"
        )
    return np.array(values)


def _parse_datetime_column(values):
    values = [
        pd.NaT if is_missing_data_marker(v) else pd.to_datetime(v, dayfirst=True) for v in values
    ]
    return np.array(values)

# ====== End column parsers ======================================================
# ================================================================================


_column_parsers = {
    "text": lambda values: np.array(values, dtype=np.str),
    "onoff": _parse_onoff_column,
    "datetime": _parse_datetime_column,
}


_block_factory_lookup = {BlockType.TABLE: make_table}


def make_block(block_type: BlockType, lines: List[List], origin) -> Tuple[BlockType, Any]:
    factory = _block_factory_lookup.get(block_type, None)
    return block_type, None if factory is None else factory(lines, origin)


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
