"""Machinery to read StarTable data from an Excel workbook file.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others.

openpyxl (and eventually other engines) are not required at install time;
only when write_excel() (or something else in this module) is called for the first time.
"""

import itertools
from os import PathLike
from typing import List, Tuple, Any, Optional, Iterable

import numpy as np
import pandas as pd

from tables.store import StarBlockType, BlockGenerator
from .. import pdtable

try:
    import openpyxl

    try:
        from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
    except ImportError:
        # openpyxl < 2.6
        from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError as err:
    raise ImportError(
        "Unable to find a usable Excel engine. "
        "Tried using: 'openpyxl'.\n"
        "Please install openpyxl for Excel I/O support."
    )


def normalize_if_str(x):
    """If it's a string, strip it and make it lowercase; otherwise leave it alone."""
    if isinstance(x, str):
        return x.strip().lower()
    return x


def is_missing_data_marker(x):
    """Return True if, after normalization, it's a valid StarTable missing-data marker"""
    return normalize_if_str(x) in {"-", "nan"}


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


_column_parsers = {
    "text": lambda values: np.array(values, dtype=np.str),
    "onoff": _parse_onoff_column,
    "datetime": _parse_datetime_column,
}
# ====== End column parsers ======================================================
# ================================================================================


def _make_table(lines: List[List], origin=None) -> pdtable.Table:
    """Makes a Table from the cell contents of the lines of a given table block"""
    # Parse header things
    table_name = lines[0][0][2:]
    destinations = {s.strip() for s in lines[1][0].split(" ")}
    column_names = list(
        itertools.takewhile(lambda s: s is not None and len(s.strip()) > 0, lines[2])
    )
    column_names = [el.strip() for el in column_names]
    n_col = len(column_names)
    units = lines[3][:n_col]

    # Parse column values
    columns = {}
    for name, values, unit in zip(column_names, zip(*lines[4:]), units):
        try:
            columns[name] = _column_parsers.get(unit, _parse_float_column)(values)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column {name} of table {table_name} as {unit}"
            ) from e

    # Shove it all in a Table
    return pdtable.Table(
        pdtable.make_pdtable(
            pd.DataFrame(columns),
            units=units,
            metadata=pdtable.TableMetadata(
                name=table_name, destinations=destinations, origin=origin
            ),
        )
    )


_block_factory_lookup = {StarBlockType.TABLE: _make_table}


def make_block(block_type: StarBlockType, lines: List[List], origin) -> Tuple[StarBlockType, Any]:
    factory = _block_factory_lookup.get(block_type, None)
    return block_type, None if factory is None else factory(lines, origin)


def parse_blocks(ws: OpenpyxlWorksheet, origin: Optional[str] = None) -> BlockGenerator:
    """Parses blocks from a single Openpyxl worksheet"""
    block_lines = []
    block_type = StarBlockType.METADATA
    block_start_row = 0
    for irow_0based, row in enumerate(ws.iter_rows(values_only=True)):
        # TODO iterate on cells instead of rows? because all rows are as wide as the rightmost thing in the sheet
        next_block_type = None
        first_cell = row[0]
        first_cell_is_str = isinstance(first_cell, str)
        if first_cell_is_str:
            if first_cell.startswith("**"):
                if first_cell.startswith("***"):
                    next_block_type = StarBlockType.DIRECTIVE
                else:
                    next_block_type = StarBlockType.TABLE
            elif first_cell.startswith(":"):
                next_block_type = StarBlockType.TEMPLATE_ROW
        elif (
            first_cell is None or (first_cell_is_str and first_cell == "")
        ) and not block_type == StarBlockType.METADATA:
            next_block_type = StarBlockType.BLANK

        if next_block_type is not None:
            yield make_block(
                block_type, block_lines, pdtable.TableOriginCSV(origin, block_start_row)
            )
            block_lines = []
            block_type = next_block_type
            block_start_row = irow_0based + 1
        block_lines.append(row)

    if block_lines:
        yield make_block(block_type, block_lines, pdtable.TableOriginCSV(origin, block_start_row))


def read_excel(path: PathLike) -> BlockGenerator:
    """Reads StarTable blocks from an Excel workbook.

    Reads StarTable blocks from an Excel workbook file at the specified path.
    Yields them one at a time as a tuple: (block type, block content)

    Args:
        path:
            Path of workbook to read.

    Yields:
        Tuples of the form (block type, block content)
    """
    wb = openpyxl.load_workbook(path)
    for ws in wb.worksheets:
        yield from parse_blocks(ws)
