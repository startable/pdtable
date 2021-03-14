"""Machinery to parse columns in accordance with their unit indicator.

Parsers to convert column values of uncontrolled data types into values with a data type
consistent with the intended representation given the column's unit indicator.

A data-type-specific parser is implemented for each of the allowable StarTable column unit
indicators:
- 'text' -> str
- 'onoff' -> bool
- 'datetime' -> datetime / NaT
- everything else -> float / NaN

The parse_column() wrapper takes care of switching between these data-type-specific parsers.
This wrapper is the intended API.
"""
import datetime
from collections import defaultdict
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from .fixer import ParseFixer


def normalize_if_str(val):
    """If it's a string, strip it and make it lowercase. Otherwise, leave it alone."""
    return val.strip().lower() if isinstance(val, str) else val


def is_missing_data_marker(normalized_val):
    """Return True if, after normalization, it's a valid StarTable missing-data marker"""
    return normalize_if_str(normalized_val) in {"-", "nan"}


def _parse_text_column(values: Iterable, fixer: ParseFixer = None):
    # Ensure that 'values' is a Sequence, else np.array() will not unpack it
    return np.array(values if isinstance(values, Sequence) else list(values), dtype=np.str)


def _onoff_to_bool(val) -> bool:
    """Converts typical onoff columns values to bools"""
    conversions = {
        0: False,
        1: True,
        False: False,
        True: True,
        "0": False,
        "1": True,
        "false": False,
        "true": True,
    }
    return conversions[normalize_if_str(val)]


def _parse_onoff_column(values: Iterable, fixer: ParseFixer = None):
    bool_values = []
    for row, val in enumerate(values):
        try:
            bool_values.append(_onoff_to_bool(val))
        except KeyError as err:
            if fixer is not None:
                fixer.table_row = row
                fix_value = fixer.fix_illegal_cell_value("onoff", val)
                bool_values.append(fix_value)
            else:
                raise ValueError("Illegal value in onoff column", val) from err
    return np.array(bool_values, dtype=bool)


def _float_convert(val: str) -> float:
    if val in {"nan", "-"}:
        return np.nan
    return float(val)


def _parse_float_column(values: Iterable, fixer: ParseFixer = None):
    float_values = []
    for row, val in enumerate(values):
        if isinstance(val, float) or isinstance(val, int):
            # It's already a number.
            float_values.append(float(val))
            continue

        # It's something else than a number.
        if isinstance(val, str):
            # It's a string.
            val = normalize_if_str(val)
            try:
                # Parsing the string as one of the expected things (a number or missing value)
                float_values.append(_float_convert(val))
            except (KeyError, ValueError) as err:
                if fixer is not None:
                    fixer.table_row = row
                    fix_value = fixer.fix_illegal_cell_value("float", val)
                    float_values.append(fix_value)
                else:
                    raise ValueError("Illegal value in numerical column", val) from err
        else:
            # It isn't even a string. WTF let the fixer have a shot at it.
            if fixer is not None:
                fixer.table_row = row
                fix_value = fixer.fix_illegal_cell_value("float", val)
                float_values.append(fix_value)
            else:
                raise ValueError("Illegal value in numerical column", val)

    return np.array(float_values)


def _to_datetime(val):
    return pd.NaT if val in ["-", "nan"] else pd.to_datetime(val, dayfirst=True)


def _parse_datetime_column(values: Iterable, fixer: ParseFixer = None):
    datetime_values = []
    for row, val in enumerate(values):
        if isinstance(val, datetime.datetime):
            # It's already a datetime
            datetime_values.append(val)
            continue

        # It's something else than a datetime. Presumably a string.
        # TODO Fails when val is None. There is ambiguity about this None: if it came from a JsonData, it could be a legal NaT, but from Excel it could be an illegal empty cell!  # noqa:E501
        if val is None:
            # fixer should always be defined (= default ParseFixer)
            # when used via our top level API (read_csv, parse_blocks &c.)
            if fixer is not None:
                fixer.table_row = row
                fix_value = fixer.fix_illegal_cell_value("datetime", val)
                datetime_values.append(fix_value)
                continue
            else:
                raise ValueError(f"Illegal value in datetime column {val}")

        if not isinstance(val, str):
            raise ValueError(f"Illegal value in datetime column {val}")

        val = val.strip()
        if len(val) > 0 and (val[0].isdigit() or val in ["-", "nan"]):
            try:
                # Parsing the string as one of the expected things (a datetime or missing value)
                datetime_values.append(_to_datetime(val))
            except ValueError as err:
                if fixer is not None:
                    fixer.table_row = row
                    fix_value = fixer.fix_illegal_cell_value("datetime", val)
                    datetime_values.append(fix_value)
                    continue
                else:
                    raise ValueError("Illegal value in datetime column", val) from err
        else:
            if fixer is not None:
                fixer.table_row = row
                fix_value = fixer.fix_illegal_cell_value("datetime", val)
                datetime_values.append(fix_value)
            else:
                raise ValueError("Illegal value in datetime column", val)

    return np.array(datetime_values)


_column_parsers = defaultdict(lambda: _parse_float_column)
_column_parsers["text"] = _parse_text_column
_column_parsers["onoff"] = _parse_onoff_column
_column_parsers["datetime"] = _parse_datetime_column


def parse_column(unit_indicator: str, values: Iterable, fixer: ParseFixer = None) -> np.ndarray:
    """Parses column values to the intended data type as per the column's unit indicator.

    Parses column values to a consistent internal representation.
    The parser is chosen by switching on unit_indicator.
    The parsed values are returned as a numpy array with a suitable dtype.

    Args:
        unit_indicator:
            The column's unit indicator, e.g. 'text', 'onoff', 'datetime', '-', 'kg'...

        values:
            Iterable of the column's values

        fixer:
            Optional

    Returns:
        Parsed values, placed in a numpy array of a suitable dtype.

    """
    return _column_parsers[unit_indicator](values, fixer)
