"""Machinery to parse columns in accordance with their unit indicator.

Parsers to convert column values of uncontrolled data types into values with a data type
consistent with the intended representation given the column's unit indicator.

A parser is implemented for each of the allowable StarTable column unit indicators:
- 'text' -> str
- 'datetime' -> datetime / NaT
- 'onoff' -> bool
- everything else -> float / NaN

A wrapper takes care of switching between these.
"""
import datetime
from collections import defaultdict
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from tables.readers.FixFactory import FixFactory


def _parse_text_column(values: Iterable, fixer: FixFactory):
    # Ensure that values is a Sequence, else np.array will not unpack it
    return np.array(values if isinstance(values, Sequence) else list(values), dtype=np.str)


_onoff_to_bool = {"0": False, "1": True, "-": False}


def _parse_onoff_column(values: Iterable, fixer: FixFactory = None):
    bool_values = []
    for row, val in enumerate(values):
        if isinstance(val, bool) or isinstance(val, int):
            # TODO why are we letting ints in???
            bool_values.append(val)
            continue
        try:
            bool_values.append(_onoff_to_bool[val.strip()])
        except KeyError as err:
            if fixer is not None:
                fixer.TableRow = row  # TBC: index
                fix_value = fixer.fix_illegal_cell_value("onoff", val)
                bool_values.append(fix_value)
            else:
                raise err
    return np.array(bool_values, dtype=np.bool)


_float_converters_by_1st_char = {
    "N": lambda val: np.nan,
    "n": lambda val: np.nan,
    "-": lambda val: np.nan if (len(val) == 1) else float(val),
}
for ch in "+0123456789":
    _float_converters_by_1st_char[ch] = lambda val: float(val)


def _parse_float_column(values: Iterable, fixer: FixFactory = None):
    float_values = []
    for row, val in enumerate(values):
        if isinstance(val, float) or isinstance(val, int):
            float_values.append(float(val))
            continue
        if len(val) > 0 and (val[0] in _float_converters_by_1st_char):
            try:
                float_values.append(_float_converters_by_1st_char[val[0]](val))
            except (KeyError, ValueError) as err:
                if fixer is not None:
                    fixer.TableRow = row  # TBC: index
                    fix_value = fixer.fix_illegal_cell_value("float", val)
                    float_values.append(fix_value)
                else:
                    raise err
        else:
            if fixer is not None:
                fixer.TableRow = row  # TBC: index
                fix_value = fixer.fix_illegal_cell_value("float", val)
                float_values.append(fix_value)

    return np.array(float_values)


_to_datetime = lambda val: pd.NaT if val == "-" else pd.to_datetime(val, dayfirst=True)


def _parse_datetime_column(values: Iterable, fixer: FixFactory = None):
    datetime_values = []
    for row, val in enumerate(values):
        if isinstance(val, datetime.datetime):
            datetime_values.append(val)
            continue
        if len(val) > 0 and (val[0].isdigit() or val == "-"):
            try:
                datetime_values.append(_to_datetime(val))
            except ValueError as err:
                # TBC: register exc !?
                if fixer is not None:
                    fixer.TableRow = row  # TBC: index
                    fix_value = fixer.fix_illegal_cell_value("datetime", val)
                    datetime_values.append(fix_value)
                else:
                    raise err
        else:
            if fixer is not None:
                fixer.TableRow = row  # TBC: index
                fix_value = fixer.fix_illegal_cell_value("datetime", val)
                datetime_values.append(fix_value)

    return np.array(datetime_values)


_column_parsers = defaultdict(lambda: _parse_float_column)
_column_parsers["text"] = _parse_text_column
_column_parsers["onoff"] = _parse_onoff_column
_column_parsers["datetime"] = _parse_datetime_column


def parse_column(unit_indicator: str, values: Iterable, fixer: FixFactory = None) -> np.ndarray:
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
