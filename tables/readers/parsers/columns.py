import datetime
from collections import defaultdict

import numpy as np
import pandas as pd

from tables.readers.FixFactory import FixFactory


def _parse_text_column(values, fixer: FixFactory):
    return np.array(values, dtype=np.str)


_onoff_to_bool = {"0": False, "1": True, "-": False}


def _parse_onoff_column(values, fixer: FixFactory = None):
    bool_values = []
    for row, val in enumerate(values):
        if isinstance(val, bool) or isinstance(val, int):
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
    _float_converters_by_1st_char[ch] = lambda v: float(v)


def _parse_float_column(values, fixer: FixFactory = None):
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


def _parse_datetime_column(values, fixer: FixFactory = None):
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


column_parsers = defaultdict(lambda: _parse_float_column)
column_parsers["text"] = _parse_text_column
column_parsers["onoff"] = _parse_onoff_column
column_parsers["datetime"] = _parse_datetime_column
