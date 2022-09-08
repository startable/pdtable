from itertools import repeat

import pandas as pd

from typing import Iterable


def _represent_row_elements(row: Iterable, units: Iterable, na_rep: str = "-"):
    """Prepares row element representations for writing.

    In preparation for writing, coerce row values to representations compliant with
    the StarTable standard, in accordance with the values' respective column units:

    - NaN-like things in non-text columns are coerced to the specified nan representation
    - 'onoff' column values are coerced to 0's and 1's (where possible)
    - 'text' column values are coerced to strings
    - If the first column is 'text', its empty strings are replaced with an arbitrary but
      reasonable sealant

    Values are not, in general, converted to strings. If writing to a string format,
    stringification must be done by the client code.
    """
    for col, (val, unit) in enumerate(zip(row, units)):
        if unit != "text" and pd.isna(val):
            # Represent NaN-like things, except leave them be in text columns
            yield na_rep
        elif unit == "onoff":
            # Represent obvious booleans as 0's and 1's
            if val in [True, 1]:
                yield 1
            elif val in [False, 0]:
                yield 0
            else:
                # If it isn't an obvious boolean, leave it be
                yield val
        elif unit == "text":
            if val == "" and col == 0:
                # Prevent illegal empty string in first column
                yield "-"  # some arbitrary but reasonable sealant
            else:
                # Coerce everything to strings
                yield str(val)
        else:
            # Leave everything else be as it is
            yield val


def _represent_col_elements(values: Iterable, unit: str, na_rep: str = "-"):
    """Prepare column value representations for writing"""
    # Let's be lazy and just reuse the row code, sending it the same unit forever
    yield from _represent_row_elements(values, repeat(unit), na_rep)
