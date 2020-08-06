import pandas as pd

from typing import Iterable


def _represent_row_elements(row: Iterable, col_units: Iterable, na_rep: str = "-"):
    """Prepares row element representations for writing.
    
    In preparation for writing, converts row values to representations compliant with 
    the StarTable standard, in accordance with the values' respective column units. 
    """
    for col, (val, unit) in enumerate(zip(row, col_units)):
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
                yield "-"  # some reasonable placeholder non-empty string
            else:
                # Ensure everything becomes a string, even non-string things
                yield str(val)
        else:
            # Leave everything else be as it is
            yield val