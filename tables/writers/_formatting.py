from typing import Iterable


def _format_row_elements(row: Iterable, col_units: Iterable, na_rep: str = "-"):
    """Formats row elements to comply with the StarTable standard when written to CSV. 
    """
    for col, (val, unit) in enumerate(zip(row, col_units)):
        if unit != "text" and pd.isna(val):
            # Format NaN-like things, except leave them be in text columns
            yield na_rep
        elif unit == "onoff":
            # Format obvious booleans as 0's and 1's
            if val in [True, 1]:
                yield "1"
            elif val in [False, 0]:
                yield "0"
            else:
                # If it isn't an obvious boolean, leave it be
                yield str(val)
        elif unit == "text" and val == "" and col == 0:
            # Prevent illegal empty string in first column
            yield "-"  # some reasonable placeholder non-empty string
        else:
            # Leave everything else be as it is
            yield str(val)