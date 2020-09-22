import pandas as pd

from pdtable.io._represent import _represent_row_elements


def test__represent_row_elements():
    units = ["text", "km", "datetime", "onoff"]
    # Standard stuff
    assert list(
        _represent_row_elements(("foo", 123, pd.to_datetime("2020-08-04 08:00"), True), units)
    ) == ["foo", 123, pd.to_datetime("2020-08-04 08:00"), 1]

    # With NaN-like things
    assert list(_represent_row_elements(("foo", float("nan"), pd.NaT, 1), units)) == [
        "foo",
        "-",
        "-",
        1,
    ]

    # Specifying how NaN-like things should be represented
    assert list(
        _represent_row_elements(("foo", float("nan"), pd.NaT, False), units, na_rep="NaN")
    ) == ["foo", "NaN", "NaN", 0]

    # Empty strings: replace illegal in first column, leave others
    assert list(_represent_row_elements(("", "", float("nan")), ["text", "text", "text"])) == [
        "-",
        "",
        "nan",
    ]
