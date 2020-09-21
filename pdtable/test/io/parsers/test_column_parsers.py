import datetime as dt

import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pytest import raises

from pdtable.io.parsers.columns import (
    normalize_if_str,
    is_missing_data_marker,
    _parse_onoff_column,
    _parse_float_column,
    _parse_datetime_column,
    _parse_text_column,
    parse_column,
)


def test_normalize_if_str():
    assert normalize_if_str(" NoRmALiZe me\t") == "normalize me"
    assert normalize_if_str("already normalized") == "already normalized"
    assert normalize_if_str(42) == 42
    assert normalize_if_str(None) is None


@pytest.mark.parametrize(
    "x,out",
    [
        ("-", True),
        ("NaN", True),
        ("nan", True),
        ("NAN", True),
        ("nAn", True),
        ("Non!", False),
        (None, False),
    ],
)
def test_is_missing_data_marker(x, out):
    assert is_missing_data_marker(x) == out


def test__parse_text_column():
    strings = ["foo", "bart", ""]
    max_str_length = max(len(s) for s in strings)
    col = _parse_text_column(strings)
    assert_array_equal(col, np.array(["foo", "bart", ""]))
    assert col.dtype == np.dtype(f"<U{max_str_length}")  # that's how numpy does dtype for strings


def test__parse_onoff_column():
    col = _parse_onoff_column([0, 1, False, True, "0", "1", " 0\t", "1 \n "])
    assert_array_equal(col, np.array([False, True, False, True, False, True, False, True,]))
    assert col.dtype == bool


def test__parse_onoff_column__panics_on_illegal_value():
    illegal_values = ["-", 2, -1]
    for x in illegal_values:
        with raises(ValueError):
            _parse_onoff_column([x])


def test__parse_float_column():
    col = _parse_float_column([-1, 42, "-1", "42", "-", "NaN", "nan"])
    assert_array_equal(col, np.array([-1, 42, -1, 42, np.nan, np.nan, np.nan]))
    assert col.dtype == float


def test__parse_float_column__panics_on_illegal_value():
    illegal_values = ["foo", "", None]
    for x in illegal_values:
        with raises(ValueError):
            _parse_float_column([x])


def test__parse_datetime_column():
    col = _parse_datetime_column(["2020-08-11", dt.datetime(2020, 8, 11, 11, 40)])
    assert_array_equal(
        col, np.array([pd.to_datetime("2020-08-11"), pd.to_datetime("2020-08-11 11:40")])
    )

    col = _parse_datetime_column(["-", "nan"])
    assert all(v is pd.NaT for v in col)


@pytest.mark.parametrize(
    "unit_indicator,values,expected",
    [
        ("text", ["yes", "", "no"], np.array(["yes", "", "no"])),
        (
            "onoff",
            [0, 1, False, True, "0", "1", " 0\t", "1 \n "],
            np.array([False, True, False, True, False, True, False, True,]),
        ),
        (
            "float",
            [-1, 42, "-1", "42", "-", "NaN", "nan"],
            np.array([-1, 42, -1, 42, np.nan, np.nan, np.nan]),
        ),
        (
            "datetime",
            ["2020-08-11", dt.datetime(2020, 8, 11, 11, 40)],
            np.array([pd.to_datetime("2020-08-11"), pd.to_datetime("2020-08-11 11:40")]),
        ),
    ],
)
def test__parse_column(unit_indicator, values, expected):
    assert_array_equal(parse_column(unit_indicator, values), expected)
