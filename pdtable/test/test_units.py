from textwrap import dedent
from typing import Optional

import numpy as np
import pytest
from pytest import fixture

from ..demo.unit_converter import convert_this
from ..io.parsers.blocks import make_table
from ..proxy import UnitConversionNotDefinedError


def test_demo_converter__converts_values():
    assert convert_this(1, "m", "mm") == 1000
    assert convert_this(0, "C", "K") == 273.16
    np.testing.assert_array_equal(
        convert_this(np.array([1, 42]), "m", "mm"), np.array([1000, 42000])
    )


@fixture
def table_cells():
    return [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
    **foo;
    all;
    diameter;mean_temp;no_conversion;remark;tod;
    mm;C;mm;text;datetime;
    42000;0;666;pretty cold;2020-10-09;
    1000;20;666;room temp;2020-10-09;
    """
        )
        .strip()
        .split("\n")
    ]


def test_convert_units__list(table_cells):
    t = make_table(table_cells)
    t.convert_units(to=["m", "K", None, None, None], converter=convert_this)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.16, 293.16]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__dict(table_cells):
    t = make_table(table_cells)
    t.convert_units(to={"diameter": "m", "mean_temp": "K"}, converter=convert_this)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.16, 293.16]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__callable(table_cells):
    def to_units_fun(table_name: str) -> Optional[str]:
        return {"diameter": "m", "mean_temp": "K"}.get(table_name)

    t = make_table(table_cells)
    t.convert_units(to=to_units_fun, converter=convert_this)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.16, 293.16]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__fails_on_inconvertible_unit(table_cells):
    t = make_table(table_cells)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a datetime
        t.convert_units(to=[None, None, None, "m", None], converter=convert_this)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a text
        t.convert_units(to=[None, None, None, None, "m"], converter=convert_this)


# TODO deal with NaN values in columns