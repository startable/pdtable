from textwrap import dedent
from typing import Optional

import numpy as np
import pytest
from pint import DimensionalityError
from pytest import fixture, raises

from ..demo.unit_converter import convert_this
from ..io.parsers.blocks import make_table
from ..proxy import UnitConversionNotDefinedError
from pdtable.units.converter import convert_units, DefaultUnitConverter


def test_demo_converter__converts_values():
    assert convert_this(1, "m", "mm") == 1000
    assert convert_this(0, "C", "K") == 273.16
    np.testing.assert_array_equal(
        convert_this(np.array([1, 42]), "m", "mm"), np.array([1000, 42000])
    )


def test_default_converter__works():
    assert convert_units(1, "m", "mm") == 1000
    assert convert_units(0, "degC", "K") == 273.15
    with raises(DimensionalityError):
        # "C" means "Coulomb" in Pint's unit registry
        assert convert_units(0, "C", "K") == 273.15


class CustomUnitConverter(DefaultUnitConverter):
    def __init__(self):
        super().__init__()

    def __call__(self, value, from_unit, to_unit):
        # Let's say we think that "C" should mean "degrees Celsius" and not "Coulomb".
        custom_unit_symbols = {"C": "degC"}
        f = custom_unit_symbols.get(from_unit, from_unit)
        t = custom_unit_symbols.get(to_unit, to_unit)
        return super().__call__(value, f, t)


@fixture
def cuc():
    return CustomUnitConverter()


def test_custom_converter__works(cuc):
    # Pint units still work
    assert cuc(1, "m", "mm") == 1000
    assert cuc(0, "degC", "K") == 273.15
    # Units overridden in subclass work as intended
    assert cuc(0, "C", "K") == 273.15


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


def test_convert_units__list(table_cells, cuc):
    t = make_table(table_cells)
    t.convert_units(to=["m", "K", None, None, None], converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.15, 293.15]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__dict(table_cells, cuc):
    t = make_table(table_cells)
    t.convert_units(to={"diameter": "m", "mean_temp": "K"}, converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.15, 293.15]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__callable(table_cells, cuc):
    def to_units_fun(table_name: str) -> Optional[str]:
        return {"diameter": "m", "mean_temp": "K"}.get(table_name)

    t = make_table(table_cells)
    t.convert_units(to=to_units_fun, converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.15, 293.15]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


def test_convert_units__fails_on_inconvertible_unit(table_cells, cuc):
    t = make_table(table_cells)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a datetime
        t.convert_units(to=[None, None, None, "m", None], converter=cuc)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a text
        t.convert_units(to=[None, None, None, None, "m"], converter=cuc)


# TODO deal with NaN values in columns
