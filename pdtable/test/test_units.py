from textwrap import dedent
from typing import Optional

import numpy as np
import pandas as pd
import pytest
from pint import DimensionalityError, UndefinedUnitError
from pytest import fixture, raises

import pdtable
from pdtable.units.pint import PintUnitConverter, pint_converter
from ..demo.unit_converter import convert_this
from ..io.parsers.blocks import make_table
from ..proxy import UnitConversionNotDefinedError, MissingUnitConverterError


def test_demo_converter__works():
    # Converts single value
    assert convert_this(1, "m", "mm") == (1000, "mm")
    assert convert_this(0, "C", "K") == (273.15, "K")
    # Supports aliases
    assert convert_this(1000, "mm", "mètre") == (1, "m")
    # Returns NaN when given NaN
    new_val, _ = convert_this(np.nan, "m", "mm")
    assert np.isnan(new_val)
    # Converts array
    converted_vals, out_unit = convert_this(np.array([1, 42, np.nan]), "m", "mm")
    np.testing.assert_array_equal(converted_vals, np.array([1000, 42000, np.nan]))
    assert out_unit == "mm"
    # Converts to base unit by default
    assert convert_this(42_000, "mm") == (42, "m")
    # Fails when dimensionality error
    with raises(KeyError):
        convert_this(1, "m", "kg")
    # Fails when unknown units
    with raises(KeyError):
        convert_this(1, "m", "zonk")
    with raises(KeyError):
        convert_this(1, "gork", "m")
    with raises(KeyError):
        convert_this(1, "quxx")


def test_pint_converter__works():
    # Converts single value
    assert pint_converter(1, "m", "mm") == (1000, "millimeter")
    assert pint_converter(0, "degC", "K") == (273.15, "kelvin")
    # Returns NaN when given NaN
    new_val, _ = pint_converter(np.nan, "m", "mm")
    assert np.isnan(new_val)
    # Converts array
    converted_vals, out_unit = pint_converter(np.array([1, 42]), "m", "mm")
    np.testing.assert_array_equal(converted_vals, np.array([1000, 42000]))
    assert out_unit == "millimeter"
    # Converts to base unit by default
    assert convert_this(42_000, "mm") == (42, "m")
    # Fails when dimensionality error
    with raises(DimensionalityError):
        # "C" means "Coulomb" in Pint's unit registry
        pint_converter(0, "C", "K")  # Can't convert Coulomb to kelvin
    # Fails when unknown units
    with raises(UndefinedUnitError):
        pint_converter(1, "m", "zonk")
    with raises(UndefinedUnitError):
        pint_converter(1, "gork", "m")
    with raises(UndefinedUnitError):
        pint_converter(1, "quxx")


class CustomUnitConverter(PintUnitConverter):
    def __init__(self):
        super().__init__()

    def __call__(self, value, from_unit, to_unit="__base_unit__"):
        # Let's say we think that "C" should mean "degrees Celsius" and not "Coulomb".
        custom_unit_symbols = {"C": "degC"}
        # Translate supplied unit symbols into pint unit symbols (if translation is defined)
        f = custom_unit_symbols.get(from_unit, from_unit)
        t = custom_unit_symbols.get(to_unit, to_unit)
        return super().__call__(value, f, t)


@fixture
def cuc():
    return CustomUnitConverter()


def test_custom_converter__works(cuc):
    # Pint units still work
    assert cuc(1, "m", "mm") == (1000, "millimeter")
    assert cuc(0, "degC", "K") == (273.15, "kelvin")
    # Units overridden in subclass work as intended
    assert cuc(0, "C", "K") == (273.15, "kelvin")


@fixture
def table_cells():
    return [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
    **foo;
    all;
    diameter;mean_temp;depth;remark;measurement_date;
    mm;C;mm;text;datetime;
    42000;20;666;pretty cold;2020-10-09;
    1000;-;666;room temp;2020-10-09;
    """
        )
        .strip()
        .split("\n")
    ]


def test_convert_units__to_base_units(table_cells, cuc):
    t = make_table(table_cells).convert_units(to="base", converter=cuc)

    # Converted to base units
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "meter"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([293.15, np.nan]))
    assert t["mean_temp"].unit == "kelvin"
    np.testing.assert_array_equal(t["depth"].values, np.array([0.666, 0.666]))
    assert t["depth"].unit == "meter"
    # Columns with inconvertible units were skipped
    np.testing.assert_array_equal(t["remark"].values, np.array(["pretty cold", "room temp"]))
    assert t["remark"].unit == "text"
    assert all(x == pd.to_datetime("2020-10-09") for x in t["measurement_date"].values)
    assert t["measurement_date"].unit == "datetime"


def test_convert_units__list(table_cells, cuc):
    t = make_table(table_cells).convert_units(to=["m", "K", None, None, None], converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([293.15, np.nan]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["depth"].values, np.array([666, 666]))
    assert t["depth"].unit == "mm"


def test_convert_units__dict(table_cells, cuc):
    t = make_table(table_cells).convert_units(to={"diameter": "m", "mean_temp": "K"}, converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([293.15, np.nan]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["depth"].values, np.array([666, 666]))
    assert t["depth"].unit == "mm"


def test_convert_units__callable(table_cells, cuc):
    def to_units_fun(table_name: str) -> Optional[str]:
        return {"diameter": "m", "mean_temp": "K"}.get(table_name)

    t = make_table(table_cells).convert_units(to=to_units_fun, converter=cuc)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([293.15, np.nan]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["depth"].values, np.array([666, 666]))
    assert t["depth"].unit == "mm"


def test_convert_units__using_default_converter(table_cells, cuc):
    t_old_units = make_table(table_cells)

    assert pdtable.units.default_converter is None
    with raises(MissingUnitConverterError):
        # No default unit converter was set
        t_old_units.convert_units(to={"diameter": "m", "mean_temp": "K"})

    # Now set a default converter
    pdtable.units.default_converter = cuc
    # Do conversion; no explicitly specified unit converter; uses default
    t = t_old_units.convert_units(to={"diameter": "m", "mean_temp": "K"})

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([293.15, np.nan]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["depth"].values, np.array([666, 666]))
    assert t["depth"].unit == "mm"


def test_convert_units__fails_on_inconvertible_unit(table_cells, cuc):
    t = make_table(table_cells)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a datetime
        t.convert_units(to=[None, None, None, "m", None], converter=cuc)
    with pytest.raises(UnitConversionNotDefinedError):
        # Attempt to convert units of a text
        t.convert_units(to=[None, None, None, None, "m"], converter=cuc)
