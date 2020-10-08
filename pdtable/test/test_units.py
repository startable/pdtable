from textwrap import dedent

import numpy as np

from ..demo.unit_converter import convert_this
from ..io.parsers.blocks import make_table


def test_demo_converter__converts_values():
    assert convert_this(1, "m", "mm") == 1000
    assert convert_this(0, "C", "K") == 273.16
    np.testing.assert_array_equal(
        convert_this(np.array([1, 42]), "m", "mm"), np.array([1000, 42000])
    )


def test_convert_units():

    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
    **foo;
    all;
    diameter;mean_temp;no_conversion;
    mm;C;mm;
    42000;0;666;
    1000;20;666;
    """
        )
        .strip()
        .split("\n")
    ]
    t = make_table(cells)

    t.convert_units(to={"diameter": "m", "mean_temp": "K"}, converter=convert_this)

    # Conversion done on columns as requested
    np.testing.assert_array_equal(t["diameter"].values, np.array([42, 1]))
    assert t["diameter"].unit == "m"
    np.testing.assert_array_equal(t["mean_temp"].values, np.array([273.16, 293.16]))
    assert t["mean_temp"].unit == "K"

    # Column for which no conversion was requested stays unchanged
    np.testing.assert_array_equal(t["no_conversion"].values, np.array([666, 666]))
    assert t["no_conversion"].unit == "mm"


