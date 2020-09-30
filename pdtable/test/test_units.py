from typing import Tuple, Any

import pytest
from ..units import UnitPolicy
from ..io.parsers.blocks import make_table
from textwrap import dedent


class SuperSimpleUnitPolicy(UnitPolicy):
    def convert_value_to_base(self, value, unit: str) -> Tuple[Any, str]:
        if unit == "mm":
            return value * 1e-3, "m"
        else:
            return value, unit


@pytest.fixture
def unit_policy() -> UnitPolicy:
    return SuperSimpleUnitPolicy()


def test_unit_policy__converts_values(unit_policy):
    assert unit_policy.convert_value_to_base(1, "mm") == (1e-3, "m")
    assert unit_policy.convert_value_to_base("test", "text") == ("test", "text")


def test_convert_units(unit_policy):

    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
    **input_files_derived;
    all;
    file_bytes;file_date;has_table;length;
    -;text;onoff;mm;
    15373;a;0;1;
    15326;b;1;2;
    """
        )
        .strip()
        .split("\n")
    ]
    t = make_table(cells)

    t.convert_units(unit_policy)

    assert t["length"].values[0] == 1e-3
    assert t["length"].unit == "m"


class MoreComplexUnitPolicy(UnitPolicy):
    """ Unit conversion based on column_name and table_name
    """

    def convert_value_to_base(self, value, unit: str) -> Tuple[Any, str]:
        """ Here any unit converter can be integrated, pint, Unum &c.
            This converter demonstrates the use of table_name and column_name
        """
        if self.table_name != "input_files_derived":
            return value, unit

        if self.column_name == "length":
            if unit == "mm":
                return value * 1e-3, "m"
        elif self.column_name == "flt":
            if unit == "m":
                return value * 100, "cm"
        print(f"{self.column_name} {value} {unit}")
        return value, unit


def test_convert_units__with_more_complex_unit_policy():
    # fmt off
    cells = [
        ["**input_files_derived"],
        ["all"],
        ["file_bytes", "file_date", "has_table", "length", "flt"],
        ["-", "text", "onoff", "mm", "m"],
        [15373, "a", 0, 1, 22.4],
        [15326, "b", 1, 2, 21.7],
    ]
    # fmt on
    t = make_table(cells)
    t.convert_units(MoreComplexUnitPolicy())

    assert t["length"].values[0] == 1e-3
    assert t["length"].unit == "m"
    assert t["flt"].values[0] == 2240.0
    assert t["flt"].unit == "cm"

    # fmt off
    cells2 = [
        ["**input_2"],
        ["all"],
        ["file_bytes", "file_date", "has_table", "length"],
        ["-", "text", "onoff", "mm"],
        [15373, "a", 0, 1],
        [15326, "b", 1, 2],
    ]
    # fmt on

    t_ident = make_table(cells2)
    t_ident.convert_units(MoreComplexUnitPolicy())

    assert t_ident["length"].values[0] == 1
    assert t_ident["length"].unit == "mm"
