from typing import Tuple, Any

import pytest
from ..units import UnitPolicy, normalize_table_in_place
from ..readers.read_csv import make_table
from textwrap import dedent


class MockUnitPolicy(UnitPolicy):
    def convert_value_to_base(self, value, unit: str) -> Tuple[Any, str]:
        if unit == "mm":
            return value * 1e-3, "m"
        else:
            return value, unit


@pytest.fixture
def unit_policy() -> UnitPolicy:
    return MockUnitPolicy()


def test_convert_values(unit_policy):
    assert unit_policy.convert_value_to_base(1, "mm") == (1e-3, "m")
    assert unit_policy.convert_value_to_base("test", "text") == ("test", "text")


def test_update_table(unit_policy):

    lines = (
        dedent(
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
    )
    t = make_table(lines, ";")

    normalize_table_in_place(unit_policy, t)

    assert t["length"].values[0] == 1e-3
    assert t["length"].unit == "m"


class myUnitPolicy(UnitPolicy):
    """ Unit conversion based on TableColumn and TableName
    """
    def convert_value_to_base(self, value, unit: str) -> Tuple[Any, str]:
        """ Here any unit converter can be integrated, pint, Unum &c.
            This converter demonstrates the use of TableName and TableColumn
        """
        if self.TableName != "input_files_derived":
            return value, unit

        if self.TableColumn == "length":
            if unit == "mm":
                return value * 1e-3, "m"
        elif self.TableColumn == "flt":
            if unit == "m":
                return value * 100, "cm"
        print(f"{self.TableColumn} {value} {unit}")
        return value, unit


def test_UnitPolicy():
    lines = (
        dedent(
            r"""
    **input_files_derived;
    all;
    file_bytes;file_date;has_table;length;flt
    -;text;onoff;mm;m;
    15373;a;0;1;22.4;
    15326;b;1;2;21.7;
    """
        )
        .strip()
        .split("\n")
    )
    t = make_table(lines, ";")

    normalize_table_in_place(myUnitPolicy(), t)

    assert t["length"].values[0] == 1e-3
    assert t["length"].unit == "m"
    assert t["flt"].values[0] == 2240.0
    assert t["flt"].unit == "cm"

    lines = (
        dedent(
            r"""
    **input_2;
    all;
    file_bytes;file_date;has_table;length;
    -;text;onoff;mm;
    15373;a;0;1;
    15326;b;1;2;
    """
        )
        .strip()
        .split("\n")
    )
    t_ident = make_table(lines, ";")
    normalize_table_in_place(myUnitPolicy(), t_ident)

    assert t_ident["length"].values[0] == 1
    assert t_ident["length"].unit == "mm"
