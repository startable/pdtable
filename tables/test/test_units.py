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
