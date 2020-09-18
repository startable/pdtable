from pathlib import Path

from ... import Table
from ...readers import read_excel
from ...store import BlockType


def test_read_excel():

    # Prepare the expected tables.
    # Note: deliberately not testing datetime columns due to upstream bug in openpyxl:
    # timestamps are off by one microsecond.
    # https://foss.heptapod.net/openpyxl/openpyxl/-/issues/1493

    t0 = Table(name="places_to_go")
    t0["place"] = ["home", "work", "beach", "wonderland"]
    t0.add_column("distance", list(range(3)) + [float("nan")], "km")
    t0.add_column("is_hot", [True, False, True, False], "onoff")

    t1 = Table(name="spelling_numbers")
    t1.add_column("number", [1, 6, 42], "-")
    t1.add_column("spelling", ["one", "six", "forty-two"], "text")

    expected_tables = [t0, t1]

    # Read tables from file
    blocks = read_excel(Path(__file__).parent / "input" / "foo.xlsx")
    tables_read = [block for (block_type, block) in blocks if block_type == BlockType.TABLE]
    assert len(expected_tables) == len(tables_read)

    # Assert read tables are equal to the expected ones
    for te, tr in zip(expected_tables, tables_read):
        assert te.equals(tr)


def test_read_excel__applies_filter():

    # Make a filter
    # def is

    # Read tables from file
    blocks = list(read_excel(Path(__file__).parent / "input" / "foo.xlsx"))
