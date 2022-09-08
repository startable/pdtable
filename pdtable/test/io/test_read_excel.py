import tempfile
from pathlib import Path

from ... import Table, write_excel
from ...io import read_excel
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
    t1.add_column("spelling", ["one", "six, as formula", "forty-two"], "text")

    t2 = Table(name="this_one_is_transposed")
    t2.add_column("diameter", [1.23], "cm")
    t2.add_column("melting_point", [273], "K")

    expected_tables = [t0, t1, t2]
    expected_transposed_flag = [False, False, True]

    # Read tables from file
    blocks = read_excel(Path(__file__).parent / "input" / "foo.xlsx")
    tables_read = [block for (block_type, block) in blocks if block_type == BlockType.TABLE]
    assert len(expected_tables) == len(tables_read)

    # Tables read are equal to the expected ones
    for te, tr, flag in zip(expected_tables, tables_read, expected_transposed_flag):
        assert te.equals(tr)
        assert tr.metadata.transposed == flag

    # test_read_excel__from_stream
    with open(Path(__file__).parent / "input" / "foo.xlsx", "rb") as fh:
        blocks = read_excel(fh)
        tables_read_stream = [
            block for (block_type, block) in blocks if block_type == BlockType.TABLE
        ]
        assert len(expected_tables) == len(tables_read_stream)

    # read from tempfile (#77)
    with tempfile.TemporaryFile() as f:
        write_excel(t0, f)
        f.seek(0)
        assert t0.equals(list(read_excel(f))[0][1])


def test_read_excel__applies_filter():

    # Make a filter
    def is_table_about_spelling(block_type: BlockType, block_name: str) -> bool:
        return block_type == BlockType.TABLE and "spelling" in block_name

    # Read blocks from file
    blocks = list(
        read_excel(Path(__file__).parent / "input" / "foo.xlsx", filter=is_table_about_spelling)
    )

    # Assert only that one table block was parsed
    assert len(blocks) == 1
    table: Table = blocks[0][1]
    assert table.name == "spelling_numbers"
