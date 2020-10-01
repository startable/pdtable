from io import StringIO
from textwrap import dedent

from ..store import TableBundle
from ..io.parsers.blocks import parse_blocks


cell_rows = [
    line.split(";")
    # fmt off
    for line in dedent(
        r"""
        **foo
        all
        column
        text
        bar
        zoo

        ::;Table foo describes
        ;the fooness of things
        :.column;Column is a column in foo

        **input_files_derived;
        all;
        file_bytes;file_date;has_table;
        -;text;onoff;
        15373;a;0;
        15326;b;1;
        """
            )
            .strip()
            .split("\n")
    # fmt on
]

def test_bundle_from_csv():

    # with StringIO(lines) as f:
    table = TableBundle(parse_blocks(cell_rows))

    assert table.foo.column.values[0] == "bar"

def test_TableBundle():

    # pdtable generator
    table = TableBundle(parse_blocks(cell_rows,to="pdtable"))
#    assert table.input_files_derived.column.values[0] == "file_bytes"
    assert table is not None
    assert len(table) == 2

    # do not error on other table types
    table = TableBundle(parse_blocks(cell_rows,to="cellgrid"))

    assert table is not None
