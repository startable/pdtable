from textwrap import dedent

from pdtable import TableBundle
from pdtable.io.parsers.blocks import parse_blocks

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
    bundle = TableBundle(parse_blocks(cell_rows))

    assert bundle.foo.column.values[0] == "bar"

def test_TableBundle():

    # pdtable generator
    bundle = TableBundle(parse_blocks(cell_rows,to="pdtable"))
    assert bundle.input_files_derived.file_bytes.values[1] == 15326.0
    assert bundle is not None
    assert len(bundle) == 2

    # do not error on other table types
    bundle = TableBundle(parse_blocks(cell_rows,to="cellgrid"))

    assert bundle is not None
