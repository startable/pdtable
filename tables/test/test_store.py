from io import StringIO
from textwrap import dedent

from foundation_core.tables import TableBundle
from foundation_core.tables.readers.read_csv import read_stream_csv


def test_bundle_from_csv():
    lines = dedent(r"""
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
    """)

    with StringIO(lines) as f:
        table = TableBundle(read_stream_csv(f, sep=';'))

    assert table.foo.column.values[0] == 'bar'
