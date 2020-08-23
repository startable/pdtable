from io import StringIO
from textwrap import dedent

import tables.proxy
from ..readers.read_csv import make_metadata_block, make_directive, make_table, read_stream_csv
from ..store import TableBundle, BlockType


def test_make_metadata_block():
    lines = dedent("""\
    author:;XYODA;
    purpose:;Save the galaxy
    """).strip().split("\n")
    ml = make_metadata_block(lines, ";")
    assert ml["author"] == "XYODA"
    assert ml["purpose"] == "Save the galaxy"


def test_make_directive():
    lines = dedent("""\
    ***foo
    bar
    baz
    """).strip().split("\n")
    d = make_directive(lines, ";")
    assert d.name == "foo"
    assert d.lines == ["bar", "baz"]


def test_make_table():
    lines = (
        dedent(
            r"""
    **input_files_derived;
    all;
    file_bytes;file_date;file_name;has_table;
    -;text;text;onoff;
    15373;20190516T104445;PISA_Library\results\check_Soil_Plastic_ULS1-PISA_C1.csv;1;
    15326;20190516T104445;PISA_Library\results\check_Soil_Plastic_ULS1-PISA_C2.csv;1;
    """
        )
        .strip()
        .split("\n")
    )
    t = make_table(lines, ";").df
    assert t.file_bytes[0] == 15373

    tt = tables.proxy.Table(t)
    assert tt.name == 'input_files_derived'
    assert set(tt.metadata.destinations) == {'all'}
    assert tt.units == ['-', 'text', 'text', 'onoff']


def test_make_table__parses_onoff_column():
    lines = dedent(r"""
    **input_files_derived;
    all;
    file_bytes;file_date;has_table;
    -;text;onoff;
    15373;a;0;
    15326;b;1;
    """
        ).strip().split("\n")

    table_df = make_table(lines, ";").df
    assert table_df.file_bytes[0] == 15373
    assert table_df.has_table[0] == False
    assert table_df.has_table[1] == True
    tt = tables.proxy.Table(table_df)
    assert tt.name == 'input_files_derived'
    assert set(tt.metadata.destinations) == {'all'}
    assert tt.units == ['-', 'text', 'onoff']


def test_make_table__no_trailing_sep():
    lines=dedent(r"""
        **foo
        all
        column;pct;dash;mm;
        text;%;-;mm;
        bar;10;10;10;
        """
        ).strip().split("\n")

    t = make_table(lines, ";").df
    assert t.column[0] == "bar"
    assert t.dash[0] == 10


def test_read_stream_csv():
    lines = dedent("""\
        author: ;XYODA     ;
        purpose:;Save the galaxy
    
        ***gunk
        grok
        jiggyjag
        
        **foo
        all
        column;pct;dash;mm;
        text;%;-;mm;
        bar;10;10;10;
        
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

    with StringIO(lines) as f:
        blocks = [b for b in read_stream_csv(f, sep=';')]
        assert len(blocks) == 10  # includes blanks

    metadata_blocks = [b for t, b in blocks if t == BlockType.METADATA]
    assert len(metadata_blocks) == 1
    mb = metadata_blocks[0]
    assert len(mb) == 2
    assert mb["author"] == "XYODA"
    assert mb["purpose"] == "Save the galaxy"

    directives = [b for t, b in blocks if t == BlockType.DIRECTIVE]
    assert len(directives) == 1
    d = directives[0]
    assert d.name == "gunk"
    assert d.lines == ["grok", "jiggyjag"]

    with StringIO(lines) as f:
        table = TableBundle(read_stream_csv(f, sep=";"))

    assert table.foo.column.values[0] == "bar"
    assert table.foo.dash.values[0] == 10
