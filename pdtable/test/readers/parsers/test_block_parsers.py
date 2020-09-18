from io import StringIO

from textwrap import dedent
from pdtable.proxy import Table
from pdtable.readers.parsers.blocks import (
    make_metadata_block,
    make_directive,
    make_table,
    parse_blocks,
)
from pdtable.store import TableBundle, BlockType


def test_make_metadata_block():
    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            """\
    author:;XYODA;
    purpose:;Save the galaxy
    """
        )
        .strip()
        .split("\n")
    ]
    ml = make_metadata_block(cells, ";")
    assert ml["author"] == "XYODA"
    assert ml["purpose"] == "Save the galaxy"


def test_make_directive():
    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            """\
    ***foo;
    bar;
    baz;
    """
        )
        .strip()
        .split("\n")
    ]
    d = make_directive(cells)
    assert d.name == "foo"
    assert d.lines == ["bar", "baz"]


def test_make_table():
    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
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
    ]

    t = make_table(cells).df
    assert t.file_bytes[0] == 15373

    tt = Table(t)
    assert tt.name == "input_files_derived"
    assert set(tt.metadata.destinations) == {"all"}
    assert tt.units == ["-", "text", "text", "onoff"]


def test_make_table__parses_onoff_column():
    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
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
    ]

    table_df = make_table(cells).df
    assert table_df.file_bytes[0] == 15373
    assert table_df.has_table[0] == False
    assert table_df.has_table[1] == True
    tt = Table(table_df)
    assert tt.name == "input_files_derived"
    assert set(tt.metadata.destinations) == {"all"}
    assert tt.units == ["-", "text", "onoff"]


def test_make_table__no_trailing_sep():
    cells = [
        [cell.strip() for cell in line.split(";")]
        for line in dedent(
            r"""
        **foo
        all
        column;pct;dash;mm;
        text;%;-;mm;
        bar;10;10;10;
        """
        )
        .strip()
        .split("\n")
    ]

    t = make_table(cells).df
    assert t.column[0] == "bar"
    assert t.dash[0] == 10


def test_parse_blocks():
    cell_rows = [
        line.split(";")
        for line in dedent(
            """\
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
        .strip()
        .split("\n")
    ]

    blocks = [b for b in parse_blocks(cell_rows)]
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

    tabs = [b for t, b in blocks if t == BlockType.TABLE]
    assert len(tabs) == 2
    t = tabs[0]
    assert t.name == "foo"
    assert t.df["column"].iloc[0] == "bar"

    # Bundle
    table_bundle = TableBundle(parse_blocks(cell_rows))
    assert table_bundle.foo.column.values[0] == "bar"
    assert table_bundle.foo.dash.values[0] == 10


def test_parse_blocks__filters_correctly():
    """ API test, parse_blocks:
        Verify that parse_blocks filters correctly and parses correctly to different output types
    """

    # Make some input data cell rows
    # fmt: off
    cell_rows = [
        ["**ignore_me"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
        ["**ignore_me_2"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
        ["**keep_me!"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 5     , 6     , 3.14    , 8   ],
        [ ],
    ]
    # fmt: on

    # Make some interesting block filter
    def my_table_filter(tp: BlockType, tn: str) -> bool:
        """Only keeps table blocks named 'keep_me!', filters out everything else"""
        return tp == BlockType.TABLE and tn == "keep_me!"

    # Parse the input, filtering it using that filter, and returning different output types
    cell_grid = []
    for tp, tt in parse_blocks(cell_rows, filter=my_table_filter, to="cellgrid"):
        cell_grid.append(tt)

    json_data = []
    for tp, tt in parse_blocks(cell_rows, filter=my_table_filter, to="jsondata"):
        json_data.append(tt)

    tables = []
    for tp, tt in parse_blocks(cell_rows, filter=my_table_filter, to="pdtable"):
        tables.append(tt)

    # Expected result from filtering and parsing to those different output types?
    assert len(cell_grid) == 1
    assert isinstance(cell_grid[0], list)
    assert cell_grid[0][4][3] == 3.14

    assert len(json_data) == 1
    assert isinstance(json_data[0], dict)
    assert json_data[0]["columns"]["a1"][0] == 3.14

    assert len(tables) == 1
    assert isinstance(tables[0], Table)
    assert tables[0].df["a1"][0] == 3.14


def test_filter_read_csv():
    """ API test, read_csv:
        Verify that read_csv returns correct values for to in
           {"pdtable", "jsondata", "cellgrid"}
    """
    #
    # TTT TBD: test "to", "filter" on all high-level API's
    # read_bundle &c.
    pass


def test_read_csv_compatible1():
    """
      test_read_csv_compatible

      handle '-' in cells
      handle leading and trailing wsp
    """

    cell_rows = [
        line.split(";")
        for line in dedent(
            r"""
    **test_input;
    all;
    numerical;dates;onoffs;
    -;datetime;onoff;
    123;08/07/2020;0;
     123; 08-07-2020; 1;
     123 ; 08-07-2020 ; 1 ;
    1.23;-;-;
     1.23; -; -;
     1.23 ; - ; - ;
     -1.23 ; - ; - ;
     +1.23 ; - ; - ;
    """
        )
        .strip()
        .split("\n")
    ]

    table = TableBundle(parse_blocks(cell_rows))
    assert table

    assert table.test_input.onoffs[0] == False
    assert table.test_input.onoffs[1] == True
    assert table.test_input.onoffs[2] == True
    for idx in range(0, 3):
        assert table.test_input.dates[idx].year == 2020
        assert table.test_input.dates[idx].month == 7
        assert table.test_input.dates[idx].day == 8

    for idx in range(0, 3):
        assert table.test_input.numerical[idx] == 123

    assert table.test_input.numerical[3] == 1.23
    assert table.test_input.numerical[5] == 1.23
    assert table.test_input.numerical[7] == 1.23
    assert table.test_input.numerical[6] == -1.23


def test_read_csv_compatible2():
    """
      test_read_csv_compatible2

      handle leading and trailing wsp in column_name, unit
    """

    cell_rows = [
        line.split(";")
        for line in dedent(
            r"""
    **test_input;
    all;
    numerical ; dates; onoffs ;
     - ; datetime;onoff ;
    123;08/07/2020;0;
    """
        )
        .strip()
        .split("\n")
    ]

    table = TableBundle(parse_blocks(cell_rows))
    assert table

    assert table.test_input.onoffs[0] == False
    assert table.test_input.dates[0].year == 2020
    assert table.test_input.numerical[0] == 123
