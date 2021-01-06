import numpy as np
import pandas as pd
import datetime as dt
from textwrap import dedent

from pdtable import Table, TableBundle, BlockType
from pdtable import ParseFixer
from ....io.parsers.blocks import (
    make_metadata_block,
    make_directive,
    make_table,
    parse_blocks,
)


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
    lines = [
        ["**foo", None, None, None],
        ["all", None, None, None],
        ["place", "distance", "ETA", "is_hot"],
        ["text", "km", "datetime", "onoff"],
        ["home", 0.0, dt.datetime(2020, 8, 4, 8, 0, 0), 1],
        ["work", 1.0, dt.datetime(2020, 8, 4, 9, 0, 0), 0],
        ["beach", 2.0, dt.datetime(2020, 8, 4, 17, 0, 0), 1],
        ["wonderland", "-", "-", "FALSE"],
    ]

    t = make_table(lines)

    assert t.name == "foo"
    assert set(t.metadata.destinations) == {"all"}
    assert t.column_names == ["place", "distance", "ETA", "is_hot"]
    assert t.units == ["text", "km", "datetime", "onoff"]

    df = pd.DataFrame(
        [
            ["home", 0.0, dt.datetime(2020, 8, 4, 8, 0, 0), True],
            ["work", 1.0, dt.datetime(2020, 8, 4, 9, 0, 0), False],
            ["beach", 2.0, dt.datetime(2020, 8, 4, 17, 0, 0), True],
            ["wonderland", np.nan, np.nan, False],
        ],
        columns=["place", "distance", "ETA", "is_hot"],
    )
    pd.testing.assert_frame_equal(t.df, df)


def test_make_table__with_backslashes():
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
    assert not table_df.has_table[0]
    assert table_df.has_table[1]
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


def test_make_table__empty_values():
    lines = [
        ["**foo"],
        ["all"],
        ["place", "distance"],
        ["text", "km"],
    ]

    t = make_table(lines)

    assert t.name == "foo"
    assert set(t.metadata.destinations) == {"all"}
    assert t.column_names == ["place", "distance"]
    assert t.units == ["text", "km"]

    df = pd.DataFrame({"place": [], "distance": []})
    pd.testing.assert_frame_equal(t.df, df)


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

    blocks = list(parse_blocks(cell_rows))

    metadata_blocks = [b for t, b in blocks if t == BlockType.METADATA]
    assert len(metadata_blocks) == 1
    mb = metadata_blocks[0]
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
    table_bundle = TableBundle(parse_blocks(cell_rows), as_dataframe=True)
    assert table_bundle.foo.column.values[0] == "bar"
    assert table_bundle.foo.dash.values[0] == 10


def test_parse_blocks__filters_correctly():
    """ Unit test
        Verify that misc. float input are handled consistently
    """
    # Make some input data cell rows
    # fmt: off
    cell_rows = [
        ["**incl_1"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
        ["**xcl_1"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
        ["**incl_2"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
        ["**xcl_2"],
        ["dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],
    ]
    # fmt: on

    # Make some interesting block filter
    def table_whose_name_starts_with_i(tp: BlockType, tn: str) -> bool:
        """Only keeps table blocks whose name starts with 'i', filters out everything else"""
        return tp == BlockType.TABLE and tn[0] == "i"

    # Filter and parse the blocks
    tables = [b for _, b in parse_blocks(cell_rows, filter=table_whose_name_starts_with_i)]

    assert len(tables) == 2
    assert tables[0].name == "incl_1"


def test_parse_blocks__filters_correctly_for_different_output_types():
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
    def table_asking_to_be_kept(tp: BlockType, tn: str) -> bool:
        """Only keeps table blocks named 'keep_me!', filters out everything else"""
        return tp == BlockType.TABLE and tn == "keep_me!"

    # Parse the input, filtering it using that filter, and returning different output types
    cell_grid = [
        b for _, b in parse_blocks(cell_rows, filter=table_asking_to_be_kept, to="cellgrid")
    ]
    json_data = [
        b for _, b in parse_blocks(cell_rows, filter=table_asking_to_be_kept, to="jsondata")
    ]
    tables = [b for _, b in parse_blocks(cell_rows, filter=table_asking_to_be_kept, to="pdtable")]

    # Expected result from filtering and parsing to those different output types?
    assert len(cell_grid) == 1
    assert isinstance(cell_grid[0], list)
    assert cell_grid[0][4][3] == 3.14

    assert len(json_data) == 1
    assert isinstance(json_data[0], dict)
    assert json_data[0]["columns"]["a1"]["values"][0] == 3.14

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

    # fmt off
    cell_rows = [
        ["**test_input"],
        ["all"],
        ["numerical", "dates", "onoffs"],
        ["-", "datetime", "onoff"],
        [123, "08/07/2020", 0],
        [123, "08-07-2020", 1],
        [123, "08-07-2020", 1],
        [1.23, None, None],
        [1.23, None, None],
        [1.23, None, None],
        [-1.23, None, None],
        [1.23, None, None],
    ]
    # fmt on
    fix = ParseFixer()
    fix.stop_on_errors = False
    fix._called_from_test = True
    table_bundle = TableBundle(parse_blocks(cell_rows, fixer=fix), as_dataframe=True)
    assert table_bundle

    assert not table_bundle.test_input.onoffs[0]
    assert table_bundle.test_input.onoffs[1]
    assert table_bundle.test_input.onoffs[2]
    for idx in range(0, 3):
        assert table_bundle.test_input.dates[idx].year == 2020
        assert table_bundle.test_input.dates[idx].month == 7
        assert table_bundle.test_input.dates[idx].day == 8

    for idx in range(0, 3):
        assert table_bundle.test_input.numerical[idx] == 123

    assert table_bundle.test_input.numerical[3] == 1.23
    assert table_bundle.test_input.numerical[5] == 1.23
    assert table_bundle.test_input.numerical[7] == 1.23
    assert table_bundle.test_input.numerical[6] == -1.23


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

    table_bundle = TableBundle(parse_blocks(cell_rows), as_dataframe=True)
    assert table_bundle

    assert not table_bundle.test_input.onoffs[0]
    assert table_bundle.test_input.dates[0].year == 2020
    assert table_bundle.test_input.numerical[0] == 123


def test_parse_blocks__block_types():
    """ Unit test
        Verify that block types are  are handled consistently
    """
    # fmt: off
    cell_rows = [
        ["author: ", "XYODA"                       ],
        ["purpose:", "Save the galaxy"             ],
        ["**tab_1"                                 ],
        ["all" ,                                   ],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        [ ],                                           # term. by newline
        ["**tab_2"                                 ],
        ["all" ,                                   ],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        ["**tab_3" ,                               ],  # term. by table
        ["all" ,                                   ],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        ["***foo"],                                    # term. by directive
        ["bar"],
        ["baz"],
        [":template", "whatnot?"],
        [],
        ["# extra lines 1", "check type"],             # BLANK 1
        ["# extra lines 2"],
        ["**tab_4"                                 ],
        ["all"                                     ],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        ["meta-test1:", "test meta not in header"  ],  # BLANK 2
        ["meta-test2:", "test meta not in header"  ],  # BLANK 3
        ["**tab_5"                                 ],
        ["all"                                     ],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        ["***foo2"],
        ["bar"],
        ["baz"],
    ]
    # fmt: on

    seen = {}
    for ty, block in parse_blocks(cell_rows, to="cellgrid"):
        #  print(f"\n-oOo- {ty} {block}")
        if seen.get(ty) is None:
            seen[ty] = []
        seen[ty].append(block)

    assert len(seen[BlockType.METADATA]) == 1
    assert len(seen[BlockType.TABLE]) == 5
    assert len(seen[BlockType.DIRECTIVE]) == 2
    assert len(seen[BlockType.TEMPLATE_ROW]) == 1
    assert len(seen[BlockType.BLANK]) == 3


def test_parse_blocks__test_demo():
    """ Unit test
        Verify that block types are  are handled consistently
    """
    # fmt: off
    cell_rows = [
       ["author:" ,"XYODA"                     ],
       ["purpose:","Save the galaxy"           ],
       [                                       ],
       ["***gunk"                              ],
       ["grok"                                 ],
       ["jiggyjag"                             ],
       [                                       ],
       ["**places",                            ],
       ["all"                                  ],
       ["place","distance","ETA","is_hot"      ],
       ["text","km","datetime","onoff"         ],
       ["home",0.0,"2020-08-04 08:00:00",1     ],
       ["work",1.0,"2020-08-04 09:00:00",0     ],
       ["beach",2.0,"2020-08-04 17:00:00",1    ],
       [                                       ],
       ["**farm_animals"                       ],
       ["your_farm my_farm other_farm"         ],
       ["species","n_legs","avg_weight"        ],
       ["text","-","kg"                        ],
       ["chicken",2,2                          ],
       ["pig",4,89                             ],
       ["cow",4,200                            ],
       ["unicorn",4,None                       ],
     ]
    # fmt: on

    seen = {}
    for ty, block in parse_blocks(cell_rows, to="cellgrid"):
        # print(f"\n-oOo- {ty} {block}")
        if seen.get(ty) is None:
            seen[ty] = []
        seen[ty].append(block)

    assert len(seen.get(BlockType.METADATA)) == 1
    assert len(seen.get(BlockType.TABLE)) == 2
    assert len(seen.get(BlockType.DIRECTIVE)) == 1
    assert seen.get(BlockType.TEMPLATE_ROW) is None
    assert seen.get(BlockType.BLANK) is None
