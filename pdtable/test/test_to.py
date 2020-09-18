from pathlib import Path
from pdtable import BlockType, Table
from ..readers.parsers import parse_blocks


def input_dir() -> Path:
    return Path(__file__).parent / "input/with_errors"


def test_filter_parse_blocks():
    """ API test, parse_blocks:
        Verify that parse_blocks returns correct values for to in
           {"pdtable", "jsondata", "cellgrid"}
    """

    # fmt: off
    lines_input = [
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
        ["chicken" , 5     , 6     , 3.14    , 8   ],
        [ ],
    ]
    # fmt: on

    def filter(tp: BlockType, tn: str) -> bool:
        return tp == BlockType.TABLE and tn == "incl_2"

    cell_grid = []
    for tp, tt in parse_blocks(lines_input, filter=filter, to="cellgrid"):
        cell_grid.append(tt)

    jsondata = []
    for tp, tt in parse_blocks(lines_input, filter=filter, to="jsondata"):
        jsondata.append(tt)

    pdtables = []
    for tp, tt in parse_blocks(lines_input, filter=filter, to="pdtable"):
        pdtables.append(tt)

    assert len(cell_grid) == 1
    assert isinstance(cell_grid[0], list)
    assert cell_grid[0][4][3] == 3.14
    assert len(jsondata) == 1
    assert isinstance(jsondata[0], dict)
    assert jsondata[0]["columns"]["a1"][0] == 3.14
    assert len(pdtables) == 1
    assert isinstance(pdtables[0], Table)
    assert pdtables[0].df["a1"][0] == 3.14


def test_filter_read_csv():
    """ API test, read_csv:
        Verify that read_csv returns correct values for to in
           {"pdtable", "jsondata", "cellgrid"}
    """
    #
    # TTT TBD: test "to", "filter" on all high-level API's
    # read_bundle &c.
    pass
