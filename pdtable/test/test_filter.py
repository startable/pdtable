import json
import os
from pathlib import Path

from pdtable import parse_blocks, BlockType


def input_dir() -> Path:
    return Path(__file__).parent / "input/with_errors"


def test_filter_make_table():
    """ Unit test
        Verify that misc. float input are handled consistently
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
    def filter(tp: BlockType, tn: str) -> bool:
        return tp == BlockType.TABLE and tn[0] == "i"

    tables = []
    for tp, tt in parse_blocks(lines_input, filter=filter):
        tables.append(tt)

    assert len(tables) == 2
    assert tables[0].name == "incl_1"
