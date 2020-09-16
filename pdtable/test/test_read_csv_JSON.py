import json
import os
from pathlib import Path
from textwrap import dedent

import pandas as pd

from ..json import StarTableJsonEncoder, json_data_to_table, table_to_json_data
from ..pandastable import make_pdtable
from ..readers.parsers.blocks import make_table, parse_blocks
from ..readers.read_csv import pdtable
from ..store import BlockType
from pdtable import json_data_to_table, table_to_json_data
from pdtable import make_pdtable
from pdtable import make_table, parse_blocks
from pdtable import Table, TableMetadata
from pdtable import BlockType


def input_dir() -> Path:
    return Path(__file__).parent / "input/with_errors"


def test_json_pdtable():
    """ ensure dict-obj to pdtable conversion
        compare to target created w. read_stream_csv
    """
    cell_rows = [
        line.split(";")
        for line in dedent(
            """\
        **farm_types1;;;
        your_farm my_farm farms_galore;;;
        species;  num;  flt;    log;
        text;       -;   kg;  onoff;
        chicken;    2;    3;      1;
        pig;        4;   39;      0;
        goat;       4;    -;      1;
        zybra;      4;    -;      0;
        cow;      NaN;  200;      1;
        goose;      2;    9;      0;
        """
        )
        .strip()
        .split("\n")
    ]
    pandas_pdtab = None
    # with io.StringIO(csv_src) as fh:
    g = parse_blocks(cell_rows, **{"origin": '"types1.csv" row 1'})
    for tp, tab in g:
        pandas_pdtab = tab

    table_data = {
        "name": "farm_types1",
        "columns": {
            "species": ["chicken", "pig", "goat", "zybra", "cow", "goose"],
            "num": [2.0, 4.0, 4.0, 4.0, None, 2.0],
            "flt": [3.0, 39.0, None, None, 200.0, 9.0],
            "log": [True, False, True, False, True, False],
        },
        "units": ["text", "-", "kg", "onoff"],
        "destinations": {"your_farm": None, "my_farm": None, "farms_galore": None},
        "origin": '"types1.csv" row 1',
    }
    json_pdtab = Table(
        make_pdtable(
            pd.DataFrame(table_data["columns"]),
            units=table_data["units"],
            metadata=TableMetadata(
                name=table_data["name"],
                destinations={dest for dest in table_data["destinations"]},
                origin=table_data["origin"],
            ),
        )
    )
    assert pandas_pdtab.equals(json_pdtab)


def test_json_data_to_pdtable():
    """ ensure dict-obj to pdtable conversion
        compare to target created w. make_table(List[List]])
    """
    lines_target = [
        ["**farm_types1"],
        ["your_farm my_farm farms_galore"],
        ["species", "num", "flt", "log"],
        ["text", "-", "kg", "onoff"],
        ["chicken", 2, 3, 1],
        ["pig", 4, 39, 0],
        ["goat", 4, None, 1],
        ["zybra", 4, None, 0],
        ["cow", None, 200, 1],
        ["goose", 2, 9, 0],
    ]

    pandas_pdtab = make_table(lines_target)

    table_data = {
        "name": "farm_types1",
        "columns": {
            "species": ["chicken", "pig", "goat", "zybra", "cow", "goose"],
            "num": [2.0, 4.0, 4.0, 4.0, None, 2.0],
            "flt": [3.0, 39.0, None, None, 200.0, 9.0],
            "log": [True, False, True, False, True, False],
        },
        "units": ["text", "-", "kg", "onoff"],
        "destinations": ["your_farm my_farm farms_galore"],
        "origin": '"types1.csv" row 1',
    }

    json_pdtab = json_data_to_table(table_data)
    assert pandas_pdtab.equals(json_pdtab)

    # reverse
    table_data_back = table_to_json_data(json_pdtab)
    json_pdtab_back = json_data_to_table(table_data)
    assert pandas_pdtab.equals(json_pdtab_back)


def test_FAT():
    """ Factory Acceptance Test

        Read input files as dictionary objects.
        Compare objects to stored target objects (input_dir() / all.json)

    """
    all_files = 0
    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ["auto_fixed.py", "__init__.py", "all.json", "all.csv"]:
            continue
        all_files += 1

    with open(input_dir() / "all.json") as f:
        all_json = json.load(f)

    count = 0
    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ["auto_fixed.py", "__init__.py", "all.json", "all.csv"]:
            continue
        with open(input_dir() / fn, "r") as fh:
            cell_rows = (line.rstrip("\n").split(";") for line in fh)
            g = parse_blocks(cell_rows, **{"origin": f'"{fn}"', "to": "jsondata"})

            for tp, tt in g:
                if tp == BlockType.TABLE:
                    """  compare generic object
                         i.e. containing None instead of pd.NaT, np.nan &c.
                    """
                    count += 1
                    assert tt == all_json[fn]

    assert count == all_files


from .._json import pure_json_obj
import numpy as np


def test_pure_json_obj():
    """ Unit test pure_json_obj / json_esc
    """
    obj = {
        "k1": 'k1 w. "quotes"',
        "nix": None,
        "no-flt": np.nan,
        "no-date": pd.NaT,
        "flt": 1.23,
        "int": 123,
    }
    json_obj = pure_json_obj(obj)
    # verify that json_obj is directly json serializable
    jstr = json.dumps(json_obj)
    assert len(jstr) > 0
    assert json_obj["k1"] == 'k1 w. "quotes"'
    assert json_obj["nix"] is None
    assert json_obj["no-flt"] is None
    assert json_obj["no-date"] is None
    assert json_obj["flt"] == 1.23
    assert json_obj["int"] == 123


def test_preserve_column_order():
    """ Unit test
        Verify that column order is preserved when translating btw. jsondata
        and pdtable.Table
    """
    # fmt: off
    lines_target = [
        ["**col_order"],
        ["dst2 dst2 dst2"],
        ["species" , "a3"  , "a2"  , "a1"  , "a4"  ],
        ["text"    , "-"   , "-"   , "-"   , "-"   ],
        ["chicken" , 1     , 2     , 3     , 4     ],
        ["pig"     , 1     , 2     , 3     , 4     ],
        ["goat"    , 1     , 2     , 3     , 4     ],
        ["zybra"   , 1     , 2     , 3     , 4     ],
        ["cow"     , 1     , 2     , 3     , 4     ],
        ["goose"   , 1     , 2     , 3     , 4     ],
    ]
    # fmt: on

    pandas_pdtab = make_table(lines_target)
    js_obj = table_to_json_data(pandas_pdtab)
    pdtab = json_data_to_table(js_obj)
    assert pdtab.df.iloc[0][3] == 3
    assert pdtab.df.iloc[1][1] == 1
    assert pdtab.df.iloc[2][4] == 4
    assert pandas_pdtab.equals(pdtab)

    # now verify from json-string
    jstr = json.dumps(js_obj)
    print(jstr)
    js_obj_from_json = json.loads(jstr)
    pdtab_from_json = json_data_to_table(js_obj_from_json)

    assert pandas_pdtab.equals(pdtab_from_json)