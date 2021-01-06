import json
import os
from pathlib import Path
from textwrap import dedent

import numpy as np
import pandas as pd

from pdtable import BlockType, ParseFixer
from pdtable.io import json_data_to_table, table_to_json_data
from pdtable.io._json import to_json_serializable
from pdtable.io.parsers import parse_blocks
from pdtable.io.parsers.blocks import make_table, make_table_json_data


class custom_test_fixer(ParseFixer):
    def __init__(self):
        ParseFixer.__init__(self)
        self.stop_on_errors = False
        self._called_from_test = True


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
    g = parse_blocks(cell_rows, **{"origin": '"types1.csv" row 1'}, fixer=custom_test_fixer)
    for tp, tab in g:
        pandas_pdtab = tab
    # fmt: off
    table_json_data = {
      "name": "farm_types1",
      "columns": {
         "species": {"unit": "text",
                     "values": ["chicken", "pig", "goat", "zybra", "cow", "goose"]},
         "num": {"unit": "-",
                 "values": [2.0, 4.0, 4.0, 4.0, None, 2.0]},
         "flt": {"unit": "kg",
                 "values": [3.0, 39.0, None, None, 200.0, 9.0]},
         "log": {"unit": "onoff",
                 "values": [True, False, True, False, True, False]}
      },
      "destinations": {"your_farm": None, "my_farm": None, "farms_galore": None}
    }
    # fmt: on

    json_pdtab = json_data_to_table(table_json_data, fixer=custom_test_fixer)
    assert pandas_pdtab.equals(json_pdtab)


def test_json_data_to_pdtable():
    """ ensure dict-obj to pdtable conversion
        compare to target created w. make_table(List[List]])
    """
    # Make a table using the cell grid parser
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

    table_from_cell_grid = make_table(lines_target, fixer=custom_test_fixer)

    # Make an identical table, but starting from JSON

    # fmt: off
    table_json_data = {
      "name": "farm_types1",
      "columns": {
         "species": {"unit": "text",
                     "values": ["chicken", "pig", "goat", "zybra", "cow", "goose"]},
         "num": {"unit": "-",
                 "values": [2.0, 4.0, 4.0, 4.0, None, 2.0]},
         "flt": {"unit": "kg",
                 "values": [3.0, 39.0, None, None, 200.0, 9.0]},
         "log": {"unit": "onoff",
                 "values": [True, False, True, False, True, False]}
      },
      "destinations": {"your_farm": None, "my_farm": None, "farms_galore": None}
    }
    # fmt: on

    table_from_json = json_data_to_table(table_json_data, fixer=custom_test_fixer)
    assert table_from_cell_grid.equals(table_from_json)

    # Round trip
    table_json_data_back = table_to_json_data(table_from_json)
    table_from_json_round_trip = json_data_to_table(table_json_data_back, fixer=custom_test_fixer)
    assert table_from_cell_grid.equals(table_from_json_round_trip)


def test_fat():
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
            g = parse_blocks(
                cell_rows, **{"origin": f'"{fn}"', "to": "jsondata"}, fixer=custom_test_fixer
            )

            for tp, tt in g:
                if tp == BlockType.TABLE:
                    """  compare generic object
                         i.e. containing None instead of pd.NaT, np.nan &c.
                    """
                    count += 1
                    assert tt == all_json[fn]

    assert count == all_files


def test_to_json_serializable():
    """ Unit test pure_json_obj / json_esc
    """
    obj = {
        "k1": r'k1 w. \"quotes"',
        "nix": None,
        "no-flt": np.nan,
        "no-date": pd.NaT,
        "flt": 1.23,
        "int": 123,
    }
    js_obj = to_json_serializable(obj)

    # verify that js_obj is directly json serializable
    jstr = json.dumps(js_obj)
    js_obj_from_json = json.loads(jstr)

    assert js_obj_from_json["k1"] == obj["k1"]
    assert js_obj_from_json["nix"] is None
    assert js_obj_from_json["no-flt"] is None
    assert js_obj_from_json["no-date"] is None
    assert js_obj_from_json["flt"] == obj["flt"]
    assert js_obj_from_json["int"] == obj["int"]


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
    js_obj_from_json = json.loads(jstr)
    pdtab_from_json = json_data_to_table(js_obj_from_json)

    assert pandas_pdtab.equals(pdtab_from_json)


def test_make_table_json_data__empty_table():
    """ tests reading in an empty table (ie no rows) to a jsondata object
    and creating a table via that json using json_data_to_table
    """
    lines_target = [
        ["**farm_types1"],
        ["your_farm my_farm farms_galore"],
        ["species", "num", "flt", "log"],
        ["text", "-", "kg", "onoff"]
    ]
    # parse the table to a jsondata
    table_json_data = make_table_json_data(lines_target, 'farm_types1.csv', fixer=custom_test_fixer)

    exp_table_json_data = {
      "name": "farm_types1",
      "columns": {
         "species": {"unit": "text",
                     "values": []},
         "num": {"unit": "-",
                 "values": []},
         "flt": {"unit": "kg",
                 "values": []},
         "log": {"unit": "onoff",
                 "values": []}
      },
      "destinations": {"your_farm": None, "my_farm": None, "farms_galore": None}
    }
    # correct json representation created
    assert table_json_data == exp_table_json_data

    # and json with empty values can be created into table
    table_from_json = json_data_to_table(table_json_data, fixer=custom_test_fixer)
    table_from_lines = make_table(lines_target, fixer=custom_test_fixer)
    assert table_from_lines.equals(table_from_json)
