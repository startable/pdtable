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


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"


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
    g = parse_blocks(cell_rows, origin='"types1.csv" row 1')
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
        "destinations": ["your_farm my_farm farms_galore"],
        "origin": '"types1.csv" row 1',
    }
    json_pdtab = pdtable.proxy.Table(
        make_pdtable(
            pd.DataFrame(table_data["columns"]),
            units=table_data["units"],
            metadata=pdtable.table_metadata.TableMetadata(
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
            g = parse_blocks(cell_rows, origin=fn, do="json")

            for tp, tt in g:
                if tp == BlockType.TABLE:
                    """  compare generic object
                         i.e. containing None instead of pd.NaT, np.nan &c.
                    """
                    count += 1
                    jstr = json.dumps(tt, cls=StarTableJsonEncoder, ensure_ascii=False)
                    jobj = json.loads(jstr)
                    assert jobj == all_json[fn]
    assert count == all_files
