import datetime
import json
import os
import io
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from textwrap import dedent

from tables.readers.read_csv import read_stream_csv
from tables.readers.read_csv import tables, pdtable
from tables.readers.read_csv import make_table
from ..store import BlockType
from ..table_metadata import TableOriginCSV


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"

# TBC: this stuff should prob. be included in some pdtable util package
# TBD: need a good name for the in-memory object (JSON_data)
class StarTableJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            if f"{obj.dtype}" == "float64":
                # https://stackoverflow.com/questions/26921836/correct-way-to-test-for-numpy-dtype
                return [val if (not np.isnan(val)) else None for val in obj.tolist()]
            else:
                return obj.tolist()
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, TableOriginCSV):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            jval = str(obj)
            return jval if jval != "NaT" else None

        return json.JSONEncoder.default(self, obj)

def JSON_data_to_pdtable(table_data:dict):
    """  translate table-dictionary (JSON-like) to pdtable
    """
    lines_json =  []
    lines_json.append([f'**{table_data["name"]}'])
    lines_json.append([f'{dst}' for dst in table_data["destinations"]])
    lines_json.append([f'{cname}' for cname in table_data["columns"].keys()])
    lines_json.append([f'{unit}' for unit in table_data["units"]])
    json_rows = list(map(list, zip(*table_data["columns"].values()))) # transposed columns
    lines_json.extend(json_rows)
    # note: this allows us to use FixFactory !
    return make_table(lines_json,origin=table_data["origin"])

def pdtable_to_JSON_data(tab):
    """  translate pdtable to table-dictionary (JSON-like)
    """
    table_data = { "name": tab.name, "origin": tab.metadata.origin,
                    "destinations":  tab.metadata.destinations,
                    "units": tab.units
                 }
    table_data["columns"] = {}
    for cname in tab.column_names:
        table_data["columns"][cname] = [vv for vv in tab.df[cname]]
    return table_data

def test_JSON_pdtable():
    """ ensure dict-obj to pdtable conversion
        compare to target created w. read_stream_csv
    """
    csv_src = dedent(
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
    pandas_pdtab = None
    with io.StringIO(csv_src) as fh:
        g = read_stream_csv(fh, sep=";", origin='"types1.csv" row 1')
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
    json_pdtab = tables.proxy.Table(
        pdtable.make_pdtable(
            pd.DataFrame(table_data["columns"]),
            units=table_data["units"],
            metadata=tables.table_metadata.TableMetadata(
                name=table_data["name"],
                destinations={dest for dest in table_data["destinations"]},
                origin=table_data["origin"],
            ),
        )
    )
    assert pandas_pdtab.equals(json_pdtab)

def test_JSON_data_to_pdtable():
    """ ensure dict-obj to pdtable conversion
        compare to target created w. make_table(List[List]])
    """
    lines_target = [
       ["**farm_types1"],
       ["your_farm my_farm farms_galore"],
       ["species","num","flt","log"],
       ["text","-","kg","onoff"],
       ["chicken",2,3,1],
       ["pig",4,39,0],
       ["goat",4,None,1],
       ["zybra",4,None,0],
       ["cow",None,200,1],
       ["goose",2,9,0]
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

    json_pdtab = JSON_data_to_pdtable(table_data)
    assert pandas_pdtab.equals(json_pdtab)

    # reverse
    table_data_back = pdtable_to_JSON_data(json_pdtab)
    json_pdtab_back = JSON_data_to_pdtable(table_data)
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
        if fn in ["auto_fixed.py", "__init__.py", "all.json"]:
            continue
        all_files += 1

    with open(input_dir() / "all.json") as f:
        all_json = json.load(f)

    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ["auto_fixed.py", "__init__.py", "all.json"]:
            continue
        with open(input_dir() / fn, "r") as fh:
            g = read_stream_csv(fh, sep=";", origin=fn, do="json")
            count = 0
            for tp, tt in g:
                if True:
                    if tp == BlockType.TABLE:
                        count += 1
                        jstr = json.dumps(tt, cls=StarTableJsonEncoder, ensure_ascii=False)
                        if fn != "all.csv":
                            jobj = json.loads(jstr)
                            assert jobj == all_json.get(fn)

