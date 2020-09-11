import os
import json
from pathlib import Path

from tables import StarTableJsonEncoder, table_to_json_data
from tables.readers.read_csv import read_csv
from tables.writers._csv import _table_to_csv
from .input.test_read_csv_pragmatic.auto_fixed import autoFixed
from ..store import BlockType


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"


def test_columns_duplicate():
    """
       Verify that default FixFactory corrects duplicate column names

    """
    tab = None
    with open(input_dir() / "cols1.csv", "r") as fh:
        g = read_csv(fh)
        count = 0
        for tp, tt in g:
            if True:
                if tp == BlockType.TABLE:
                    tab = tt
                    break
    assert tab is not None
    assert tab.df["flt_fixed_001"] is not None
    assert tab.df["flt_fixed_001"][6] == 7.6
    assert tab.df["flt"][0] == 3.0


def test_columns_missing():
    """
       Verify that default FixFactory corrects missing column name

    """
    tab = None
    with open(input_dir() / "cols2.csv", "r") as fh:
        g = read_csv(fh)
        count = 0
        for tp, tt in g:
            if True:
                if tp == BlockType.TABLE:
                    tab = tt
                    break
    assert tab is not None
    assert tab.df["missing_fixed_000"] is not None
    assert tab.df["flt"][6] == 7.11


def test_FAT():
    """ Factory Acceptance Test

        Verify that we are able to read all files in ./input
        Using default FixFactory
    """
    all_files = 0
    ignore_files = ["auto_fixed.py", "__init__.py", "all.json"]
    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ignore_files:
            continue
        all_files += 1

    # load targets
    with open(input_dir() / "all.json") as f:
        all_json = json.load(f)

    for fn in os.listdir(input_dir()):
        print(f"-oOo- read {fn}")
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ignore_files:
            continue

        with open(input_dir() / fn, "r") as fh:
            g = read_csv(fh, origin=f'"{fn}"')
            count = 0
            for tp, tt in g:
                if tp == BlockType.TABLE:
                    count += 1
                    """  compare generic object
                         i.e. containing None instead of pd.NaT, np.nan &c.
                    """
                    if fn != "all.csv":
                        jdata = table_to_json_data(tt)

                        # translate to generic JSON
                        jstr = json.dumps(jdata, cls=StarTableJsonEncoder, ensure_ascii=False)
                        print("\njstr:")
                        print(jstr)

                        jobj = json.loads(jstr)

                        print("\njobj:")
                        print(jobj)
                        print("\nall_json[fn]:")
                        print(all_json[fn])
                        assert jobj == all_json[fn]

            if fn == "all.csv":
                assert count == all_files - 1
            else:
                assert count == 1
