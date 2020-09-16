import os
import json
from pathlib import Path

from tables import FixFactory, BlockType
from tables import StarTableJsonEncoder, table_to_json_data
from tables import read_csv
from .input.with_errors.auto_fixed import autoFixed


def input_dir() -> Path:
    return Path(__file__).parent / "input/with_errors"


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


def test_custom_FixFactory():
    """ Test custom FixFactory
        Verify that read_csv uses custom FixFactory
    """

    class fix_pi(FixFactory):
        def __init__(self):
            super().__init__()

        # augment existing method, simple fix float
        def fix_illegal_cell_value(self, vtype, value):
            if vtype == "float":
                return 22.0 / 7.0
            else:
                fix_value = FixFactory.fix_illegal_cell_value(self, vtype, value)
                return fix_value

    with open(input_dir() / "types3.csv", "r") as fh:
        g = read_csv(fh, to="jsondata", fixer=fix_pi)
        for tp, tt in g:
            if tp == BlockType.TABLE:
                assert tt["columns"]["num"][2] == 22.0 / 7.0
                assert tt["columns"]["flt"][0] == 22.0 / 7.0
                assert tt["columns"]["flt"][0] == 22.0 / 7.0
                assert tt["columns"]["flt2"][2] == 22.0 / 7.0


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
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ignore_files:
            continue

        with open(input_dir() / fn, "r") as fh:
            g = read_csv(fh, origin=f'"{fn}"', to="jsondata")
            count = 0
            for tp, tt in g:
                if tp == BlockType.TABLE:
                    count += 1
                    if fn != "all.csv":
                        assert tt == all_json[fn]

            if fn == "all.csv":
                assert count == all_files - 1
            else:
                assert count == 1
