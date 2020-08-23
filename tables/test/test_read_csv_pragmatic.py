from io import StringIO
import os
from textwrap import dedent
from pathlib import Path
from tables.readers.read_csv_pragmatic import read_stream_csv_pragmatic
from ..store import BlockType
from tables.readers.read_csv_pragmatic import FixFactory
from tables.writers._csv import _table_to_csv
from .input_test_read_csv_pragmatic.auto_fixed import autoFixed


def input_dir() -> Path:
    return Path(__file__).parent / "input_test_read_csv_pragmatic"


def test_FAT():
    """ Factory Acceptance Test

        Verify that we are able to read all files in ./input
        Using default FixFactory
    """
    all_files = 0
    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ["auto_fixed.py", "__init__.py"]:
            continue
        all_files += 1

    for fn in os.listdir(input_dir()):
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ["auto_fixed.py", "__init__.py"]:
            continue
        with open(input_dir() / fn, "r") as fh:
            g = read_stream_csv_pragmatic(fh, sep=";", origin=fn)
            count = 0
            for tp, tt in g:
                if True:
                    if tp == BlockType.TABLE:
                        count += 1
                        with StringIO() as out:
                            _table_to_csv(tt, out)
                            test_output = out.getvalue().strip()
                        if fn != "all.csv":
                            assert test_output == dedent(autoFixed[fn]).strip()

            if fn == "all.csv":
                assert count == all_files - 1
            else:
                assert count == 1
