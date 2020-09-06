import os
from io import StringIO
from pathlib import Path
from textwrap import dedent

import pytest

from tables.readers.read_csv import parse_blocks
from tables.writers._csv import _table_to_csv
from .input.test_read_csv_pragmatic.auto_fixed import autoFixed
from ..store import BlockType


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"


@pytest.mark.skip(reason='Major refactoring CSV reader. Unskip when done.')
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

    for fn in os.listdir(input_dir()):
        print(f"-oOo- read {fn}")
        path = input_dir() / fn
        if not os.path.isfile(path):
            continue
        if fn in ignore_files:
            continue

        with open(input_dir() / fn, "r") as fh:
            g = parse_blocks(fh, origin=fn)
            count = 0
            for tp, tt in g:
                if True:
                    if tp == BlockType.TABLE:
                        count += 1
                        with StringIO() as out:
                            _table_to_csv(tt, out, sep=";", na_rep="-")
                            test_output = out.getvalue().strip()
                            print(test_output)
                        if fn != "all.csv":
                            assert test_output == dedent(autoFixed[fn]).strip()


            if fn == "all.csv":
                assert count == all_files - 1
            else:
                assert count == 1
