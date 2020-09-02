import os
import sys
import json
from io import StringIO
from pathlib import Path
from textwrap import dedent

from tables.readers.read_csv import read_stream_csv
from .input.test_read_csv_pragmatic.auto_fixed import autoFixed
from ..store import BlockType


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"


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
            g = read_stream_csv(fh, sep=";", origin=fn, do="json")
            count = 0
            for tp, tt in g:
                if True:
                    if tp == BlockType.TABLE:
                        count += 1
                        # jstr = json.dumps(tt)
                        #test_output = jstr
                        #print(f"{fn} {jstr}")
                        print(f"{fn} {tt}")

#                        if fn != "all.csv":
#                            print(f"file: {fn}")
#                            print("\ntest_output:\n",test_output);
#                            print("\ntarget:\n",dedent(autoFixed[fn]).strip());
#                            sys.stdout.flush()
#                            assert test_output == dedent(autoFixed[fn]).strip()

            if fn == "all.csv":
                assert count == all_files - 1
            else:
                assert count == 1
