import datetime
import json
import os
from pathlib import Path

import numpy as np

from tables.readers.read_csv import read_stream_csv
from ..store import BlockType
from ..table_metadata import TableOriginCSV


def input_dir() -> Path:
    return Path(__file__).parent / "input/test_read_csv_pragmatic"


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

    targets = {}
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
                        targets[fn] = tt
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

    jstr = json.dumps(targets, cls=StarTableJsonEncoder)
    print(f"\n{jstr}\n")
