import datetime
import json
import os
import sys
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
                            print("\ntest_output:\n",jstr);
                            jobj = json.loads(jstr)
                            print("\nmemory_obj:\n",jobj);
                            print("\ntarget_obj:\n",all_json.get(fn));
                            sys.stdout.flush()
                            assert jobj == all_json.get(fn)

