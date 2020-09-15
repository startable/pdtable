import datetime
import json

import numpy as np
from tables.table_metadata import TableOriginCSV


def json_esc(text: str) -> str:
    return "".join([cc if (ord(cc) < 127 and cc != '"') else '"' for cc in text])


_cnv = {
    dict: lambda obj: {kk: pure_json_obj(obj[kk]) for kk in obj.keys()},
    list: lambda obj: [pure_json_obj(kk) for kk in obj],
    float: lambda obj: obj if (not np.isnan(obj)) else None,
    int: lambda obj: obj,
    str: lambda obj: json_esc(obj),
    bool: lambda obj: obj,
    type(None): lambda obj: obj,
}


def pure_json_obj(obj):
    """ return JSON serializable version of obj
    """
    tp = type(obj)
    if tp in _cnv.keys():
        return _cnv[tp](obj)

    if isinstance(obj, np.ndarray):
        if f"{obj.dtype}" == "float64":
            return [val if (not np.isnan(val)) else None for val in obj.tolist()]
        else:
            return [pure_json_obj(val) for val in obj.tolist()]
    elif isinstance(obj, TableOriginCSV):
        return json_esc(obj._file_name)
    elif isinstance(obj, datetime.datetime):
        jval = str(obj)
        return jval if jval != "NaT" else None
    else:
        print(f"TBD: pure_json_obj, Handle type: {type(obj)}")
        assert 0
