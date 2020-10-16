"""Conversion to JSON-serializable data structure.
"""
import datetime
from typing import Union, Dict, List

import numpy as np

from pdtable.table_metadata import TableOriginCSV

# Typing alias:
# JSON-like data structure of nested dicts ("objects"), lists ("arrays"), and JSON-native values
JsonData = Union[Dict[str, "JsonData"], List["JsonData"], str, float, int, bool, None]
# Typing alias: Same as JsonData, extended with a few non-JSON-native but readily JSONable types
JsonDataPrecursor = Union[
    Dict[str, "JsonDataPrecursor"],
    List["JsonDataPrecursor"],
    np.ndarray,
    str,
    float,
    int,
    bool,
    None,
    datetime.datetime,
    TableOriginCSV,
]

_json_encodable_value_maps = {
    dict: lambda obj: {kk: to_json_serializable(obj[kk]) for kk in obj.keys()},
    list: lambda obj: [to_json_serializable(kk) for kk in obj],
    float: lambda obj: obj if (not np.isnan(obj)) else None,
    int: lambda obj: obj,
    str: lambda obj: obj,
    bool: lambda obj: obj,
    type(None): lambda obj: obj,
}


def to_json_serializable(obj: JsonDataPrecursor) -> JsonData:
    """Converts object to a JSON-serializable data structure.

    Converts an object to a JSON-serializable hierarchical data structure of
    nested dicts ("objects"), lists ("arrays"), and values directly mappable to JSON-native types
    {float, int, str, bool} as well as None ("null").

    The types mentioned above are left as is.
    Also, additional element types are supported, mapping as follows:
    - numpy array -> list
    - values of type {datetime, table origin} -> string representation thereof
    """
    object_type = type(obj)
    if object_type in _json_encodable_value_maps:
        return _json_encodable_value_maps[object_type](obj)

    # Vanilla JSON encoder will choke on this value type.
    # Represent value as a JSON-encoder-friendly type.
    if isinstance(obj, np.ndarray):
        if f"{obj.dtype}" == "float64":
            return [val if (not np.isnan(val)) else None for val in obj.tolist()]
            # Note: would fail for obj.ndim > 1, but this is never the case here (columns are 1 dim)
        else:
            return [to_json_serializable(val) for val in obj.tolist()]

    if isinstance(obj, TableOriginCSV):
        return str(obj._file_name)

    if isinstance(obj, datetime.datetime):
        jval = str(obj)
        return jval if jval != "NaT" else None

    raise NotImplementedError(
        "Converting this type to a JSON-encodable type not yet implemented", type(obj)
    )
