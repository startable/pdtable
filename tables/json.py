import datetime
import json

import numpy as np

from tables.readers.parsers.blocks import make_table
from tables.table_metadata import TableOriginCSV


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


def json_data_to_pdtable(table_data: dict):
    """  translate table-dictionary (JSON-like) to pdtable
    """
    lines_json = []
    lines_json.append([f'**{table_data["name"]}'])
    lines_json.append([f'{dst}' for dst in table_data["destinations"]])
    lines_json.append([f'{cname}' for cname in table_data["columns"].keys()])
    lines_json.append([f'{unit}' for unit in table_data["units"]])
    json_rows = list(map(list, zip(*table_data["columns"].values())))  # transposed columns
    lines_json.extend(json_rows)
    # note: this allows us to use FixFactory !
    return make_table(lines_json, origin=table_data["origin"])


def pdtable_to_json_data(tab):
    """  translate pdtable to table-dictionary (JSON-like)
    """
    table_data = {"name": tab.name, "origin": tab.metadata.origin,
                  "destinations": tab.metadata.destinations,
                  "units": tab.units
                  }
    table_data["columns"] = {}
    for cname in tab.column_names:
        table_data["columns"][cname] = [vv for vv in tab.df[cname]]
    return table_data

