import datetime
import json

import numpy as np

from tables import Table
from tables.readers.parsers.blocks import make_table, JsonData
from tables.table_metadata import TableOriginCSV
from tables._json import pure_json_obj


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
            return obj._file_name
        if isinstance(obj, datetime.datetime):
            jval = str(obj)
            return jval if jval != "NaT" else None

        return json.JSONEncoder.default(self, obj)


def json_data_to_table(table_json_data: JsonData) -> Table:
    """  translate table-dictionary (JSON-like) to Table
    """
    lines_json = []
    lines_json.append([f'**{table_json_data["name"]}'])
    lines_json.append([f"{dst}" for dst in table_json_data["destinations"]])
    lines_json.append([f"{cname}" for cname in table_json_data["columns"].keys()])
    lines_json.append([f"{unit}" for unit in table_json_data["units"]])
    json_rows = list(map(list, zip(*table_json_data["columns"].values())))  # transposed columns
    lines_json.extend(json_rows)
    # note: this allows us to use FixFactory !
    return make_table(lines_json, origin=table_json_data["origin"])


def table_to_json_data(table: Table) -> JsonData:
    """  translate Table to table-dictionary (JSON-like)
    """

    table_data = {
        "name": table.name,
        "origin": table.metadata.origin,
        "destinations": {dst: None for dst in table.metadata.destinations},
        "units": table.units,
    }
    table_data["columns"] = {}
    for cname in table.column_names:
        table_data["columns"][cname] = [vv for vv in table.df[cname]]
    return pure_json_obj(table_data)

