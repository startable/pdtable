from ._json import to_json_serializable
from . import Table, JsonData
from .readers.parsers.blocks import make_table


def json_data_to_table(table_json_data: JsonData) -> Table:
    """  translate table-dictionary (JSON-like) to Table
    """
    lines_json = []
    lines_json.append([f'**{table_json_data["name"]}'])
    lines_json.append([" ".join(table_json_data["destinations"])])
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
    return to_json_serializable(table_data)
