from typing import Dict

from pdtable import Table

DEFAULT_SHEET_NAME = "Sheet1"
DEFAULT_STYLE_SPEC = {
    "table_name": {
        "font": {"color": "1F4E78", "bold": True,},  # hex color code
        "fill": {"color": "D9D9D9",},  # RGB color code
    },
    "destinations": {"font": {"color": "808080", "bold": True,}, "fill": {"color": "D9D9D9",},},
    "column_names": {"fill": {"color": "F2F2F2",}, "font": {"bold": True,},},
    "units": {"fill": {"color": "F2F2F2",},},
    "values": {},
}


def _pack_tables(tables) -> Dict[str, Table]:
    if not isinstance(tables, dict):
        # For convenience, pack it in a dict
        return {DEFAULT_SHEET_NAME: tables}
    else:
        return tables


def _table_header(table: Table) -> str:
    if table.metadata.transposed:
        return f"**{table.name}*"
    else:
        return f"**{table.name}"


def _table_destinations(table: Table) -> str:
    return " ".join(str(x) for x in table.metadata.destinations)