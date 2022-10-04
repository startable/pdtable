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


def pack_tables(tables) -> Dict[Table]:
    if not isinstance(tables, dict):
        # For convenience, pack it in a dict
        return {DEFAULT_SHEET_NAME: tables}
    else:
        return tables

