"""Interface to write Tables to an Excel workbook.

The only Excel I/O engine supported right now is 'openpyxl', but this module can
be extended to support others such as 'xlsxwriter'. 

openpyxl (and eventually other engines) are not required at install time; 
only when write_excel() is called for the first time. 
"""


from tables.store import TableBundle
from typing import Iterable, Union
import os

from ..pdtable import Table


def write_excel(
    tables: Union[Table, Iterable[Table], TableBundle],
    out: Union[str, os.PathLike],
    na_rep: str = "-",
):
    """Writes one or more tables to an Excel workbook.

    Writes table blocks to an Excel workbook file. 
    Values are formatted to comply with the StarTable standard where necessary and possible. 

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        out:
            File path to which to write. 
        na_rep:
            Optional; String representation of missing values (NaN, None, NaT). If overriding the default '-', it is recommended to use another value compliant with the StarTable standard.
    """
    try:
        import openpyxl
        from ._excel_openpyxl import write_excel_openpyxl as write_excel_func

    except ImportError as err:
        raise ImportError(
            "Unable to find a usable spreadsheet engine. "
            "Tried using: 'openpyxl'.\n"
            "Please install openpyxl for Excel I/O support."
        ) from err

    write_excel_func(na_rep, out, tables)
