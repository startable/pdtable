import os
from contextlib import nullcontext
from typing import Iterable, TextIO, Union

from tables.store import TableBundle
from ._represent import _represent_row_elements
from ..pdtable import Table


def write_csv(
    tables: Union[Table, Iterable[Table], TableBundle],
    out: Union[str, os.PathLike, TextIO],
    sep: str = ";",
    na_rep: str = "-",
):
    """Writes one or more tables to CSV file or stream.

    Writes table blocks in CSV format to a file or stream. Values are formatted to comply with the StarTable standard where necessary and possible; 
    otherwise they are simply str()'ed. 

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        out:
            File path or text stream to which to write. 
        sep:
            Optional; CSV field delimiter.
        na_rep:
            Optional; String representation of missing values (NaN, None, NaT). If overriding the default '-', it is recommended to use another value compliant with the StarTable standard.
    """

    if isinstance(tables, Table):
        # For convenience, pack single table in an iterable
        tables = [tables]

    # If it looks like a path, open a file and close when done.
    # Else we assume it's a stream that the caller is responsible for managing; leave it open.
    with open(out, "w") if isinstance(out, (str, os.PathLike)) else nullcontext(out) as stream:
        for table in tables:
            _table_to_csv(table, stream, sep, na_rep)


def _table_to_csv(table: Table, stream: TextIO, sep: str = ";", na_rep: str = "-") -> None:
    """Writes a single Table to stream as CSV. 
    """
    units = table.units
    stream.write(f"**{table.name}\n")
    stream.write(" ".join(str(x) for x in table.metadata.destinations) + "\n")
    stream.write(sep.join(str(x) for x in table.column_names) + "\n")
    stream.write(sep.join(str(x) for x in units) + "\n")
    precisions = [table.column_metadata[c].display_format.precision for c in table.column_metadata]
    for row in table.df.itertuples(index=False, name=None):
        stream.write(sep.join(_format(x, p) for x, p in zip(_represent_row_elements(row, units, na_rep), precisions)) + "\n")
    stream.write("\n")


def _format(x, precision: int):
    return ("{:." + str(precision) + "f}").format(x) if precision else str(x)
