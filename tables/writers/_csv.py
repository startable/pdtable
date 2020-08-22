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
    """Writes one or more tables to a CSV file or text stream.

    Writes table blocks in CSV format to a file or text stream. Values are formatted to comply with
    the StarTable standard where necessary and possible; otherwise they are simply str()'ed.

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        out:
            File path or text stream to which to write.
            If a file path, then this file gets closed after writing.
            If a stream, then it is left open; it is assumed that the caller owns
            the stream and is responsible for managing it.
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
    display_formats = [table.column_metadata[c].display_format for c in table.column_metadata]
    format_strings = [f"{{:{f.specifier}}}" if f else None for f in display_formats]

    # Build entire string at once
    the_whole_thing = \
        f"**{table.name}\n" + \
        " ".join(str(x) for x in table.metadata.destinations) + "\n" + \
        sep.join(str(x) for x in table.column_names) + "\n" + \
        sep.join(str(x) for x in units) + "\n" + \
        "\n".join(sep.join(fs.format(x) if fs else str(x) for x, fs in zip(_represent_row_elements(row, units, na_rep), format_strings)) for row in table.df.itertuples(index=False, name=None)) + \
        "\n\n"

    stream.write(the_whole_thing)

    # Alternatively, write to stream one row at a time:
    # stream.write(f"**{table.name}\n")
    # stream.write(" ".join(str(x) for x in table.metadata.destinations) + "\n")
    # stream.write(sep.join(str(x) for x in table.column_names) + "\n")
    # stream.write(sep.join(str(x) for x in units) + "\n")
    # for row in table.df.itertuples(index=False, name=None):
    #     stream.write(sep.join(fs.format(x) if fs else str(x) for x, fs in zip(_represent_row_elements(row, units, na_rep), format_strings)) + "\n")
    # stream.write("\n")

