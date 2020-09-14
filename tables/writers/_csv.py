import os
from contextlib import nullcontext
from typing import Iterable, TextIO, Union

import tables as tables_module
from ..store import TableBundle
from ._represent import _represent_row_elements
from .. import Table


def write_csv(
    tables: Union[Table, Iterable[Table], TableBundle],
    to: Union[str, os.PathLike, TextIO],
    sep: str = None,
    na_rep: str = "-",
):
    """Writes one or more tables to a CSV file or text stream.

    Writes table blocks in CSV format to a file or text stream. Values are formatted to comply with
    the StarTable standard where necessary and possible; otherwise they are simply str()'ed.

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        to:
            File path or text stream to which to write.
            If a file path, then this file gets created/overwritten and then closed after writing.
            If a stream, then it is left open after writing; the caller is responsible for managing
            the stream.
        sep:
            Optional; CSV field delimiter. Default is ';'.
        na_rep:
            Optional; String representation of missing values (NaN, None, NaT). Default is '-'.
            If overriding this default, use another value compliant with the StarTable standard.
    """
    if sep is None:
        sep = tables_module.CSV_SEP

    if isinstance(tables, Table):
        # For convenience, pack single table in an iterable
        tables = [tables]

    # If it looks like a path, open a file and close when done.
    # Else we assume it's a stream that the caller is responsible for managing; leave it open.
    with open(to, "w") if isinstance(to, (str, os.PathLike)) else nullcontext(to) as stream:
        for table in tables:
            _table_to_csv(table, stream, sep, na_rep)


def _table_to_csv(table: Table, stream: TextIO, sep: str, na_rep: str) -> None:
    """Writes a single Table to stream as CSV. 
    """

    units = table.units
    display_formats = [table.column_metadata[c].display_format for c in table.column_metadata]
    format_strings = [f"{{:{f.specifier}}}" if f else None for f in display_formats]

    # Build entire string at once
    the_whole_thing = \
        f"**{table.name}{sep}\n" + \
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

