"""Interface to read/write StarTable data from/to CSV"""
import os
import io
from contextlib import nullcontext
from itertools import chain
from os import PathLike

from typing import TextIO, Union, Callable, Iterable

import pdtable  # Required to read dynamically-set pdtable.CSV_SEP
from ._represent import _represent_row_elements, _represent_col_elements
from .. import BlockType, Table, TableBundle
from ..store import BlockIterator
from .parsers.fixer import ParseFixer
from .parsers.blocks import parse_blocks


def read_csv(
    source: Union[str, PathLike, TextIO],
    sep: str = None,
    origin: str = None,
    fixer: ParseFixer = None,
    to: str = "pdtable",
    filter: Callable[[BlockType, str], bool] = None,
) -> BlockIterator:
    """Reads StarTable data from a CSV file or text stream, yielding one block at a time.

    Reads StarTable data from a CSV-format file or text stream. StarTable blocks are parsed from
    this data and yielded one at a time as a (block_type, block_content) tuple, where
    - 'block_type' is a BlockType enum indicating which of the StarTable block types this is (table,
      metadata, directive, template); and
    - 'block_content' is the block content.

    'block_content' is presented as one of the following data types, depending on the 'to' argument
    passed:
    - A pdtable.Table object,
    - A JSON serializable object (structure of nested dicts and lists of JSON-mappable values); or
    - A list of list of values, representing the raw cell grid (row and columns from the CSV data).

    Blocks can be filtered prior to parsing, by passing a callable as 'filter' argument. This can
    reduce reading time substantially when reading a subset of tables from an otherwise large file
    or stream. Only those blocks for which 'filter' returns True are fully parsed. Other blocks
    are parsed only superficially i.e. only the block's top-left cell, which is just
    enough to recognize block type and name to pass to 'filter', thus avoiding the much more
    expensive task of parsing the entire block, e.g. the values in all columns and rows of a large
    table.

    This is a thin wrapper around parse_blocks(). The only thing it does is present the contents of
    a CSV file or stream as a Iterable of cell rows, where each row is a sequence of values.

    Args:
        source:
            File path or text stream from which to read.
            If a file path, then this file gets opened, and then closed after reading.
            If a stream, then it is left open after reading; the caller is responsible for managing
            the stream.

        sep:
            Optional; CSV field delimiter. Default is ';'.

        origin:
            Optional; Table location

        fixer:
            Customized ParseFixer instance to be used instead of default fixer.
            fixer corrects simple errors in source stream.

        to:
            Determines the data type of the yielded blocks. Can be either of:
            - 'pdtable' (Default): pdtable.Table and other pdtable-style block objects
            - 'jsondata': JSON serializable objects (nested structure of dicts, lists, and
              JSON-mappable values) that can be passed directly to json.dump()
            - 'cellgrid': A grid of raw input cells i.e. a List[List[values]]

        filter:
            A callable that takes a (BlockType, block_name) tuple, and returns true if a block
            meeting this description is to be parsed, or false if it is to be ignored and discarded.

    Yields:
        Tuples of (BlockType, block) where 'block' is one of {Table, MetadataBlock, Directive,
        TemplateBlock}

    """
    if sep is None:
        sep = pdtable.CSV_SEP

    if origin is None:
        if hasattr(source, "name"):
            origin = source.name
        else:
            origin = str(source)

    kwargs = {"sep": sep, "origin": origin, "fixer": fixer, "to": to, "filter": filter}

    if not isinstance(source, (str, PathLike)):
        assert isinstance(source, io.TextIOBase)

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, **kwargs)


def write_csv(
    tables: Union[Table, Iterable[Table]],
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
        sep = pdtable.CSV_SEP

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
    if table.metadata.transposed:
        formatted_col_vals = (
            (
                fs.format(x) if fs else str(x)
                for x in _represent_col_elements(col.values, col.unit, na_rep)
            )
            for col, fs in zip(table, format_strings)
        )
        the_whole_thing = (
            f"**{table.name}*{sep}\n"
            + " ".join(str(x) for x in table.metadata.destinations)
            + "\n"
            + "\n".join(
                str(col.name) + sep + str(col.unit) + sep + sep.join(vals)
                for col, vals in zip(table, formatted_col_vals)  # FIXME shouldnt' be looping over formatted_col_vals here, its' already a string
            )
            + "\n\n"
        )
    else:
        # if True:
        formatted_rows = (
            sep.join(
                fs.format(x) if fs else str(x)
                for x, fs in zip(_represent_row_elements(row, units, na_rep), format_strings)
            )
            for row in table.df.itertuples(index=False, name=None)
        )
        the_whole_thing = (
            f"**{table.name}{sep}\n"
            + " ".join(str(x) for x in table.metadata.destinations)
            + "\n"
            + sep.join(str(x) for x in table.column_names)
            + "\n"
            + sep.join(str(x) for x in units)
            + "\n"
            + "\n".join(formatted_rows)
            + "\n\n"
        )

    stream.write(the_whole_thing)
