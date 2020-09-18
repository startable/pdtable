"""Interface to read starTables from CSV

This is a thin wrapper around parse_blocks(). The only thing it does is to present the contents of
an CSV file or stream as a Iterable of cell rows, where each row is a sequence of values.

"""
from contextlib import nullcontext
from os import PathLike
import io
from typing import TextIO, Union, Callable

import pdtable
from .parsers.blocks import parse_blocks
from pdtable import BlockGenerator, BlockType

from typing import ClassVar

FixFactory = ClassVar


def read_csv(
    source: Union[str, PathLike, TextIO],
    sep: str = None,
    origin: str = None,
    fixer: FixFactory = None,
    to: str = "pdtable",
    filter: Callable[[BlockType, str], bool] = None,
) -> BlockGenerator:
    """Reads StarTable data from a CSV file or text stream, yielding one block at a time.

    Reads StarTable data from a CSV-format file or text stream. StarTable blocks are parsed from
    this data and yielded one at a time as a (block_type, block) tuple, where
    - 'block_type' is a BlockType enum indicating which of the StarTable block types this is (table,
      metadata, directive, template); and
    - 'block' is the block content.

    'block' is given as one of the following data types, depending on the 'to' argument passed:
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
            Customized FixFactory instance to be used instead of default fixer.
            fixer corrects simple errors in source stream.
        to:
            StarTable return type
              "pdtable": pdtable.Table
              "jsondata": dict (json serializable object)
              "cellgrid": List[List[obj]] (raw input cells)

            TBV: Table, JsonData, CellGrid ?
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

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, **kwargs)
