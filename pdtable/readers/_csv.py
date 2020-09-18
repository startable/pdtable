"""Interface to read starTables from CSV

This is a thin wrapper around parse_blocks(). The only thing it does is present the contents of
a CSV file or stream as a Iterable of cell rows, where each row is a sequence of values.

"""
from contextlib import nullcontext
from os import PathLike

from typing import TextIO, Union, Callable

import pdtable  # Required to read dynamically set pdtable.CSV_SEP
from .. import BlockType
from ..store import BlockGenerator
from .parsers.FixFactory import FixFactory
from .parsers.blocks import parse_blocks


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

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, **kwargs)
