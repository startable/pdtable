"""
Read starTables from CSV

Central idea is that the reader emits a stream of StarBlock objects.
This allows early abort of reads as well as generic postprocessing (
as discussed in store-module docstring).

"""
from contextlib import nullcontext
from os import PathLike
from typing import TextIO, Union

import tables
from .parsers.blocks import parse_blocks
from ..store import BlockGenerator

# ======== Typing aliases, to clarify intent ==============
# ========================================================

# TBC: wrap in specific reader instance, this is global for all threads


def read_csv(source: Union[str, PathLike, TextIO], sep: str = None, fixer=None) -> BlockGenerator:
    """Read starTable blocks from CSV file or text stream, yielding them one block at a time.

    Args:
        source:
            File path or text stream from which to read.
            If a file path, then this file gets opened, and then closed after reading.
            If a stream, then it is left open after reading; the caller is responsible for managing
            the stream.
        sep:
            Optional; CSV field delimiter. Default is ';'.
        fixer:
            Thomas will have to help explain that one

    Yields:
        Tuples of (BlockType, block) where 'block' is one of {Table, MetadataBlock, Directive, TemplateBlock}

    """
    if sep is None:
        sep = tables.CSV_SEP

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, origin=str(source), fixer=fixer)
