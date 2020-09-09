"""Interface to read starTables from CSV

This is a thin wrapper around parse_blocks(). The only thing it does is to present the contents of
an CSV file or stream as a Iterable of cell rows, where each row is a sequence of values.

"""
from contextlib import nullcontext
from os import PathLike
from typing import TextIO, Union

import tables
from .parsers.blocks import parse_blocks, BlockType
from ..store import BlockGenerator
from .parsers.FixFactory import FixFactory


def read_csv(
    source: Union[str, PathLike, TextIO], sep: str = None, fixer: FixFactory = None
) -> BlockGenerator:
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
            Customized FixFactory instance to be used instead of default fixer.
            fixer corrects simple errors in source stream.

    Yields:
        Tuples of (BlockType, block) where 'block' is one of {Table, MetadataBlock, Directive,
        TemplateBlock}

    """
    if sep is None:
        sep = tables.CSV_SEP

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, origin=str(source), fixer=fixer)
