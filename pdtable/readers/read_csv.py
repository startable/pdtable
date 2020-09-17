"""Interface to read starTables from CSV

This is a thin wrapper around parse_blocks(). The only thing it does is to present the contents of
an CSV file or stream as a Iterable of cell rows, where each row is a sequence of values.

"""
from contextlib import nullcontext
from os import PathLike
import io
from typing import TextIO, Union

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

    kwargs = {"sep": sep, "origin": origin, "fixer": fixer, "to": to}

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from parse_blocks(cell_rows, **kwargs)
