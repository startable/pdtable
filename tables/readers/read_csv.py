"""
Read starTables from CSV

Central idea is that the reader emits a stream of StarBlock objects.
This allows early abort of reads as well as generic postprocessing (
as discussed in store-module docstring).

Not implemented:
Current implementation ignores everything except table blocks.
"""
import itertools
from os import PathLike
from typing import List, Optional, Tuple, Any, TextIO

import numpy as np
import pandas as pd

import tables.proxy
import tables.table_metadata
from .. import pdtable, csv_sep
from ..ancillary_blocks import Directive, MetadataBlock
from ..store import BlockType, BlockGenerator

_TF_values = {"0": False, "1": True, "-": False}


def _parse_onoff_column(values):
    try:
        as_bool = [_TF_values[v.strip()] for v in values]
    except KeyError:
        raise ValueError("Entries in onoff columns must be 0 (False) or 1 (True)")
    return np.array(as_bool, dtype=np.bool)


_cnv_flt = {
    "N": lambda v: np.nan,
    "n": lambda v: np.nan,
    "-": lambda v: np.nan if (len(v) == 1) else float(v),
}
for ch in "+0123456789":
    _cnv_flt[ch] = lambda v: float(v)

_cnv_datetime = lambda v: pd.NaT if (v == "-") else pd.to_datetime(v, dayfirst=True)


def _parse_float_column(values):
    fvalues = [_cnv_flt[vv[0]](vv) for vv in values]
    return np.array(fvalues)


def _parse_datetime_column(values):
    dtvalues = [_cnv_datetime(vv) for vv in values]
    return np.array(dtvalues)


_column_dtypes = {
    "text": lambda values: np.array(values, dtype=np.str),
    "onoff": _parse_onoff_column,
    "datetime": _parse_datetime_column,
}


def make_metadata_block(lines: List[str], sep: str, origin: Optional[str] = None) -> MetadataBlock:
    mb = MetadataBlock(origin)
    for ll in lines:
        spl = ll.split(sep)
        if len(spl) > 1:
            key_field = spl[0].strip()
            if key_field[-1] == ":":
                mb[key_field[:-1]] = spl[1].strip()
    return mb


def make_directive(
    lines: List[str], sep: str, origin: Optional[str] = None
) -> Directive:
    name = lines[0].split(sep)[0][3:]
    directive_lines = [ll.split(sep)[0] for ll in lines[1:]]
    return Directive(name, directive_lines, origin)


def make_table(
    lines: List[str], sep: str, origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> tables.proxy.Table:
    table_name = lines[0].split(sep)[0][2:]
    destinations = {s.strip() for s in lines[1].split(sep)[0].split(" ,;")}
    column_names = list(itertools.takewhile(lambda s: len(s.strip()) > 0, lines[2].split(sep)))
    column_names = [el.strip() for el in column_names]

    n_col = len(column_names)
    units = lines[3].split(sep)[:n_col]
    units = [el.strip() for el in units]

    column_data = [l.split(sep)[:n_col] for l in lines[4:]]
    column_data = [[el.strip() for el in col] for col in column_data]

    column_dtype = [_column_dtypes.get(u, _parse_float_column) for u in units]

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict()
    for name, dtype, unit, values in zip(column_names, column_dtype, units, zip(*column_data)):
        try:
            columns[name] = dtype(values)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column {name} of table {table_name} as {unit}"
            ) from e

    return tables.proxy.Table(
        pdtable.make_pdtable(
            pd.DataFrame(columns),
            units=units,
            metadata=tables.table_metadata.TableMetadata(
                name=table_name, destinations=destinations, origin=origin
            ),
        )
    )


_token_factory_lookup = {
    BlockType.METADATA: make_metadata_block,
    BlockType.DIRECTIVE: make_directive,
    BlockType.TABLE: make_table,
}


def make_token(token_type, lines, sep, origin) -> Tuple[BlockType, Any]:
    factory = _token_factory_lookup.get(token_type, None)
    return token_type, (lines if factory is None else factory(lines, sep, origin))


def read_stream_csv(f: TextIO, sep: str = None, origin: Optional[str] = None) -> BlockGenerator:
    # Loop seems clunky with repeated init and emit clauses -- could probably be cleaned up
    # but I haven't seen how.
    # Template data handling is half-hearted, mostly because of doubts on StarTable syntax
    # Must all template data have leading `:`?
    # In any case, avoiding row-wise emit for multi-line template data should be a priority.
    if sep is None:
        sep = csv_sep()

    if origin is None:
        origin = "Stream"

    def is_blank(s):
        """
        True if first cell is empty

        assert is_blank('   ')
        assert is_blank('')
        assert is_blank(';')
        assert not is_blank('foo')
        assert not is_blank('  foo;')
        assert not is_blank('foo;')
        """
        ss = s.lstrip()
        return not ss or ss.startswith(sep)

    lines = []
    block = BlockType.METADATA
    block_line = 0
    for line_number_0based, line in enumerate(f):
        next_block = None
        if line.startswith("**"):
            if line.startswith("***"):
                next_block = BlockType.DIRECTIVE
            else:
                next_block = BlockType.TABLE
        elif line.startswith(":"):
            next_block = BlockType.TEMPLATE_ROW
        elif is_blank(line) and not block == BlockType.METADATA:
            next_block = BlockType.BLANK

        if next_block is not None:
            yield make_token(
                block, lines, sep, tables.table_metadata.TableOriginCSV(origin, block_line)
            )
            lines = []
            block = next_block
            block_line = line_number_0based + 1

        line = line.rstrip("\n")
        lines.append(line)

    if lines:
        yield make_token(block, lines, sep, tables.table_metadata.TableOriginCSV(origin, block_line))


def read_file_csv(file: PathLike, sep: str = None) -> BlockGenerator:
    """
    Read starTable tokens from CSV file, yielding them one token at a time.
    """
    if sep is None:
        sep = csv_sep()

    with open(file) as f:
        yield from read_stream_csv(f, sep)
