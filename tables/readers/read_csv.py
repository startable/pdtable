"""
Read starTables from CSV

Central idea is that the reader emits a stream of StarBlock objects.
This allows early abort of reads as well as generic postprocessing (
as discussed in store-module docstring).

"""
import sys
import itertools
from os import PathLike
from typing import List, Optional, Tuple, Any, TextIO

import numpy as np
import datetime
import pandas as pd

import tables
from .FixFactory import FixFactory
from .. import pdtable, Table
from ..store import BlockType, BlockGenerator
from ..ancillary_blocks import Directive, MetadataBlock
from ..table_metadata import TableOriginCSV

_TF_values = {"0": False, "1": True, "-": False}

# TBC: wrap in specific reader instance, this is global for all threads
_myFixFactory = FixFactory()


def _parse_onoff_column(values):
    bvalues = []
    for row, vv in enumerate(values):
        if(isinstance(vv,bool) or isinstance(vv,int)):
            bvalues.append(vv)
            continue
        try:
            bvalues.append(_TF_values[vv.strip()])
        except KeyError as exc:
            _myFixFactory.TableRow = row  # TBC: index
            fix_value = _myFixFactory.fix_illegal_cell_value("onoff", vv)
            bvalues.append(fix_value)
    return np.array(bvalues, dtype=np.bool)


_cnv_flt = {
    "N": lambda v: np.nan,
    "n": lambda v: np.nan,
    "-": lambda v: np.nan if (len(v) == 1) else float(v),
}
for ch in "+0123456789":
    _cnv_flt[ch] = lambda v: float(v)

_cnv_datetime = lambda v: pd.NaT if (v == "-") else pd.to_datetime(v, dayfirst=True)


def _parse_float_column(values):
    fvalues = []
    for row, vv in enumerate(values):
        if(isinstance(vv,float) or isinstance(vv,int)):
            fvalues.append(float(vv))
            continue
        if len(vv) > 0 and (vv[0] in _cnv_flt):
            try:
                fvalues.append(_cnv_flt[vv[0]](vv))
            except Exception as exc:
                _myFixFactory.TableRow = row  # TBC: index
                fix_value = _myFixFactory.fix_illegal_cell_value("float", vv)
                fvalues.append(fix_value)
        else:
            _myFixFactory.TableRow = row  # TBC: index
            fix_value = _myFixFactory.fix_illegal_cell_value("float", vv)
            fvalues.append(fix_value)
    return np.array(fvalues)


def _parse_datetime_column(values):
    dtvalues = []
    for row, vv in enumerate(values):
        if(isinstance(vv,datetime.datetime)):
            dtvalues.append(vv)
            continue
        if len(vv) > 0 and (vv[0].isdigit() or vv == "-"):
            try:
                dtvalues.append(_cnv_datetime(vv))
            except Exception as exc:
                # TBC: register exc !?
                _myFixFactory.TableRow = row  # TBC: index
                fix_value = _myFixFactory.fix_illegal_cell_value("datetime", vv)
                dtvalues.append(fix_value)
        else:
            _myFixFactory.TableRow = row  # TBC: index
            fix_value = _myFixFactory.fix_illegal_cell_value("datetime", vv)
            dtvalues.append(fix_value)

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
            if len(key_field) > 0 and key_field[-1] == ":":
                mb[key_field[:-1]] = spl[1].strip()
    return mb


def make_directive(lines: List[str], sep: str, origin: Optional[str] = None) -> Directive:
    name = lines[0].split(sep)[0][3:]
    directive_lines = [ll.split(sep)[0] for ll in lines[1:]]
    return Directive(name, directive_lines, origin)


def _column_names(cnames_raw):
    """
       handle known issues in column_names
    """
    n_names_col = len(cnames_raw)
    for el in reversed(cnames_raw):
        if el != None and len(el) > 0:
            break
        n_names_col -= 1

    # handle multiple columns w. same name
    column_names = []
    cnames_all = [el.strip() for el in cnames_raw[:n_names_col]]
    names = {}
    for icol, cname in enumerate(cnames_all):
        if not cname in names and len(cname) > 0:
            names[cname] = 0
            column_names.append(cname)
        else:
            _myFixFactory.TableColumn = icol
            _myFixFactory.TableColumNames = column_names  # so far
            if len(cname) == 0:
                cname = _myFixFactory.fix_missing_column_name(col=icol, input_columns=cnames_all)
            elif cname in names:
                cname = _myFixFactory.fix_duplicate_column_name(col=icol, input_columns=cnames_all)
            print(f"-oOo- {cname} {names}")
            assert not cname in names
            names[cname] = 0
            column_names.append(cname)
    return column_names


def make_table_data(
    lines: List[List], origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> dict :
    table_name = lines[0][0][2:]
    _myFixFactory.TableName = table_name
    destinations = { lines[1][0].strip() }

    # handle multiple columns w. same name
    cnames_raw = lines[2]
    column_names = _column_names(cnames_raw)
    _myFixFactory.TableColumNames = column_names

    n_col = len(column_names)
    units = lines[3][:n_col]
    units = [el.strip() for el in units]

    column_data = [l[:n_col] for l in lines[4:]]
    column_data = [[el for el in col] for col in column_data]

    # ensure all data columns are populated
    for irow, row in enumerate(column_data):
        if len(row) < n_col:
            fix_row = _myFixFactory.fix_missing_rows_in_column_data(
                row=irow, row_data=row, num_columns=n_col
            )
            column_data[irow] = fix_row

    column_dtype = [_column_dtypes.get(u, _parse_float_column) for u in units]

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict()
    for name, dtype, unit, values in zip(column_names, column_dtype, units, zip(*column_data)):
        try:
            _myFixFactory.TableColumn = name
            columns[name] = dtype(values)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column {name} of table {table_name} as {unit}"
            ) from e

    return {
         "name": table_name,
         "columns": columns,
         "units": units,
         "destinations": destinations,
         "origin": origin
    }

def make_table_data_csv(
    lines: List[str], sep: str, origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> dict :

    print("-oOo- lines: \n",lines)
    # TBC: augment csv-splitting as method for

    lines = [ [cell.strip() for cell in ll.split(sep)] for ll in lines]
    return make_table_data(lines,origin)

def _make_table(
    lines: List[List], origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> tables.proxy.Table:

    table_data =  make_table_data(lines,origin)

    return tables.proxy.Table(
        pdtable.make_pdtable(
            pd.DataFrame(table_data["columns"]),
            units=table_data["units"],
            metadata=tables.table_metadata.TableMetadata(
                name=table_data["name"], destinations=table_data["destinations"],
                origin=table_data["origin"]
            ),
        )
    )

def make_table(
    lines: List[str], sep: str, origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> tables.proxy.Table:

    table_name = lines[0].split(sep)[0][2:]

    # TTT TBD: filer on table_name : evt. før dette kald, hvor **er identificeret
    lines = [ [cell.strip() for cell in ll.split(sep)] for ll in lines]

    return _make_table(lines,origin)

_token_factory_lookup = {
    BlockType.METADATA: make_metadata_block,
    BlockType.DIRECTIVE: make_directive,
    BlockType.TABLE: make_table,
}

def make_token(token_type, lines, sep, origin) -> Tuple[BlockType, Any]:
    factory = _token_factory_lookup.get(token_type, None)
    return token_type, lines if factory is None else factory(lines, sep, origin)


def read_stream_csv(
    f: TextIO, sep: str = None, origin: Optional[str] = None, fixFactory=None,
    do: str = "Table"
) -> BlockGenerator:
    # Loop seems clunky with repeated init and emit clauses -- could probably be cleaned up
    # but I haven't seen how.
    # Template data handling is half-hearted, mostly because of doubts on StarTable syntax
    # Must all template data have leading `:`?
    # In any case, avoiding row-wise emit for multi-line template data should be a priority.

    if(do == "Table"):
        _token_factory_lookup[BlockType.TABLE] = make_table
    else:
        _token_factory_lookup[BlockType.TABLE] = make_table_data_csv

    if sep is None:
        sep = tables.CSV_SEP

    if origin is None:
        origin = "Stream"

    global _myFixFactory
    if not fixFactory is None:
        if type(fixFactory) is type:
            _myFixFactory = fixFactory()
        else:
            _myFixFactory = fixFactory
    assert _myFixFactory != None

    _myFixFactory.FileName = origin

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
            yield make_token(block, lines, sep, TableOriginCSV(origin, block_line))
            lines = []
            block = next_block
            block_line = line_number_0based + 1

        line = line.rstrip("\n")
        lines.append(line)

    if lines:
        yield make_token(block, lines, sep, TableOriginCSV(origin, block_line))

    _myFixFactory = FixFactory()


def read_file_csv(file: PathLike, sep: str = None, fixFactory=None) -> BlockGenerator:
    """
    Read starTable tokens from CSV file, yielding them one token at a time.
    """
    if sep is None:
        sep = tables.CSV_SEP

    with open(file) as f:
        yield from read_stream_csv(f, sep, origin=file, fixFactory=fixFactory)
