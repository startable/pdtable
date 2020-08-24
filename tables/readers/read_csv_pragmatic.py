"""
Read starTables from CSV

clone of read_csv.py

* pragmatic reader, accept/handle errors

* Error in data:
  * callback's w. info
  * placeholder lines in pandas where / placebo lines
* Error in header:
  * callback's to fix / ignore


"""
import itertools
from os import PathLike
from typing import List, Optional, Tuple, Any, TextIO
import pandas as pd
import numpy as np

import sys

from .. import pdtable, Table, csv_sep
from ..store import BlockType, BlockGenerator
from .FixFactory import FixFactory
from ..table_metadata import TableOriginCSV

_TF_values = {"0": False, "1": True, "-": False}

# TBC: wrap in specific reader instance, this is global for all threads
_myFixFactory = FixFactory()


def _parse_onoff_column(values):
    bvalues = []
    for row, vv in enumerate(values):
        try:
            bvalues.append(_TF_values[vv.strip()])
        except KeyError:
            _myFixFactory.TableRow = row  # TBC: index
            fix_value = _myFixFactory.fix_illegal_cell_value("onoff", vv)
            bvalues.append(fix_value)
    return np.array(bvalues, dtype=np.bool)


_cnv_flt = {
    "N": lambda v: np.nan,
    "n": lambda v: np.nan,
    "{": lambda v: np.nan,
    "-": lambda v: np.nan if (len(v) == 1) else float(v),
}
for ch in "+0123456789":
    _cnv_flt[ch] = lambda v: float(v)

_cnv_datetime = lambda v: pd.NaT if (v == "-") else pd.to_datetime(v, dayfirst=True)


def _parse_float_column(values):
    fvalues = []
    for row, vv in enumerate(values):
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


def make_table(
    lines: List[str], sep: str, origin: Optional[TableOriginCSV] = None
) -> Table:
    table_name = lines[0].split(sep)[0][2:]

    _myFixFactory.TableName = table_name
    destinations = {s.strip() for s in lines[1].split(sep)[0].split(" ,;")}

    cnames_raw = lines[2].split(sep)
    # strip empty elements at end of list
    # Thingie: dbg
    # print(f"---oOo- column_names raw: {cnames_raw}")
    n_names_col = len(cnames_raw)
    for el in reversed(cnames_raw):
        if len(el) > 0:
            break
        n_names_col -= 1

    # TBC: also forward scan for first empty column (additional comment blocks)

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
            assert not cname in names
            names[cname] = 0
            column_names.append(cname)

    _myFixFactory.TableColumNames = column_names  # final

    units_raw = lines[3].split(sep)
    n_units_col = len(units_raw)
    for el in reversed(units_raw):
        if len(el) > 0:
            break
        n_units_col -= 1
    # Thingie: dbg
    # print(f"---oOo- units raw: {units_raw}")
    units = [el.strip() for el in units_raw[:n_units_col]]

    # Thingie: mark changes
    # auto filler
    units = [el if (len(el) > 0) else "-" for el in units]

    n_col = n_names_col
    if n_names_col != n_units_col:
        # Thingie
        print(
            f"Thingie: #-coloumn mismatch, n_names: {n_names_col} differ from n_units: {n_units_col}"
        )
        # TBC: choose larger
        n_col = max(n_names_col, n_units_col)
        pass

    column_data = [l.split(sep)[:n_col] for l in lines[4:]]
    column_data = [[el.strip() for el in col] for col in column_data]

    column_dtype = [_column_dtypes.get(u, _parse_float_column) for u in units]

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict()

    # determine # columns in data
    cols_stat = dict()
    for row in column_data:
        cols = len(row)
        if cols in cols_stat:
            cols_stat[cols] += 1
        else:
            cols_stat[cols] = 1

    maxval = 0
    num_data_col = 0  #  # columns seen in most rows
    for cnt in cols_stat.keys():
        if cols_stat[cnt] > maxval:
            maxval = cols_stat[cnt]
            num_data_col = cnt

    #    print(f"-oOo- n_names: {n_names_col}, n_units: {n_units_col}")
    #    print(f"-oOo- num_data columns: {num_data_col}, stat:  {cols_stat}")
    # here we have num_data_col, n_col, n_units_col
    if n_col > num_data_col:
        # Thingie: register warning / delegate callback
        # 1) truncate units and column_names
        #    or:
        # 2) extend column_data
        pass
    elif n_col < num_data_col:
        # Thingie: register warning / delegate callback
        # 1) extend units and column_names
        #    or:
        # 2) truncate column_data
        pass

    if len(cols_stat.keys()) > 1:
        for irow, row in enumerate(column_data):
            if len(row) < num_data_col:
                fix_row = _myFixFactory.fix_missing_rows_in_column_data(
                    row=irow, row_data=row, num_columns=num_data_col
                )
                column_data[irow] = fix_row

    # TTT dbg / callback w. dict
    #    print("-oOo- column_data row:",column_data)
    #    print("-oOo- column_data col:")
    #    for cc in zip(*column_data):
    #        print(f"-oOo-{cc}")
    #    print("-oOo-\n")

    # zip hides missing data !
    # (zip callback ?!)

    # determine data size
    # TTT
    data_len = None
    col = 0

    for cvalues in zip(*column_data):
        sz = len(cvalues)
        #        print(f"-oOo- col: {col} len: {sz}: {cvalues}")
        col += 1
        if data_len is None:
            data_len = sz
        elif data_len != sz:
            # TBD: statistics !!
            print(f"Thingie: Stupid number of data in column '{col}': {sz}, expected {data_len}")

    for name, dtype, unit, values in zip(column_names, column_dtype, units, zip(*column_data)):
        _myFixFactory.TableColumn = name
        columns[name] = dtype(values)

    return Table(
        pdtable.make_pdtable(
            pd.DataFrame(columns),
            units=units,
            metadata=pdtable.TableMetadata(
                name=table_name, destinations=destinations, origin=origin
            ),
        )
    )


# TTT BlockType.TEMPLATE_ROW : make_template
_token_factory_lookup = {BlockType.TABLE: make_table}


def make_token(token_type, lines, sep, origin) -> Tuple[BlockType, Any]:
    factory = _token_factory_lookup.get(token_type, None)
    return token_type, None if factory is None else factory(lines, sep, origin)


def read_stream_csv_pragmatic(
    f: TextIO, sep: str = None, origin: Optional[str] = None, fixFactory=None
) -> BlockGenerator:
    # Loop seems clunky with repeated init and emit clauses -- could probably be cleaned up
    # but I haven't seen how.
    # Template data handling is half-hearted, mostly because of doubts on StarTable syntax
    # Must all template data have leading `:`?
    # In any case, avoiding row-wise emit for multi-line template data should be a priority.
    if sep is None:
        sep = csv_sep()

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


def read_file_csv_pragmatic(file: PathLike, sep: str = None, fixFactory=None) -> BlockGenerator:
    """
    Read starTable tokens from CSV file, yielding them one token at a time.
    """
    if sep is None:
        sep = csv_sep()

    with open(file) as f:
        yield from read_stream_csv_pragmatic(f, sep, origin=file, fixFactory=fixFactory)
