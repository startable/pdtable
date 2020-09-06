"""
Read starTables from CSV

Central idea is that the reader emits a stream of StarBlock objects.
This allows early abort of reads as well as generic postprocessing (
as discussed in store-module docstring).

"""
from contextlib import nullcontext
from os import PathLike
from typing import Optional, Tuple, Any, TextIO, Dict, Sequence, Iterable, Union

import pandas as pd

import tables
from .FixFactory import FixFactory
from .parsers.columns import parse_column
from .. import pdtable
from ..ancillary_blocks import Directive, MetadataBlock
from ..store import BlockType, BlockGenerator
from ..table_metadata import TableOriginCSV

# ======== Typing aliases, to clarify intent ==============
JsonPrecursor = Dict  # Json-like data structure of nested "objects" (dict) and "arrays" (list).
CellGrid = Sequence[Sequence]  # Intended indexing: cell_grid[row][col]
# ========================================================

# TBC: wrap in specific reader instance, this is global for all threads
_myFixFactory = FixFactory()


def make_metadata_block(cells: CellGrid, origin: Optional[str] = None) -> MetadataBlock:
    mb = MetadataBlock(origin)
    for row in cells:
        if len(row) > 1:
            key_field = row[0].strip()
            if len(key_field) > 0 and key_field[-1] == ":":
                mb[key_field[:-1]] = row[1].strip()
    return mb


def make_directive(cells: CellGrid, origin: Optional[str] = None) -> Directive:
    name = cells[0][0][3:]
    directive_lines = [row[0] for row in cells[1:]]
    return Directive(name, directive_lines, origin)


def make_table(
        cells: CellGrid, origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> tables.proxy.Table:
    table_name = cells[0][0][2:]
    # TODO: here we could filter on table_name; only parse tables of interest
    # TTT TBD: filer on table_name : evt. før dette kald, hvor **er identificeret

    table_data = make_table_json_precursor(cells, origin)
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


_block_factory_lookup = {
    BlockType.METADATA: make_metadata_block,
    BlockType.DIRECTIVE: make_directive,
    BlockType.TABLE: make_table,
}


def make_block(block_type: BlockType, cells: CellGrid, origin) -> Tuple[BlockType, Any]:
    factory = _block_factory_lookup.get(block_type, None)
    return block_type, cells if factory is None else factory(cells, origin)


def preprocess_column_names(col_names_raw):
    """
       handle known issues in column_names
    """
    n_names_col = len(col_names_raw)
    for el in reversed(col_names_raw):
        if el is not None and len(el) > 0:
            break
        n_names_col -= 1

    # handle multiple columns w. same name
    column_names = []
    cnames_all = [el.strip() for el in col_names_raw[:n_names_col]]
    names = {}
    for col, cname in enumerate(cnames_all):
        if cname not in names and len(cname) > 0:
            names[cname] = 0
            column_names.append(cname)
        else:
            _myFixFactory.TableColumn = col
            _myFixFactory.TableColumNames = column_names  # so far
            if len(cname) == 0:
                cname = _myFixFactory.fix_missing_column_name(col=col, input_columns=cnames_all)
            elif cname in names:
                cname = _myFixFactory.fix_duplicate_column_name(col=col, input_columns=cnames_all)
            assert cname not in names
            names[cname] = 0
            column_names.append(cname)
    return column_names


def make_table_json_precursor(
        cells: CellGrid, origin: Optional[tables.table_metadata.TableOriginCSV] = None
) -> JsonPrecursor:
    table_name = cells[0][0][2:]
    _myFixFactory.TableName = table_name
    destinations = {cells[1][0].strip()}

    # handle multiple columns w. same name
    col_names_raw = cells[2]
    column_names = preprocess_column_names(col_names_raw)
    _myFixFactory.TableColumNames = column_names
    # TODO: _myFixFactory.TableColumNames (typo!) is assigned 4 times but never read... Use?

    n_col = len(column_names)
    units = cells[3][:n_col]
    units = [el.strip() for el in units]

    column_data = [l[:n_col] for l in cells[4:]]
    column_data = [[el for el in col] for col in column_data]

    # ensure all data columns are populated
    for irow, row in enumerate(column_data):
        if len(row) < n_col:
            fix_row = _myFixFactory.fix_missing_rows_in_column_data(
                row=irow, row_data=row, num_columns=n_col
            )
            column_data[irow] = fix_row

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict()
    for name, unit, values in zip(column_names, units, zip(*column_data)):
        try:
            _myFixFactory.TableColumn = name
            columns[name] = parse_column(unit, values, _myFixFactory)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column '{name}' of table '{table_name}' as '{unit}'"
            ) from e

    return {
        "name": table_name,
        "columns": columns,
        "units": units,
        "destinations": destinations,
        "origin": origin
    }


def read_stream_csv(
        cell_rows: Iterable[Sequence], sep: str = None, origin: Optional[str] = None, fix_factory=None,
        do: str = "Table"
) -> BlockGenerator:
    """Parses blocks from a single sheet as rows of cells.

    Takes an iterable of cell rows and parses it into blocks.

    Args:
        cell_rows: Iterable of cell rows, where each row is a sequence of cells.
        origin: A thing.

    Yields:
        Blocks.
    """
    # Loop seems clunky with repeated init and emit clauses -- could probably be cleaned up
    # but I haven't seen how.
    # Template data handling is half-hearted, mostly because of doubts on StarTable syntax
    # Must all template data have leading `:`?
    # In any case, avoiding row-wise emit for multi-line template data should be a priority.

    if do == "Table":
        _block_factory_lookup[BlockType.TABLE] = make_table
    else:
        _block_factory_lookup[BlockType.TABLE] = make_table_json_precursor

    if sep is None:
        sep = tables.CSV_SEP

    if origin is None:
        origin = "Stream"

    global _myFixFactory
    if fix_factory is not None:
        if type(fix_factory) is type:
            _myFixFactory = fix_factory()
        else:
            _myFixFactory = fix_factory
    assert _myFixFactory is not None

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

    cell_grid = []
    block = BlockType.METADATA
    block_line = 0
    for line_number_0based, row in enumerate(cell_rows):
        next_block = None
        first_cell = row[0] if len(row) > 0 else None
        if first_cell.startswith("**"):
            if first_cell.startswith("***"):
                next_block = BlockType.DIRECTIVE
            else:
                next_block = BlockType.TABLE
        elif first_cell.startswith(":"):
            next_block = BlockType.TEMPLATE_ROW
        elif is_blank(first_cell) and not block == BlockType.METADATA:
            next_block = BlockType.BLANK

        if next_block is not None:
            yield make_block(block, cell_grid, TableOriginCSV(origin, block_line))
            cell_grid = []
            block = next_block
            block_line = line_number_0based + 1

        cell_grid.append(row)

    if cell_grid:
        yield make_block(block, cell_grid, TableOriginCSV(origin, block_line))

    _myFixFactory = FixFactory()


def read_csv(source: Union[str, PathLike, TextIO], sep: str = None, fixer=None) -> BlockGenerator:
    """
    Read starTable tokens from CSV file or text stream, yielding them one token at a time.
    """
    if sep is None:
        sep = tables.CSV_SEP

    with open(source) if isinstance(source, (str, PathLike)) else nullcontext(source) as f:
        cell_rows = (line.rstrip("\n").split(sep) for line in f)
        yield from read_stream_csv(cell_rows, sep, origin=source, fix_factory=fixer)
