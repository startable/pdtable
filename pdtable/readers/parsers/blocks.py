"""Parsers to convert uncontrolled cell grids into pdtable representations of StarTable blocks.

Central idea is that parse_blocks() emits a stream of StarBlock objects.
This in principle allows early abort of reads as well as generic postprocessing (
as discussed in store-module docstring).

parse_blocks() switches between different parsers depending on the StarTable block type:
- Metadata
- Directive
- Table
- Template (not yet implemented)

For each of these:

- The intended input is a two-dimensional "cell grid" i.e. a sequence of rows, with each row
  being a sequence of values (where a "sequence" is usually a list, but can also be e.g. a tuple).
  Rows need not be of equal length; only the relevant portion of a cell grid will be parsed
  depending on the relevant block type.

- The output is a pdtable representation of a StarTable block type.

"""
import sys
from typing import Sequence, Optional, Tuple, Any, Iterable, List

import pandas as pd

from .FixFactory import FixFactory
from .columns import parse_column
from ... import pandastable
from ..._json import pure_json_obj, JsonData, JsonDataPrecursor
from ...ancillary_blocks import MetadataBlock, Directive
from ...proxy import Table
from ...store import BlockType, BlockGenerator
from ...table_metadata import TableOriginCSV, TableMetadata

# Typing alias: 2D grid of cells with rows and cols. Intended indexing: cell_grid[row][col]
CellGrid = Sequence[Sequence]


def make_metadata_block(cells: CellGrid, origin: Optional[str] = None, **_) -> MetadataBlock:
    mb = MetadataBlock(origin)
    for row in cells:
        if len(row) > 1:
            key_field = row[0].strip()
            if len(key_field) > 0 and key_field[-1] == ":":
                mb[key_field[:-1]] = row[1].strip()
    return mb


def make_directive(cells: CellGrid, origin: Optional[str] = None, **_) -> Directive:
    name = cells[0][0][3:]
    directive_lines = [row[0] for row in cells[1:]]
    return Directive(name, directive_lines, origin)


def make_table_json_precursor(cells: CellGrid, **kwargs) -> JsonDataPrecursor:

    table_name = cells[0][0][2:]

    fixer = kwargs.get("fixer")
    if fixer is None:
        fixer = FixFactory()
    fixer.TableName = table_name

    # internally hold destinations as json-compatible dict
    destinations = {dest: None for dest in cells[1][0].strip().split(" ")}

    # handle multiple columns w. same name
    col_names_raw = cells[2]
    column_names = preprocess_column_names(col_names_raw, fixer)
    fixer.TableColumNames = column_names

    n_col = len(column_names)
    units = cells[3][:n_col]
    units = [el.strip() for el in units]

    column_data = [l[:n_col] for l in cells[4:]]
    column_data = [[el for el in col] for col in column_data]

    # ensure all data columns are populated
    for irow, row in enumerate(column_data):
        if len(row) < n_col:
            fix_row = fixer.fix_missing_rows_in_column_data(
                row=irow, row_data=row, num_columns=n_col
            )
            column_data[irow] = fix_row

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict()
    for name, unit, values in zip(column_names, units, zip(*column_data)):
        try:
            fixer.TableColumn = name
            columns[name] = parse_column(unit, values, fixer)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column '{name}' of table '{table_name}' as '{unit}'"
            ) from e

    if fixer.Warnings > 0:
        print(f"\nWarning: {fixer.Warnings} data errors fixed while parsing\n")

    if fixer.Errors > 0:
        sys.stderr.write(f"\nError: {fixer.Errors} column errors fixed while parsing\n")

    return {
        "name": table_name,
        "columns": columns,
        "units": units,
        "destinations": destinations,
        "origin": kwargs.get("origin"),
    }


def make_table(cells: CellGrid, origin: Optional[TableOriginCSV] = None, **_) -> Table:
    table_name = cells[0][0][2:]
    # TODO: here we could filter on table_name; only parse tables of interest
    # TTT TBD: filer on table_name : evt. før dette kald, hvor **er identificeret

    impure_json = make_table_json_precursor(cells, origin=origin)
    return Table(
        pandastable.make_pdtable(
            pd.DataFrame(impure_json["columns"]),
            units=impure_json["units"],
            metadata=TableMetadata(
                name=impure_json["name"],
                destinations=set(impure_json["destinations"].keys()),
                origin=impure_json["origin"],
            ),
        )
    )


def make_table_json_data(cells: CellGrid, origin, **kwargs) -> JsonData:
    impure_json = make_table_json_precursor(cells, origin=origin, **kwargs)
    return pure_json_obj(impure_json)


def make_block(block_type: BlockType, cells: CellGrid, origin, **kwargs) -> Tuple[BlockType, Any]:
    if block_type == BlockType.METADATA:
        factory = make_metadata_block
    elif block_type == BlockType.DIRECTIVE:
        factory = make_directive
    elif block_type == BlockType.TABLE:
        to = kwargs.get("to")
        if to == "cellgrid":
            factory = lambda c, *_, **__: c  # Just regurgitate the unprocessed cell grid
        elif to == "jsondata":
            factory = make_table_json_data
        else:
            factory = make_table
    else:
        factory = None

    return block_type, cells if factory is None else factory(cells, origin, **kwargs)


def parse_blocks(cell_rows: Iterable[Sequence], **kwargs) -> BlockGenerator:
    """Parses blocks from a single sheet as rows of cells.

    Takes an iterable of cell rows and parses it into blocks.

    Args:
        cell_rows: Iterable of cell rows, where each row is a sequence of cells.
    kwargs:
        origin: A thing.
        fixer: Also a thing, but different.
        to: generate Table of this type ("pdtable", "jsondata", "cellgrid")

    Yields:
        Blocks.
    """

    # Unpack, pre-process, validate, and repack the kwargs for downstream use
    to = kwargs.get("to")
    if to is None:
        kwargs["to"] = to = "pdtable"
    elif to not in {"pdtable", "jsondata", "cellgrid"}:
        raise NotImplementedError(f"Unsupported parsing output type", to)

    origin = kwargs["origin"] if "origin" in kwargs else "stream"

    fixer = kwargs.get("fixer")
    if fixer is not None:
        if type(fixer) is type:
            # It's a class, not an instance. Make an instance here.
            kwargs["fixer"] = fixer()
        else:
            assert isinstance(kwargs["fixer"], FixFactory)
            pass  # It's an instance. Use the instance.
    else:
        kwargs["fixer"] = FixFactory()
    assert kwargs["fixer"] is not None
    kwargs["fixer"].FileName = origin

    def is_blank(cell):
        """
        True if first cell is empty
        """
        return cell is None or (isinstance(cell, str) and not cell.strip())

    cell_grid = []
    this_block_type = BlockType.METADATA
    this_block_1st_row = 0
    for row_number_0based, row in enumerate(cell_rows):
        next_block_type = None
        first_cell = row[0] if len(row) > 0 else None

        if is_blank(first_cell) and not this_block_type == BlockType.METADATA:
            # Blank first cell means end of this block
            next_block_type = BlockType.BLANK
        elif isinstance(first_cell, str):
            # Check whether it's a block start marker
            if first_cell.startswith("**"):
                if first_cell.startswith("***"):
                    next_block_type = BlockType.DIRECTIVE
                else:
                    next_block_type = BlockType.TABLE
            elif first_cell.startswith(":"):
                next_block_type = BlockType.TEMPLATE_ROW

        if next_block_type is not None:
            # Current block has ended. Emit it.
            kwargs["origin"] = TableOriginCSV(origin, this_block_1st_row)
            yield make_block(this_block_type, cell_grid, **kwargs)

            # TODO augment TableOriginCSV with one tailored for Excel
            # Prepare to read next block
            cell_grid = []
            this_block_type = next_block_type
            this_block_1st_row = row_number_0based + 1

        cell_grid.append(row)

    if cell_grid:
        # Block terminated by EOF. Emit it.
        kwargs["origin"] = TableOriginCSV(origin, this_block_1st_row)
        yield make_block(this_block_type, cell_grid, **kwargs)


def preprocess_column_names(col_names_raw: List[str], fixer: FixFactory):
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
            fixer.TableColumn = col
            fixer.TableColumNames = column_names  # so far
            if len(cname) == 0:
                cname = fixer.fix_missing_column_name(input_columns=column_names)
            elif cname in names:
                cname = fixer.fix_duplicate_column_name(cname, input_columns=column_names)
            assert cname not in names
            names[cname] = 0
            column_names.append(cname)
    return column_names