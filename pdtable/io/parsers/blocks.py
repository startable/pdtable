"""Parsers to convert uncontrolled cell grids into representations of StarTable blocks.

parse_blocks() emits a stream of blocks objects.
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

- The output is a representation of the StarTable block, either as:
  - A pdtable-style block object e.g. Table
  - A JSON-like data structure ready for serialization via e.g. json.dump()  (only implemented for
    table blocks at this stage); or
  - The original, raw cell grid, in case the user wants to do some low-level processing.

"""
import itertools
import re
from typing import Sequence, Optional, Tuple, Any, Iterable, List, Union

import pandas as pd

from pdtable import BlockType, BlockIterator
from pdtable import Table
from pdtable.io._json import to_json_serializable, JsonData, JsonDataPrecursor
from .columns import parse_column
from .fixer import ParseFixer
from ... import frame
from ...auxiliary import MetadataBlock, Directive
from ...table_metadata import TableOriginCSV, TableMetadata

# Typing alias: 2D grid of cells with rows and cols. Intended indexing: cell_grid[row][col]
CellGrid = Sequence[Sequence]

VALID_PARSING_OUTPUT_TYPES = {"pdtable", "jsondata", "cellgrid"}


def make_metadata_block(cells: CellGrid, origin: Optional[str] = None, **_) -> MetadataBlock:
    mb = MetadataBlock(origin)
    for row in cells:
        if len(row) > 1 and row[0] is not None:
            key_field = row[0].strip()
            if len(key_field) > 0 and key_field[-1] == ":":
                mb[key_field[:-1]] = row[1].strip()
    return mb


def make_directive(cells: CellGrid, origin: Optional[str] = None, **_) -> Directive:
    name = cells[0][0][3:]
    directive_lines = [row[0] for row in cells[1:]]
    return Directive(name, directive_lines, origin)


def default_fixer(**kwargs):
    """ Determine if user has supplied custom fixer
        Else return default ParseFixer() instance.
    """
    fixer = kwargs.get("fixer")
    if fixer is not None:
        if type(fixer) is type:
            # It's a class, not an instance. Make an instance here.
            fixer = kwargs["fixer"]()
    else:
        fixer = ParseFixer()
    assert fixer is not None
    fixer.origin = kwargs.get("origin")
    return fixer


def parse_column_names(column_names_raw: Sequence[Union[str, None]]) -> List[str]:
    """Parses column names from the sequence read from file

    Rejects everything after first blank cell, since there can be comments there.
    Strips column names. 
    """
    return [
        c.strip() for c in itertools.takewhile(lambda x: not _is_cell_blank(x), column_names_raw)
    ]


def make_table_json_precursor(cells: CellGrid, **kwargs) -> Tuple[JsonDataPrecursor, bool]:
    """Parses cell grid into a JSON-like data structure but with some non-JSON-native values

    Parses cell grid to a JSON-like data structure of nested "objects" (dict), "arrays" (list),
    and values, including values with types that map 1:1 to JSON-native types, as well as some
    value types that don't directly map to JSON types.

    This JSON data "precursor" can then be sent for further processing:
    - Parsing to pdtable-style Table block object
    - Conversion to a "pure" JSON data object in which all values are of JSON-native types.

    Also returns a bool "transposed" flag.
    """

    table_name: str = cells[0][0][2:]
    transposed = table_name.endswith("*")
    if transposed:
        # Chop off the transpose decorator from the name
        table_name = table_name[:-1]

    fixer = default_fixer(**kwargs)
    fixer.table_name = table_name

    # internally hold destinations as json-compatible dict
    destinations = {dest: None for dest in cells[1][0].strip().split(" ")}

    if transposed:
        # Column names are in lines' first cell
        column_names = parse_column_names([line[0] for line in cells[2:]])
    else:
        # Column names are on line 2 (zero-based)
        column_names = parse_column_names(cells[2])
    column_names = _fix_duplicate_column_names(column_names, fixer)

    n_col = len(column_names)
    if transposed:
        units = [line[1] for line in cells[2 : 2 + n_col]]
    else:
        units = cells[3][:n_col]
    units = [unit.strip() for unit in units]

    if transposed:
        data_lines = [line[2:] for line in cells[2 : 2 + n_col]]
        len_longest_line = max(len(line) for line in data_lines)

        # Find last non-blank data row
        n_row = 0
        for i_row in range(len_longest_line):
            # Does this row have non-blank cells?
            for line in data_lines:
                if len(line) >= i_row + 1 and not _is_cell_blank(line[i_row]):
                    # Found a non-blank cell. This row is legit. Go check next row.
                    n_row = i_row + 1
                    break
            else:
                # No non-blank cells found on this row. This row is blank. Go no further.
                break

        # Collate data rows
        data_rows = zip(
            *(
                line[:n_row]  # trim empty cells off of long lines
                if len(line) >= n_row
                else line + [None] * (n_row - len(line))  # pad short lines with empty cells
                for line in data_lines
            )
        )
    else:
        data_rows = [line[:n_col] for line in cells[4:]]
    data_rows = [list(row) for row in data_rows]

    # ensure all data columns are populated
    for i_row, row in enumerate(data_rows):
        if len(row) < n_col:
            fix_row = fixer.fix_missing_rows_in_column_data(
                row=i_row, row_data=row, num_columns=n_col
            )
            data_rows[i_row] = fix_row

    # build dictionary of columns iteratively to allow meaningful error messages
    columns = dict(zip(column_names, [[]]*len(column_names)))
    for name, unit, values in zip(column_names, units, zip(*data_rows)):
        try:
            fixer.column_name = name
            columns[name] = parse_column(unit, values, fixer)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column '{name}' of table '{table_name}' as '{unit}'"
            ) from e

    fixer.report()

    return {
        "name": table_name,
        "columns": columns,
        "units": units,
        "destinations": destinations,
        "origin": kwargs.get("origin"),
    }, transposed


def make_table(cells: CellGrid, origin: Optional[TableOriginCSV] = None, **kwargs) -> Table:
    """Parses cell grid into a pdtable-style Table block object."""
    json_precursor, transposed = make_table_json_precursor(cells, origin=origin, **kwargs)
    return Table(
        frame.make_table_dataframe(
            pd.DataFrame(json_precursor["columns"]),
            units=json_precursor["units"],
            table_metadata=TableMetadata(
                name=json_precursor["name"],
                destinations=set(json_precursor["destinations"].keys()),
                origin=json_precursor["origin"],
                transposed=transposed,
            ),
        )
    )


def make_table_json_data(cells: CellGrid, origin, **kwargs) -> JsonData:
    """Parses cell grid into a JSON-ready data structure."""
    impure_json, transposed = make_table_json_precursor(cells, origin=origin, **kwargs)
    # attach unit directly to individual column
    units = impure_json["units"]
    del impure_json["units"]  # replaced by "unit" field in columns
    del impure_json["origin"]  # not relevant for json_data
    columns = {}
    for cname, unit in zip(impure_json["columns"].keys(), units):
        columns[cname] = {"unit": unit, "values": impure_json["columns"][cname]}
    impure_json["columns"] = columns
    return to_json_serializable(impure_json)


def make_block(
    block_type: BlockType, cells: CellGrid, origin, **kwargs
) -> Tuple[Optional[BlockType], Optional[Any]]:
    """Dispatches cell grid to the proper parser, depending on block type and desired output type"""
    block_name = ""
    if block_type == BlockType.METADATA:
        factory = make_metadata_block
    elif block_type == BlockType.DIRECTIVE:
        factory = make_directive
    elif block_type == BlockType.TABLE:
        block_name = cells[0][0][2:]
        to = kwargs.get("to")
        if to == "cellgrid":
            factory = lambda c, *_, **__: c  # Regurgitate the unprocessed cell grid  # noqa:E731
        elif to == "jsondata":
            factory = make_table_json_data
        else:
            factory = make_table
    else:
        factory = None

    block_filter = kwargs.get("filter")
    if block_filter:
        assert callable(block_filter)
        if not block_filter(block_type, block_name):
            return None, None

    return block_type, cells if factory is None else factory(cells, origin, **kwargs)


# Regex that matches valid block start markers
_re_block_marker = re.compile(
    r"^("  # Marker must start exactly at start of cell
    r"(?<!\*)(\*\*\*?)(?!\*)"  # **table or ***directive but not ****undefined
    r"|"
    r"((?<!:):{1,3}(?!:))[^:]*\s*$"  # :col, ::table, :::file but not ::::undefined, :ambiguous:
    r"|"
    r"([^:]+:)\s*$"  # metadata:  but not :ambiguous:
    r")"
)
# $1 = Valid block start marker
# $2 = **Table / ***Directive
# $3 = :Template
# $4 = Metadata:


def parse_blocks(cell_rows: Iterable[Sequence], **kwargs) -> BlockIterator:
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
    elif to not in VALID_PARSING_OUTPUT_TYPES:
        raise ValueError(
            f"Unknown parsing output type; expected one of {VALID_PARSING_OUTPUT_TYPES}.", to
        )

    origin = kwargs["origin"] if "origin" in kwargs else "stream"

    fixer = default_fixer(**kwargs)
    kwargs["fixer"] = fixer  # use in make_block
    fixer.reset_fixes()
    fixer.origin = origin

    cell_grid = []

    state = BlockType.METADATA
    next_state = None
    this_block_1st_row = 0
    for row_number_0based, row in enumerate(cell_rows):
        #  print(f"parse_blocks: {state} {row[0] if len(row) > 0 else ' (empty) '}")
        if row is None or len(row) == 0 or _is_cell_blank(row[0]):
            if state != BlockType.BLANK:
                next_state = BlockType.BLANK
            else:
                continue
        elif isinstance(row[0], str):
            # possible token
            mm = _re_block_marker.match(row[0])
            if mm is None:  # TBC (PEP 572)
                cell_grid.append(row)
                continue

            if mm.group(1) is not None:
                if mm.group(1) == "**":
                    next_state = BlockType.TABLE
                elif mm.group(1) == "***":
                    next_state = BlockType.DIRECTIVE
                elif mm.group(4) is not None:
                    if state == BlockType.METADATA:
                        cell_grid.append(row)
                        continue
                    else:
                        next_state = BlockType.BLANK
                else:
                    next_state = BlockType.TEMPLATE_ROW
        else:
            # binary (excel &c.)
            cell_grid.append(row)
            continue

        if next_state is not None:
            # Current block has ended. Emit it.
            if len(cell_grid) > 0:
                kwargs["origin"] = TableOriginCSV(origin, this_block_1st_row)
                block_type, block = make_block(state, cell_grid, **kwargs)
                if block_type is not None:
                    yield block_type, block
            # TODO augment TableOriginCSV with one tailored for Excel
            cell_grid = []
            state = next_state
            next_state = None
            if state != BlockType.BLANK:
                cell_grid.append(row)
            elif len(row) > 0:
                #  emit non-empty lines, comments &c. as BLANK
                if len(row) == 1 and _is_cell_blank(row[0]):
                    continue
                cell_grid.append(row)

    if cell_grid:
        # Block ended with EOF. Emit it.
        kwargs["origin"] = TableOriginCSV(origin, this_block_1st_row)
        block_type, block = make_block(state, cell_grid, **kwargs)
        if block_type is not None:
            yield block_type, block


def _fix_duplicate_column_names(col_names_raw: Sequence[str], fixer: ParseFixer):
    """Finds duplicate column names and sends them to ParseFixer for fixing."""
    column_names = []
    names = {}
    for col, cname in enumerate(col_names_raw):
        if cname not in names and len(cname) > 0:
            names[cname] = 0
            column_names.append(cname)
        else:
            fixer.column_name = col
            if cname in names:
                cname = fixer.fix_duplicate_column_name(cname, input_columns=column_names)
            assert cname not in names
            names[cname] = 0
            column_names.append(cname)
    return column_names


def _is_cell_blank(cell):
    """Is this cell blank i.e. contains nothing or only whitespace"""
    return cell is None or (isinstance(cell, str) and not cell.strip())
