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
from abc import abstractmethod
import itertools
import re
from typing import Sequence, Optional, Tuple, Any, Iterable, List, Union, Dict
from collections import defaultdict
import pandas as pd
import warnings

from pdtable import BlockType, BlockIterator
from pdtable import Table
from pdtable.io._json import to_json_serializable, JsonData, JsonDataPrecursor
from pdtable.table_origin import (
    LocationSheet,
    NullLocationFile,
    TableOrigin,
    InputIssue,
    InputIssueTracker,
    NullInputIssueTracker,
)
from .columns import parse_column
from .fixer import ParseFixer
from ... import frame
from ...auxiliary import MetadataBlock, Directive
from ...table_metadata import TableMetadata

# Typing alias: 2D grid of cells with rows and cols. Intended indexing: cell_grid[row][col]
CellGrid = Sequence[Sequence]


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


def make_fixer(origin, fixer=None, **kwargs):
    """ Determine if user has supplied custom fixer
        Else return default ParseFixer() instance.
    """
    if fixer is not None:
        if type(fixer) is type:
            # It's a class, not an instance. Make an instance here.
            fixer = fixer()
    else:
        fixer = ParseFixer()
    assert fixer is not None
    fixer.origin = origin
    # fixer.reset_fixes()
    return fixer


def parse_column_names(column_names_raw: Sequence[Union[str, None]]) -> List[str]:
    """Parses column names from the sequence read from file

    Rejects everything after first blank cell, since there can be comments there.
    Strips column names. 
    """
    return [
        c.strip() for c in itertools.takewhile(lambda x: not _is_cell_blank(x), column_names_raw)
    ]


def make_table_json_precursor(cells: CellGrid, origin, fixer:ParseFixer) -> Tuple[JsonDataPrecursor, bool]:
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
    fixer.table_name = table_name

    # internally hold destinations as json-compatible dict
    destinations = {dest: None for dest in cells[1][0].strip().split(" ")}
    table_is_empty = len(cells) < 3
    if table_is_empty:
        column_names = []
    elif transposed:
        # Column names are in lines' first cell
        column_names = parse_column_names([line[0] for line in cells[2:]])
    elif len(cells) == 3:
        raise ValueError(f"Invalid table {table_name}: no unit specification found")
    else:
        # Column names are on line 2 (zero-based)
        column_names = parse_column_names(cells[2])
    column_names = _fix_duplicate_column_names(column_names, fixer)

    n_col = len(column_names)
    if table_is_empty:
        units = []
    elif transposed:
        units = [line[1] for line in cells[2 : 2 + n_col]]
    else:
        units = cells[3][:n_col]
    units = [unit.strip() for unit in units]

    if transposed and not table_is_empty:
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
    columns = dict(zip(column_names, [[]] * len(column_names)))
    for name, unit, values in zip(column_names, units, zip(*data_rows)):
        try:
            fixer.column_name = name
            columns[name] = parse_column(unit, values, fixer)
        except ValueError as e:
            raise ValueError(
                f"Unable to parse value in column '{name}' of table '{table_name}' as '{unit}'"
            ) from e

    fixer.report()

    return (
        {
            "name": table_name,
            "columns": columns,
            "units": units,
            "destinations": destinations,
            "origin": origin,
        },
        transposed,
    )


def _make_table(cells: CellGrid, origin, fixer) -> Table:
    """Parses cell grid into a pdtable-style Table block object."""
    json_precursor, transposed = make_table_json_precursor(
        cells, origin=str(origin.input_location), fixer=fixer,
    )
    return Table(
        frame.make_table_dataframe(
            pd.DataFrame(json_precursor["columns"]),
            units=json_precursor["units"],
            table_metadata=TableMetadata(
                name=json_precursor["name"],
                destinations=set(json_precursor["destinations"].keys()),
                origin=origin,
                transposed=transposed,
            ),
        )
    )


def make_table(cells: CellGrid, origin: Optional[TableOrigin]=None, **kwargs) -> Table:
    """Parses cell grid into a pdtable-style Table block object."""
    fixer=make_fixer(origin=origin, **kwargs)
    if origin is None:
        origin = TableOrigin()
    elif isinstance(origin, str):
        warnings.warn("Passing origin as str is deprecated", DeprecationWarning, stacklevel=2)
        origin = TableOrigin(NullLocationFile(origin).make_location_sheet().make_location_block(0))
    return _make_table(cells, origin, fixer=fixer)


def make_table_json_data(cells: CellGrid, origin, fixer) -> JsonData:
    """Parses cell grid into a JSON-ready data structure."""
    impure_json, transposed = make_table_json_precursor(cells, origin=origin, fixer=fixer)
    # attach unit directly to individual column
    units = impure_json["units"]
    del impure_json["units"]  # replaced by "unit" field in columns
    del impure_json["origin"]  # not relevant for json_data
    columns = {}
    for cname, unit in zip(impure_json["columns"].keys(), units):
        columns[cname] = {"unit": unit, "values": impure_json["columns"][cname]}
    impure_json["columns"] = columns
    return to_json_serializable(impure_json)


def make_raw_cells(cells: CellGrid, origin, **kwargs) -> CellGrid:
    return cells


DEFAULT_HANDLERS = (
    (BlockType.METADATA, make_metadata_block),
    (BlockType.DIRECTIVE, make_directive),
    (BlockType.TABLE, _make_table),
)
_default_handlers = dict(DEFAULT_HANDLERS)

TABLE_HANDLERS = (
    ("pdtable", _make_table),
    ("jsondata", make_table_json_data),
    ("cellgrid", make_raw_cells),
)
_table_handlers = dict(TABLE_HANDLERS)

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


def _apply_filter(block_type, filter, handler):
    if not block_type == BlockType.TABLE:
        return lambda cellgrid, *args, **kwargs: handler(cellgrid, *args, **kwargs) if filter(
            block_type, ""
        ) else None
    return (
        lambda cellgrid, *args, **kwargs: handler(cellgrid, *args, **kwargs)
        if filter(block_type, cellgrid[0][0][2:])
        else None
    )


def parse_blocks(
    cell_rows: Iterable[Sequence],
    location_sheet: LocationSheet = None,
    to: str = "pdtable",
    filter: Any = None,
    fixer: Any = None,
    issue_tracker: InputIssueTracker = None,
    origin: Optional[str] = None,
    **kwargs,
) -> BlockIterator:
    """Parses blocks from a single sheet as rows of cells.

    This is a legacy facade for `parse_blocks_stable`.

    Takes an iterable of cell rows and parses it into blocks.

    Args:
        cell_rows: 
            Iterable of cell rows, where each row is a sequence of cells.
        location_sheet: 
            A `LocationSheet` object describing the sheet being read.
        filter: 
            Optional. Will be called as (block type, name | ""). Block is dropped if false.
        to: 
            Optional. Generate Table of this type ("pdtable", "jsondata", "cellgrid")
    kwargs:
        fixer: Also a thing, but different.
    Yields:
        Blocks.
    """

    # Set up handlers
    # Legacy default is to emit unknown types as raw cells
    handlers = {
        bt: make_raw_cells for bt in BlockType
    }  
    handlers.update(DEFAULT_HANDLERS)
    try:
        handlers[BlockType.TABLE] = _table_handlers[to]
    except KeyError:
        raise ValueError(
            f"Unknown parsing output type; expected one of {list(_table_handlers.keys())}.", to
        )
    if filter:
        for k, base_handler in handlers.items():
            handlers[k] = _apply_filter(k, filter, base_handler)
    if origin and location_sheet:
        warnings.warn("Origin is shadowed by location.sheet.")

    if fixer is not None or kwargs:
        origin_str = location_sheet.file.load_identifier if location_sheet is not None else origin
        fixer = make_fixer(origin=origin_str, fixer=fixer, **kwargs)
        warnings.warn(
            "The fixer construct is deprecated and will be removed in future release. "
            "Please file an issue describing fixer use case at https://github.com/startable/pdtable/issues."
            f"Fixer was triggered by the following keyword args: fixer: {fixer}, [{kwargs}].",
            DeprecationWarning,
            stacklevel=2,
        )
    else:
        fixer = None

    yield from parse_blocks_stable(
        cell_rows,
        location_sheet=location_sheet,
        block_handlers=handlers,
        fixer=fixer,
        issue_tracker=issue_tracker,
    )


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


def parse_blocks_stable(
    cell_rows: Iterable[Sequence],
    issue_tracker: InputIssueTracker = None,
    block_handlers: Dict[BlockType, Any] = None,
    location_sheet: LocationSheet = None,
    fixer: Any = None,
) -> BlockIterator:
    """
    Generate blocks (tables, metadata, directives,...) from cell-rows

    This is a utility function for use by format-specific readers. It will return
    a block generator based on a stream of cell rows.
    The actual blocks are built by the supplied block handlers. A reader may need to 
    modify the defaults to correctly handle format conversions.

    Block handlers:
    The supplied block_handlers dictionary is queried for a handler by block type.
    If no block handler is defined, block will be silently ignored.
    Otherwise handler is called with arguments (cell_grid, origin, fixer) and should return
    a block of the correct type. If `None` is returned the block is silently ignored.

    Args:
        issue_tracker:
            Read issues are handled by calls to the issue tracker. The default issue tracker
            will simply raise an exception.
        block_handlers:
            A dictionary mapping block types to block handlers as described above
        location_sheet:
            Describes input location
        fixer:
            The fixer construct is a legacy system which will be deprecated. Please raise
            issues on github for all use-cases.
    """
    if location_sheet is None:
        location_sheet = NullLocationFile().make_location_sheet()

    if issue_tracker is None:
        issue_tracker = NullInputIssueTracker()

    if block_handlers is None:
        block_handlers = dict(DEFAULT_HANDLERS)

    if fixer is None:
        fixer = make_fixer(origin=location_sheet.file.load_identifier)

    def block_output(block_type, cell_grid, row: int):
        """
        Emit cell_grid as given block_type
        """
        if not cell_grid:  # return on None or empty
            return
        handler = block_handlers.get(block_type, None)
        if handler is None:
            return
        origin = TableOrigin(input_location=location_sheet.make_location_block(row=row))

        fixer.reset_fixes()
        try:
            block = handler(cell_grid, origin=origin, fixer=fixer)
        except ValueError as e:
            issue_tracker.add_error(str(e), load_location=origin.input_location)

        if block is not None:
            yield block_type, block

    cell_grid = []
    state = BlockType.METADATA
    next_state = None
    this_block_1st_row = 0
    for row_number_0based, row in enumerate(cell_rows):
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
            yield from block_output(state, cell_grid, this_block_1st_row)
            cell_grid = []
            state = next_state
            next_state = None
            this_block_1st_row = row_number_0based
            if state != BlockType.BLANK:
                cell_grid.append(row)
            elif len(row) > 0:
                #  emit non-empty lines, comments &c. as BLANK
                if len(row) == 1 and _is_cell_blank(row[0]):
                    continue
                cell_grid.append(row)

    yield from block_output(state, cell_grid, this_block_1st_row)


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
