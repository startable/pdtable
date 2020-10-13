# TODO Get rid of this "utils" module; find a better home for its constituents.
# TODO These functions seem out of sync with the rest of the pdtable.io API. Needs revisiting.
from os import PathLike
from typing import Optional, Any, Tuple, Iterable, Dict, Callable, Union, Sequence

from pdtable import TableBundle, BlockType, Table
from pdtable import read_csv
from pdtable.proxy import UnitConverter, ColumnUnitDispatcher

# type hint shorthand
TableUnitDispatcher = Union[Dict[str, ColumnUnitDispatcher], Callable[[str], ColumnUnitDispatcher]]


def normalized_table_generator(block_gen: Iterable[Tuple[BlockType, Optional[Any]]],
                               convert_units_to: TableUnitDispatcher = None,
                               unit_converter: UnitConverter = None,
                               ):
    for block_type, block in block_gen:
        if block is not None and block_type == BlockType.TABLE:
            # convert table units
            table: Table = block
            if isinstance(convert_units_to, Dict):
                to_units: ColumnUnitDispatcher = convert_units_to.get(table.name)
            else:
                # Assume it's callable
                to_units: ColumnUnitDispatcher = convert_units_to(table.name)

            if to_units is not None:
                table.convert_units(to=convert_units_to[table.name], converter=unit_converter)
        yield block_type, block


def read_bundle_from_csv(
        input_path: PathLike, sep: Optional[str] = ";",
        convert_units_to: TableUnitDispatcher = None,
        unit_converter: UnitConverter = None,
) -> TableBundle:
    """Read single csv-file to TableBundle"""
    if convert_units_to and not unit_converter:
        raise ValueError("No unit converter supplied.")

    inputs = read_csv(input_path, sep)

    if convert_units_to is not None:
        inputs = normalized_table_generator(inputs, convert_units_to, unit_converter)

    return TableBundle(inputs)
