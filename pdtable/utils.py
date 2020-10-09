# TODO Get rid of this "utils" module; find a better home for its constituents.
# TODO These functions seem out of sync with the rest of the pdtable.io API. Needs revisiting.
from os import PathLike
from typing import Optional, Any, Tuple, Iterable, Dict, Callable, Union, Sequence

from pdtable import TableBundle, BlockType, Table
from pdtable import read_csv


def normalized_table_generator(block_gen: Iterable[Tuple[BlockType, Optional[Any]]],
                               unit_conversion_schedule: Dict[str, Dict[str, str]] = None,
                               unit_converter: Callable[[float, str, str], float] = None,
                               ):
    for block_type, block in block_gen:
        if block is not None and block_type == BlockType.TABLE:
            table: Table = block
            if table.name in unit_conversion_schedule:
                table.convert_units(to=unit_conversion_schedule[table.name],
                                    converter=unit_converter)
        yield block_type, block


def read_bundle_from_csv(
        input_path: PathLike, sep: Optional[str] = ";",
        unit_conversion_schedule: Dict[str, Union[Sequence[str], Dict[str, str], Callable[[str], str]]] = None,
        unit_converter: Callable[[float, str, str], float] = None,
) -> TableBundle:
    """Read single csv-file to TableBundle"""
    if (unit_conversion_schedule is None) != (unit_converter is None):
        raise ValueError("Must supply neither or both, not just one.")

    inputs = read_csv(input_path, sep)

    if unit_conversion_schedule is not None:
        inputs = normalized_table_generator(inputs, unit_conversion_schedule, unit_converter)

    return TableBundle(inputs)
