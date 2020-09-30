# TODO Get rid of this "utils" module; find a better home for its constituents.
# TODO These functions seem out of sync with the rest of the pdtable.io API. Needs revisiting.
from os import PathLike
from typing import Optional, Any, Tuple, Iterable
from abc import ABC
from pdtable import TableBundle, BlockType
from pdtable import read_csv
from . import units


def normalized_table_generator(unit_policy, ts: Iterable[Tuple[BlockType, Optional[Any]]]):
    for token_type, token in ts:
        if token is not None and token_type == BlockType.TABLE:
            token.convert_units(unit_policy)
        yield token_type, token


def read_bundle_from_csv(
    input_path: PathLike, sep: Optional[str] = ";", unit_policy: Optional[units.UnitPolicy] = None
) -> TableBundle:
    """Read single csv-file to TableBundle"""
    inputs = read_csv(input_path, sep)

    if unit_policy is not None:
        if type(unit_policy) in {type, type(ABC)}:
            unit_policy = unit_policy() #  instantiate
        inputs = normalized_table_generator(unit_policy, inputs)

    return TableBundle(inputs)
