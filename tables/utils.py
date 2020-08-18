from os import PathLike
from typing import Optional, Any, Tuple, Iterable

from .pdtable import Table
from . import units
from .store import TableBundle, BlockType
from .readers.read_csv import read_file_csv


def normalized_table_generator(unit_policy, ts: Iterable[Tuple[BlockType, Optional[Any]]]):
    for token_type, token in ts:
        if token is not None and token_type == BlockType.TABLE:
            units.normalize_table_in_place(unit_policy, token)  # in place
        yield token_type, token


def read_bundle_from_csv(input_path: PathLike,
                         sep: Optional[str] = ';',
                         unit_policy: Optional[units.UnitPolicy] = None
                         ) -> TableBundle:
    """Read single csv-file to TableBundle"""
    inputs = read_file_csv(input_path, sep)
    if unit_policy is not None:
        inputs = normalized_table_generator(unit_policy, inputs)
    return TableBundle(inputs)
