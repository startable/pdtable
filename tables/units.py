from abc import ABC, abstractmethod
from typing import Tuple, Any
from .pdtable import Table as TableType


class UnitPolicy(ABC):
    @abstractmethod
    def convert_value_to_base(self, value, unit: str, column_name: str) -> Tuple[Any, str]:
        """
        Convert a value, unit - tuple to base unit
        """
        pass


def normalize_table_in_place(unit_policy: UnitPolicy, table: TableType):
    """Apply unit policy, modifying table in-place"""
    for column in table.column_proxies:
        unit = column.unit
        new_values, new_unit = unit_policy.convert_value_to_base(column.values, unit, column.name)
        if not unit == new_unit:
            column.values = new_values
            column.unit = new_unit


