from abc import ABC, abstractmethod
from typing import Tuple, Any
from . import Table as TableType


class UnitPolicy(ABC):
    @abstractmethod
    def convert_value_to_base(self, value, unit: str) -> Tuple[Any, str]:
        """
        Convert a value, unit - tuple to base unit
        """
        pass

    @property
    def column_name(self):
        return self._column_name

    @column_name.setter
    def column_name(self, value):
        self._column_name = value

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value


def normalize_table_in_place(unit_policy: UnitPolicy, table: TableType):
    """Apply unit policy, modifying table in-place"""
    unit_policy.table_name = table.name
    for column in table.column_proxies:
        unit = column.unit
        unit_policy.column_name = column.name
        new_values, new_unit = unit_policy.convert_value_to_base(column.values, unit)
        if not unit == new_unit:
            column.values = new_values
            column.unit = new_unit
