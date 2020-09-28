from abc import ABC, abstractmethod
from typing import Tuple, Any


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
