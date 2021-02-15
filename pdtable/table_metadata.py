from dataclasses import dataclass, field
from typing import Set, List, Optional, Dict, Union

import numpy
import pandas as pd


class InvalidNamingError(Exception):
    pass


class TableOrigin:
    """
    A TableOrigin instance uniquely defines the source of a Table instance.

    Subclasses should take care to define __str__.
    If possible, as_html() should be defined to include backlink to original input.
    """

    def as_html(self) -> str:
        return str(self)


class TableOriginCSV(TableOrigin):
    def __init__(self, file_name: str = "", row: int = 0):
        self._file_name = file_name
        self._row = row

    def __str__(self) -> str:
        return f'"{self._file_name}" row {self._row}'

    def __repr__(self) -> str:
        return f"TableOriginCSV({self})"


@dataclass
class TableMetadata:
    """
    Node in tree describing table sources.

    operation: Describes operation to create table, e.g. 'Created',
    'Loaded', 'Concatenated', 'Merged'

    Only parents or origin should be defined. Neither needs to be.
    """

    name: str
    destinations: Set[str] = field(default_factory=lambda: {"all"})
    operation: str = "Created"
    parents: List["TableMetadata"] = field(default_factory=list)
    origin: Optional[
        str
    ] = ""  # Should be replaced with a TableOrigin object to allow file-edit access
    transposed: bool = False

    def __str__(self):
        dst = (
            " for {{{}}}".format(", ".join(d for d in self.destinations))
            if self.destinations
            else ""
        )
        src = ""
        if self.origin:
            src = f" from {self.origin}"
        if self.parents:
            src = " from {{{}}}".format(",".join(f"\n{c}" for c in self.parents))
        return f'Table "{self.name}" {dst}. {self.operation}{src}'


class ColumnFormat:
    def __init__(self, specifier: Union[str, int]):
        """Specifies how to format the values in this column as strings.
        Args:
            specifier:
                Can be either of:
                * An int indicating the precision i.e. number of decimal places
                  e.g. 2 will format 123.456 as '123.46' and 42 as '42.00'
                * A standard format specifier conforming to the format specification mini-language:
                  https://docs.python.org/3/library/string.html#format-specification-mini-language
                  e.g. '14.3e' will format 123.456 as '     1.235e+02'
        """
        self.specifier = f".{specifier}f" if isinstance(specifier, int) else specifier

    def __str__(self):
        return self.specifier

    def __repr__(self):
        return f"{self.__class__.__name__}: '{self.specifier}'"


# See https://docs.scipy.org/doc/numpy/reference/generated/numpy.dtype.html
_unit_from_dtype_kind = {
    "b": "onoff",
    "i": "-",
    "u": "-",
    "f": "-",
    "M": "-",
    "O": "text",
    "S": "text",
    "U": "text",
}
_units_special = {"text", "onoff"}


def unit_from_dtype(dtype: numpy.dtype) -> str:
    try:
        return _unit_from_dtype_kind[dtype.kind]
    except KeyError:
        raise ValueError(
            "The numpy data type {dtype} is of kind {dtype.kind} which "
            "cannot be assigned a StarTable unit"
        )


@dataclass
class ColumnMetadata:
    """
    Column metadata is always stored in dic with name as key
    """

    unit: str
    display_unit: Optional[str] = None
    display_format: Optional[ColumnFormat] = None

    def check_dtype(self, dtype, context: Optional[str] = None):
        base_unit = unit_from_dtype(dtype)
        context_text = " in " + context if context else ""
        if base_unit in _units_special:
            if not base_unit == self.unit:
                raise Exception(
                    f"Column unit {self.unit} not equal to {base_unit} expected "
                    f"from data type {dtype}{context_text}"
                )
        elif self.unit in _units_special:
            raise Exception(
                f"Special column unit {self.unit} not applicable for "
                f"data type {dtype}{context_text}"
            )

    @classmethod
    def from_dtype(cls, dtype: numpy.dtype, **kwargs) -> "ColumnMetadata":
        """
        Will set column unit to '-', 'onoff', or 'text' depending on dtype
        """
        return cls(unit_from_dtype(dtype), **kwargs)

    def update_from(self, b: "ColumnMetadata"):
        self.unit = b.unit
        if not self.display_unit:
            self.display_unit = b.display_unit
        if not self.display_format and b.display_format:
            self.display_format = b.display_format.copy()

    def copy(self) -> "ColumnMetadata":
        c = ColumnMetadata(self.unit)
        c.update_from(self)
        return c


class ComplementaryTableInfo:
    """A ComplementaryTableInfo object is responsible for storing any table information
    not stored by native dataframe

    A TableDataFrame object is a dataframe with such a ComplementaryTableInfo object
    attached as metadata.
    """

    def __init__(
        self, table_metadata: TableMetadata, columns: Optional[Dict[str, ColumnMetadata]] = None
    ):
        self.metadata: TableMetadata = table_metadata
        self.columns: Dict[str, ColumnMetadata] = columns if columns is not None else {}
        # self.template # TODO Table template data should/could be included here
        # self.parametrization = None: Do not include, see discussion in module docs
        self._last_dataframe_state = None

    def __str__(self):
        return str(self.metadata)

    def _update_columns(self, df):
        columns = self.columns
        df_columns = df.columns.values
        df_cname_set = set(df_columns)

        # check for duplicate names in dataframe
        if not len(df_columns) == len(df_cname_set):
            raise InvalidNamingError("Duplicate column names not allowed for Table")

        # remove columns not in dataframe
        for name in set(columns.keys()) - df_cname_set:
            del columns[name]

        # update metadata
        for name in df_columns:
            dtype = df[name].dtype
            if name in columns:
                columns[name].check_dtype(dtype)
            else:
                columns[name] = ColumnMetadata.from_dtype(dtype)

    def _check_dataframe(self, df: pd.DataFrame):
        """
        Check that column register matches columns of dataframe
        """
        dataframe_state = df.dtypes
        if dataframe_state.equals(self._last_dataframe_state):
            return
        self._update_columns(df)
        self._last_dataframe_state = dataframe_state
