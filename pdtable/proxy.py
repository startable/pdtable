from typing import Union, Dict, List, Optional, Set, Callable, Sequence

import pandas as pd

from .frame import (
    TableDataFrame,
    get_table_info,
    is_table_dataframe,
    make_table_dataframe,
    set_units,
    add_column,
)
from .table_metadata import TableMetadata, ColumnMetadata, ComplementaryTableInfo
import pdtable  # for access to pdtable.units.default_converter

INCONVERTIBLE_UNIT_INDICATORS = ["text", "datetime", "onoff"]
UnitConverter = Callable[[float, str, str], float]
ColumnUnitDispatcher = Union[Sequence[str], Dict[str, str], Callable[[str], str]]


class UnitConversionNotDefinedError(ValueError):
    """Raised when a unit conversion is attempted on an inconvertible unit indicator"""

    pass


class Column:
    """
    Proxy for column in table

    TODO: Should we allow TableMetadata.unit to be None and then return "computed unit" as unit?
          Something like this would be needed if we are to change type of column
          via proxy interface. Alternative is to use "add_column()"
    """

    def __init__(self, df: TableDataFrame, name: str, table_info: ComplementaryTableInfo = None):
        self._name = name
        self._values = df[name]
        if not table_info:
            table_info = get_table_info(df)
        self._meta = table_info.columns[name]

    @property
    def name(self):
        return self._name

    @property
    def unit(self) -> str:
        """Column unit"""
        return self._meta.unit

    @unit.setter
    def unit(self, value: str):
        self._meta.unit = value

    @property
    def values(self):
        """Reference to the .array value of the underlying dataframe column

        Should not be edited directly.
        """
        return self._values.array

    @values.setter
    def values(self, values):
        self._values.update(pd.Series(values))

    def convert_units(self, to: Union[str, None], converter: UnitConverter):
        """Converts this column's units in place.

        Args:
            to:
                Can be any of:
                - '__base__': converts to the current unit's base unit
                - '__origin__': converts to the origin unit
                - other str: converts to this explicitly specified unit
                - None: no conversion
            converter:
                The converter.

        Returns:
            None

        """
        if to is None:
            # By convention, no unit conversion.
            return
        if to == self.unit:
            # It's already that unit. Nothing to do here.
            return
        if self.unit in INCONVERTIBLE_UNIT_INDICATORS:
            raise UnitConversionNotDefinedError(
                f"Unit conversion is not defined for unit '{self.unit}' of column '{self.name}'"
            )
        if to == "__origin__":
            # Convert to origin unit
            raise NotImplementedError  # TODO
        if to == "__base__":
            # Convert to base unit
            converted_values, to = converter(self.values, self.unit)
        else:
            # Convert to specified unit
            converted_values, _ = converter(self.values, self.unit, to)
        self.values = converted_values
        self.unit = to

    def to_numpy(self):
        """
        Value of column as numpy array. May require coercion and/or copying.
        """
        return self._values.to_numpy()

    def __repr__(self):
        return f"Column(name='{self.name}', unit='{self.unit}', values={self.values})"


class MissingUnitConverterError(ValueError):
    """Raised when a unit conversion cannot be done because no converter was made available."""

    pass


class Table:
    """
    A Table object is a facade for a backing TableDataFrame object.

    Can be created in two ways:
    1) From TableDataFrame object
       table = Table(tdf)
    2) From normal dataframe by including minimum metadata:
       table = Table(df, name='Foo')
       table = Table(df, ComplementaryTableInfo(name='Foo'))

    The .df property will return a dataframe subclass that retains all table information.
    To obtain a bare DataFrame object, use `pd.DataFrame(t.df)

    One possible performance issue is that since this facade does not have get messages
    from the backing object, we need to check the dataframe on each access.

    Note on performance: the Table class will check table_data against dataframe on each call.
    For situations where this is unacceptable for performance, use direct dataframe access methods.
    """

    def __init__(self, df: Union[None, TableDataFrame, pd.DataFrame] = None, **kwargs):
        if not (df is not None and is_table_dataframe(df)):
            # Creating a new table: initialize TableDataFrame
            df = make_table_dataframe(df if df is not None else pd.DataFrame(), **kwargs)
        elif kwargs:
            raise Exception(
                f"Got unexpected keyword arguments when creating Table object from "
                f"existing pandas table: {kwargs}"
            )
        self._df = df

    @property
    def df(self) -> TableDataFrame:
        """
        Return a pandas dataframe with all table information stored as
        metadata (a TableDataFrame object).

        This dataframe always exist and is the single source of truth for table data.
        The Table objects (this object) merely acts as a facade to allow simpler manipulation of
        associated metadata. It is consequently safe to simultaneously manipulate a Table object
        and the associated TableDataFrame object, as well as deleting the Table object.
        """
        return self._df

    @property
    def table_data(self) -> ComplementaryTableInfo:
        return get_table_info(self._df)

    @property
    def metadata(self) -> TableMetadata:
        return self.table_data.metadata

    @property
    def column_metadata(self) -> Dict[str, ColumnMetadata]:
        """
        Dictionary of column metadata objects

        It may be easier to use item access syntax (square brackets).
        Example:

        assert table.column_metadata['foo'].unit == table['foo'].unit
        """
        return self.table_data.columns

    # TODO: Rename columns -> column_names
    @property
    def column_names(self) -> List[str]:
        return self._df.columns.values.tolist()

    @property
    def column_proxies(self) -> List[Column]:
        df = self._df
        table_data = get_table_info(df)
        return [Column(df, name, table_info=table_data) for name in self.column_names]

    @property
    def units(self) -> List[str]:
        cols = self.column_metadata
        return [cols[name].unit for name in self.column_names]

    @property
    def name(self) -> str:
        return self.metadata.name

    @property
    def destinations(self) -> Set[str]:
        return self.metadata.destinations

    @units.setter
    def units(self, unit_values):
        set_units(self._df, unit_values)

    def get_row(self, index: int) -> List:
        # TODO call it 'row', and make it indexable, like iloc is?
        return self._df.iloc[index, :].values.tolist()

    def add_column(self, name: str, values, unit: Optional[str] = None, **kwargs):
        """
        Create or overwrite column.

        Example:
        table.add_column('some_column', 'm', range(4))

        An alternative is to use the item access syntax, e.g.:
        table['some_column'] = range(4)
        table['some_column'].unit = 'm'
        """
        add_column(self._df, name=name, values=values, unit=unit, **kwargs)

    def __iter__(self):
        table_data = self.table_data
        return (Column(self._df, name, table_info=table_data) for name in table_data.columns.keys())

    def __getitem__(self, name: str):
        """Get column proxy for existing column"""
        return Column(self._df, name)

    def __setitem__(self, name: str, values):
        """"Create or update column value

        If column is created, unit is derived from dtype.
        value: any value accepted for dataframe column creation.
        """
        add_column(self._df, name, values)

    def as_dataframe_with_annotated_column_names(self) -> pd.DataFrame:
        """
        Returns a dataframe with units included in column names
        """
        df = self._df.copy()
        cm = self.column_metadata
        df.columns = [f"{c} [{cm[c].unit}]" for c in df.columns]
        return df

    def __repr__(self):
        m = self.metadata
        # TODO Could also display units on their own line, to look less like a dataframe and more like a table block...  # noqa
        return (
            f"**{m.name}\n{' '.join(s for s in m.destinations)}\n" + self.as_dataframe_with_annotated_column_names().to_string(index=False)
        )

    def __str__(self):
        return repr(self)

    def __metadata_comp_key(self):
        """Metadata comparison key, for use in __eq__"""
        return self.name, self.metadata.destinations, self.column_names, self.units

    def equals(self, other):
        """Checks whether the other Table has the same header and data as this one.

        Checks whether the other Table has the same name, destinations, column names, column units,
        and data values as this one. Origin is ignored (tables can be equal regardless of where they
        came from).

        Number equality does not take data type into account, in keeping with the StarTable
        convention that a number is just a number. For example, float 10.0 equals int 10.

        This is implemented as a method rather than as the __eq__() magic method because a later
        implementation of __hash__() attempting to live by the rule
        "a == b implies hash(a) == hash(b)" would have to do a deep dive in the table data
        to force equal hashes on equal numbers, even when they naturally have different hashes due
        to having different data types.
        """
        if isinstance(other, self.__class__):
            self_key = self.__metadata_comp_key()
            other_key = other.__metadata_comp_key()
            return self_key == other_key and _df_elements_all_equal_or_same(self._df, other._df)
            # Had to implement this custom equality checker for DataFrames because,
            # as of pandas 1.1.0, stupid pandas.DataFrame.equals return False when elements have
            # different dtypes e.g. 10 and 10.0 are considered 'not equal'. In StarTable, a number
            # is just a number, and no such distinction should be made between data types.
        return False

    def convert_units(self, to: ColumnUnitDispatcher, converter: UnitConverter = None) -> "Table":
        """Applies unit conversion to columns, modifying table in-place

        Returns a new table with converted units. The desired new unit are specified by
        column.

        How to do a conversion from unit X to unit Y is determined by the supplied unit
        converter.

        The converter is also responsible for deciding what unit is considered the
        "base unit" of unit X.  Depending on your application and favourite unit system, the base
        unit of 'mm' could be 'm', 'foot', 'furlong', or some other unit of dimension length.
        
        Args:
            to:
                Specifies to what units to convert which columns. Can be:
                - 'base': Converts all columns to their respective base units. Columns with
                  inconvertible unit indicators are skipped.
                - 'origin': (not yet implemented!) Converts all columns to their respective origin
                  units. Columns with inconvertible unit indicators are skipped.
                - A dictionary of {column_name: target_unit}. Superfluous column names are ignored.
                - A callable with one argument: column name. Must return the target unit, or None
                  if no unit conversion is to be done.
                - A list specifying the target unit of each column by position. (A None element
                  implies no conversion for that column.) This is discouraged in production, as
                  there is no check that the units are applied to the right columns by column name;
                  but it is a useful shorthand during experimentation.

                Individual columns' target units can be specified as any of:
                - '__base__': the column's current unit's base unit
                - '__origin__': the column's origin unit
                - other str: explicitly specified unit
                - None: keep the column's current unit; do no conversion.

            converter:
                A callable that converts values from one unit to another.
                If None (default), attempts to fall back on pdtable.units.default_converter.
                The converter must fulfill the following specification:
                - Takes three positional arguments:
                    0. value to be converted
                    1. from_unit
                    2. Optional: to_unit. If the caller does not specify to_unit, the converter
                       should assume that the target unit is from_unit's base unit. If this
                       assumption is not implemented in the converter, then calls to
                       convert_units(to='base') will fail.
                - Accepts units of type returned by the ColumnUnitDispatcher.
                - Returns a tuple of two elements:
                    0.  The value with unit conversion applied. Should be NaN if input value is NaN.
                    1.  The converter's string representation of the target unit. This may differ
                        from to_unit. For example, a pint-based converter called with to_unit="mm"
                        would return "millimeter".

        Returns:
            A new Table with converted units.

        """
        default_converter = pdtable.units.default_converter
        if converter is None:
            if default_converter is None:
                raise MissingUnitConverterError(
                    "No converter or default converter was specified.",
                    converter,
                    default_converter,
                )
            converter = default_converter

        new_table = Table(self.df.copy())

        if to == "origin":
            for col in new_table.column_proxies:
                if col.unit in INCONVERTIBLE_UNIT_INDICATORS:
                    # Skip this column
                    continue
                col.convert_units("__origin__", converter)

        elif to == "base":
            # Convert all columns to their respective base units
            for col in new_table.column_proxies:
                if col.unit in INCONVERTIBLE_UNIT_INDICATORS:
                    # Skip this column
                    continue
                col.convert_units("__base__", converter)

        elif isinstance(to, Sequence):
            if len(to) != len(self.column_proxies):
                raise ValueError(
                    "Unequal number of columns and of 'to' units", len(self.column_proxies), len(to)
                )
            for col, to_unit in zip(new_table.column_proxies, to):
                if to_unit is not None:
                    col.convert_units(to_unit, converter)

        elif isinstance(to, Dict):
            for column in new_table.column_proxies:
                to_unit = to.get(column.name)
                if to_unit is not None:
                    column.convert_units(to[column.name], converter)

        elif isinstance(to, Callable):
            for column in new_table.column_proxies:
                to_unit = to(column.name)
                if to_unit is not None:
                    column.convert_units(to_unit, converter)

        else:
            raise TypeError("Column unit dispatcher of unexpected type.", type(to), to)

        return new_table


def _equal_or_same(a, b):
    """Returns True if both values are equal or 'the same thing' (e.g. NaN's).

    Note that np.nan != float('nan') != pd.NA etc., so we collapse all of these
    missing value things using pd.isna()
    """
    return a == b or a is b or (pd.isna(a) and pd.isna(b))


def _df_elements(df):
    """Yields all the values in the data frame serially."""
    return (x for row in df.itertuples() for x in row)


def _df_elements_all_equal_or_same(df1, df2):
    """Returns True if all corresponding elements are equal or 'the same' in both data frames."""
    try:
        return all(_equal_or_same(x1, x2) for x1, x2 in zip(_df_elements(df1), _df_elements(df2)))
    except Exception:
        # If the comparison can't be made, then clearly they aren't the same
        return False
