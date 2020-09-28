from typing import Union, Dict, List, Optional, Set

import pandas as pd

from .units import UnitPolicy
from .frame import (
    TableDataFrame,
    get_table_data,
    is_table_dataframe,
    make_table_dataframe,
    set_units,
    add_column,
)
from .table_metadata import TableMetadata, ColumnMetadata, TableData


class Column:
    """
    Proxy for column in table

    TODO: Should we allow TableMetadata.unit to be None and then return "computed unit" as unit?
          Something like this would be needed if we are to change type of column
          via proxy interface. Alternative is to use "add_column()"
    """

    def __init__(self, df: TableDataFrame, name: str, table_data: TableData = None):
        self._name = name
        self._values = df[name]
        if not table_data:
            table_data = get_table_data(df)
        self._meta = table_data.columns[name]

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

    def to_numpy(self):
        """
        Value of column as numpy array. May require coercion and/or copying.
        """
        return self._values.to_numpy()

    def __repr__(self):
        return f"Column(name='{self.name}', unit='{self.unit}', values={self.values})"


class Table:
    """
    A Table object is a facade for a backing TableDataFrame object.

    Can be created in two ways:
    1) From TableDataFrame object
       table = Table(tdf)
    2) From normal dataframe by including minimum metadata:
       table = Table(df, name='Foo')
       table = Table(df, TableData(name='Foo'))

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
        Return a pandas dataframe with all table information stored as metadata (a TableDataFrame object).

        This dataframe always exist and is the single source of truth for table data.
        The Table obects (this object) merely acts as a facade to allow simpler manipulation of
        associated metadata. It is consequently safe to simultaneously manipulate a Table object and the
        associated TableDataFrame object, as well as deleting the Table object.
        """
        return self._df

    @property
    def table_data(self) -> TableData:
        return get_table_data(self._df)

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
        table_data = get_table_data(df)
        return [Column(df, name, table_data=table_data) for name in self.column_names]

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
        return (Column(self._df, name, table_data=table_data) for name in table_data.columns.keys())

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
        # TODO __repr__ shouldn't display the dataframe's index. Could also display units on their own line.
        return f'**{m.name}\n{", ".join(s for s in m.destinations)}\n{self.as_dataframe_with_annotated_column_names()}'

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

    def convert_units(self, unit_policy: UnitPolicy):
        """Apply unit policy, modifying table in-place"""
        # TODO a convenient way to specify "pls convert back to display_units"
        unit_policy.table_name = self.name
        for column in self.column_proxies:
            unit = column.unit
            unit_policy.column_name = column.name
            new_values, new_unit = unit_policy.convert_value_to_base(column.values, unit)
            if not unit == new_unit:
                column.values = new_values
                column.unit = new_unit


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
    except:
        return False
