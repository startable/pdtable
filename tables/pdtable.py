"""
The `pdtable` module allows working with StarTable tables as pandas dataframes.

This is implemented by providing both `Table` and `PandasTable` (dataframe) interfaces to the same object.

## Idea

The central idea is that as much as possible of the table information is stored as a pandas dataframe,
and that the remaining information is stored as a `TableData` object attached to the dataframe as registered metadata.
Further, access to the full table datastructure is provided through a facade object (of class `Table`). `Table` objects
have no state (except the underlying decorated dataframe) and are intended to be created when needed and discarded
afterwards:

```
dft = make_pdtable(...)
unit_height = Table(dft).height.unit
```

Advantages of this approach are that:

1. Code can be written for (and tested with) pandas dataframes and still operate on `pdtable` objects.
   This avoids unnecessary coupling to the startable project.
2. The table access methods of pandas are available for use by consumer code. This both saves the work
   of making startable-specific access methods, and likely allows better performance and documentation.

## Implementation details

The decorated dataframe objects are represented by the `PandasTable` class.

### Dataframe operations

Pandas allows us to hook into operations on dataframes via the `__finalize__` method.
This makes it possible to propagate table metadata over select dataframe operations.
See `PandasTable` documentation for details.

### Column metadata

Propagating metadata would be greatly simplified if column-specific metadata was stored with the column.
However, metadata attached to `Series` object  is not retained within the dataframe, see
https://github.com/pandas-dev/pandas/issues/6923#issuecomment-41043225.

## Alternative approaches

It should be possible to maintain coloun metadata together with the column data through the use of
`ExtensionArray`. This option was discarded early due to performance concerns, but might be viable and
would the be preferable to the chosen approach.
"""


import abc
from dataclasses import dataclass, field
import pandas as pd
import warnings
from typing import List, Union, Set, Dict, Optional, Iterable
import numpy  # for types only
from pathlib import Path

_TABLE_DATA_FIELD_NAME = '_table_data'


class UnknownOperationError(Exception):
    pass


class InvalidNamingError(Exception):
    pass


class InvalidTableCombineError(Exception):
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
    def __init__(self, file_name: str = '', row: int = 0):
        self._file_name = file_name
        self._row = row

    def __str__(self) -> str:
        return f'"{self._file_name}" row {self._row}'

    def __repr__(self) -> str:
        return f'TableOriginCSV({self})'


@dataclass
class TableMetadata:
    """
    Node in tree describing table sources. 

    operation: Describes operation to create table, e.g. 'Created', 'Loaded', 'Concatenated', 'Merged'

    Only parents or origin should be defined. Neither needs to be.
    """
    name: str
    destinations: Set[str] = field(default_factory=lambda :{'all'})
    operation: str = 'Created'
    parents: List['TableMetadata'] = field(default_factory=list)
    origin: Optional[str] = ''  # Should be replaced with a TableOrigin object to allow file-edit access

    def __str__(self):
        dst = ' for {{}}'.format(', '.join(d for d in self.destinations)) if self.destinations else ''
        src = ''
        if self.origin:
            src = f' from {self.origin}'
        if self.parents:
            src = ' from {{}}'.format(','.join(f'\n{c}' for c in self.parents))
        return f'Table "{self.name}" {dst}. {self.operation}{src}'


@dataclass
class ColumnFormat:
    precision: int = None

    def copy(self) -> 'ColumnFormat':
        return ColumnFormat(**vars(self))


# See https://docs.scipy.org/doc/numpy/reference/generated/numpy.dtype.html
_unit_from_dtype_kind = {
    'b': 'onoff',
    'i': '-',
    'u': '-',
    'f': '-',
    'M': '-',
    'O': 'text',
    'S': 'text',
    'U': 'text'
}
_units_special = {'text', 'onoff'}


def unit_from_dtype(dtype: numpy.dtype) -> str:
    try:
        return _unit_from_dtype_kind[dtype.kind]
    except KeyError:
        raise ValueError(
            'The numpy data type {dtype} is of kind {dtype.kind} which cannot be assigned a startable unit')


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
        context_text = ' in '+context if context else ''
        if base_unit in _units_special:
            if not base_unit == self.unit:
                raise Exception(
                    f'Column unit {self.unit} not equal to {base_unit} expected from data type {dtype}{context_text}')
        elif self.unit in _units_special:
            raise Exception(f'Special column unit {self.unit} not applicable for data type {dtype}{context_text}')

    @classmethod
    def from_dtype(cls, dtype: numpy.dtype, **kwargs) -> 'ColumnMetadata':
        """
        Will set column unit to '-', 'onoff', or 'text' depending on dtype
        """
        return cls(unit_from_dtype(dtype), **kwargs)

    def update_from(self, b: 'ColumnMetadata'):
        self.unit = b.unit 
        if not self.display_unit:
            self.display_unit = b.display_unit
        if not self.display_format and b.display_format:
            self.display_format = b.display_format.copy()

    def copy(self) -> 'ColumnMetadata':
        c = ColumnMetadata(self.unit)
        c.update_from(self)
        return c


class TableData:
    """A TableData object is responsible for storing any table information not stored by native dataframe

    A PandasTable object is a dataframe with such a TableData object attached as metadata.
    """
    def __init__(self, metadata, columns: Optional[Dict[str, ColumnMetadata]]=None):
        self.metadata: TableMetadata = metadata
        self.columns: Dict[str, ColumnMetadata] = columns if columns is not None else dict()
        # self.template #Table template data should be included here
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
            raise InvalidNamingError('Duplicate column names not allowed for Table')

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


def _combine_tables(obj: 'PandasTable', other, method, **kwargs) -> TableData:
    """
    Called from dataframe.__finalize__ when dataframe operations have been performed
    on the dataframe backing a table.

    Implementation policy is that this will fail except for situations where
    the metadata combination is safe.
    For other cases, the operations should be implemented via the Table facade
    """

    if method is None:
        # copy, slicing
        src = [other]
    elif method == 'merge':
        src = [other.left, other.right]
    elif method == 'concat':
        src = other.objs
    else:
        raise UnknownOperationError(f'Unknown method while combining metadata: {method}. Keyword args: {kwargs}')

    if len(src) == 0:
        raise UnknownOperationError(f'No operands for operation {method}')

    data = [get_table_data(s) for s in src if is_pdtable(s)]

    # 1: Create table metadata as combination of all
    meta = TableMetadata(
        name=data[0].metadata.name,
        operation=f'Pandas {method}',
        parents=[d.metadata for d in data])

    # 2: Check that units match for columns that appear in more than one table
    out_cols: Set[str] = set(obj.columns)
    columns: Dict[str, ColumnMetadata] = dict()
    for d in data:
        for name, c in d.columns.items():
            if name not in out_cols:
                continue
            col = columns.get(name, None)
            if not col:
                # not seen before in input
                col = c.copy()
                columns[name] = col
            else:
                if not col.unit == c.unit:
                    raise InvalidTableCombineError(
                        f'Column {name} appears with incompatible units "{col.unit}" and "{c.unit}".')
                col.update_from(c)

    return TableData(metadata=meta, columns=columns)


class PandasTable(pd.DataFrame):
    """
    A pandas dataframe subclass with associated table metadata.

    Behaves exactly as a dataframe with and will try to retain metadata
    through pandas operations. If this is not possible, the manipulations
    will return a normal dataframe.

    No table-specific methods are available directly on this class, and PandasTable
    objects should not be created directly.
    Instead, use either the methods in the pdtable module, or the table proxy
    object which may be constructed for a PandasTable object via
    Table(dft).
    """
    _metadata = [_TABLE_DATA_FIELD_NAME]  # Register metadata fieldnames here

    # If implemented, must handle metadata copying etc
    # def __init__(self, *args, **kwargs):
    #    super().__init__(*args, **kwargs)

    # These are implicit by inheritance from dataframe
    # We could override _constructor_sliced to add metadata to exported columns
    # but not clear if this would add value
    #     @property
    #     def _constructor_expanddim(self):
    #         return pd.DataFrame
    #     @property
    #     def _constructor_sliced(self):
    #         return pd.Series

    @property
    def _constructor(self):
        return PandasTable

    def __finalize__(self, other, method=None, **kwargs):
        """
        This method is responsible for populating metadata
        when creating new Table-object.

        If left out, no metadata would be retained across table
        operations. This might be a viable solution?
        Alternatively, we could return a raw frame object on some operations
        """

        try:
            data = _combine_tables(self, other, method, **kwargs)
            object.__setattr__(self, _TABLE_DATA_FIELD_NAME, data)
        except UnknownOperationError as e:
            warnings.warn(f'Falling back to dataframe: {e}')
            return pd.DataFrame(self)
        return self

    @staticmethod
    def from_table_data(df: pd.DataFrame, data: TableData) -> 'PandasTable':
        df = PandasTable(df)
        object.__setattr__(df, _TABLE_DATA_FIELD_NAME, data)
        data._check_dataframe(df)
        return df


def is_pdtable(df: pd.DataFrame) -> bool:
    return _TABLE_DATA_FIELD_NAME in df._metadata


def make_pdtable(
        df: pd.DataFrame,
        units: Optional[Iterable[str]] = None,
        unit_map: Optional[Dict[str, str]] = None,
        metadata: Optional[TableMetadata] = None,
        **kwargs) -> PandasTable:
    """
    Create PandasTable object from dataframe and table metadata

    Unknown keyword arguments (e.g. `name = ...`) are used to create `TableMetadata` object.
    Alternatively, a `TableMetadata` object can be provided directly.

    Either units (list of units for all columns) or unit_map can be provided. Otherwise default units are assigned.

    Example: 
    dft = make_pdtable(df, name='MyTable')
    """

    # build metadata
    if (metadata is not None) == bool(kwargs):
        raise Exception('Supply either metadata or keyword-arguments for TableMetadata constructor')
    if kwargs:
        # This is intended to fail if args are insufficient
        metadata = TableMetadata(**kwargs)

    df = PandasTable.from_table_data(df, data=TableData(metadata=metadata))

    # set units
    if units and unit_map:
        raise Exception('Supply at most one of unit and unit_map')
    if units is not None:
        set_all_units(df, units)
    elif unit_map is not None:
        set_units(df, unit_map)

    return df


def get_table_data(df: PandasTable, fail_if_missing=True, check_dataframe=True) -> Optional[TableData]:
    """
    Get TableData from existing PandasTable object. 

    When called with default options, get_table_data will either raise an exception
    or return a TableData object with a valid ColumnMetadata defined for each column.

    check: Check that the table data is valid with respect to dataframe.
           If the dataframe has been manipulated directly, table will be updated to match.
    fail_if_missing: Whether to raise an exception if TableData object is missing
    """
    name: str = _TABLE_DATA_FIELD_NAME
    if name not in df._metadata:
        raise Exception('Attempt to extract table data from normal pd.DataFrame object.'
                        'TableData can only be associated with PandasTable objects')
    table_data = getattr(df, _TABLE_DATA_FIELD_NAME, None)
    if not table_data:
        if fail_if_missing:
            raise Exception(
                'Missing TableData object on PandasTable.'
                'PandasTable objects should be created via make_pdtable or an intermediate Table object')
    elif check_dataframe:
        table_data._check_dataframe(df)
    return table_data


# example of manipulator function that directly manipulates PandasTable objects without constructing Table facade
def add_column(df: PandasTable, name: str, values, unit: Optional[str] = None, **kwargs):
    """
    Add or update column in table. If omitted, unit will be computed from value dtype

    keyword arguments will be forwarded to ColumnMetadata constructor together with unit
    """
    df[name] = values
    columns = get_table_data(df, check_dataframe=False).columns

    new_col =  ColumnMetadata.from_dtype(df[name].dtype, **kwargs) if unit is None \
        else ColumnMetadata(unit=unit, **kwargs)

    col = columns.get(name, None)
    if col is None:
        columns[name] = new_col
    else:
        # Existing column
        col.update_from(new_col)


def set_units(df: PandasTable, unit_map: Dict[str, str]):
    columns = get_table_data(df).columns
    for col, unit in unit_map.items():
        columns[col] = unit


def set_all_units(df: PandasTable, units: Iterable[Optional[str]]):
    """
    Set units for all columns in table.
    """
    columns = get_table_data(df).columns
    for col, unit in zip(df.columns, units):
        columns[col].unit = unit


class Column:
    """
    Proxy for column in table

    TODO: Should we allow TableMetadata.unit to be None and then return "computed unit" as unit?
          Something like this would be needed if we are to change type of column
          via proxy interface. Alternative is to use "add_column()"
    """
    def __init__(self, df: PandasTable, name: str, table_data: TableData = None):
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
    A Table object is a facade for a backing PandasTable object. 

    Can be created in two ways:
    1) From PandasTable object
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
    def __init__(self, df: Union[None, PandasTable, pd.DataFrame] = None, **kwargs):
        if not (df is not None and is_pdtable(df)):
            # Creating a new table: initialize PandasTable
            df = make_pdtable(df if df is not None else pd.DataFrame(), **kwargs)
        elif kwargs:
            raise Exception(f'Got unexpected keyword arguments when creating Table object from '
                            f'existing pandas table: {kwargs}')
        self._df = df

    @property
    def df(self) -> PandasTable:
        """
        Return a pandas dataframe with all table information stored as metadata (a PandasTable object).

        This dataframe always exist and is the single source of truth for table data.
        The Table obects (this object) merely acts as a facade to allow simpler manipulation of
        associated metadata. It is consequently safe to simultaneously manipulate a Table object and the
        associated PandasTable object, as well as deleting the Table object.
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
    def destinations(self) -> str:
        return ", ".join(s for s in self.metadata.destinations)

    @units.setter
    def units(self, unit_values):
        set_units(self._df, unit_values)

    def get_row(self, index: int) -> List:
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
        df.columns = [f'{c} [{cm[c].unit}]' for c in df.columns]
        return df

    def __repr__(self):
        m = self.metadata
        return f'**{m.name}\n{", ".join(s for s in m.destinations)}\n{self.as_dataframe_with_annotated_column_names()}'

    def __str__(self):
        return repr(self)

