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

import pandas as pd
import warnings
from typing import Set, Dict, Optional, Iterable

from .table_metadata import TableMetadata, ColumnMetadata, TableData

_TABLE_DATA_FIELD_NAME = '_table_data'


class UnknownOperationError(Exception):
    pass


class InvalidTableCombineError(Exception):
    pass


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


def get_table_data(df: PandasTable, fail_if_missing=True, check_dataframe=True) -> Optional[
    TableData]:
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


