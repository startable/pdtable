"""
The store module implements data structures for storing collections of tables.

Plan is to implement processing of a stream of StarBlockType tokens
in a generic way that can be reused across readers and storage backends.
Examples include:
- attach template tokens to previous table
- attach file level metadata to subsequent tables
- unit normalization
- directive handling
"""

import sys
from enum import Enum, auto
from typing import Iterable, Tuple, Any, Iterator, Optional, Union, List
from .frame import TableDataFrame
from .proxy import Table


class BlockType(Enum):
    """
    An enumeration of the tokens types that may be emitted by a reader.

    Design note
    Members of this enum are used to tag token type to avoid introspection.
    To aid reusable generation of metadata, it could be relevant to include
    synthetic block types FILE_BEGIN/END, SHEET_BEGIN/END.
    """

    DIRECTIVE = auto()
    TABLE = auto()  # Interface: TableType
    TEMPLATE_ROW = auto()
    METADATA = auto()
    BLANK = auto()


BlockGenerator = Iterable[Tuple[BlockType, Optional[Any]]]

TableType = Union[Table, TableDataFrame]


class TableBundle:
    """
    Simple table store with no regard for destinations.

    Upon creation from a block generator, ignores everything but Table blocks.

    Table blocks can be supplied as Table objects or any alternative data structure e.g. JsonData.
    If supplied as Table objects, the table blocks can be stored as TableDataFrame by setting
    as_Table=False.
    """

    def __init__(self, block_gen: BlockGenerator, as_Table: bool = True):
        self._tables = {}
        self._indexed = []
        for block_type, block in block_gen:
            if block_type != BlockType.TABLE:
                continue
            table = block
            if not hasattr(table, "name"):
                # Could be e.g. JsonData or other alternative data structure
                self._indexed.append(table)
                continue
            if self._tables.get(table.name) is None:
                self._tables[table.name] = []
            if as_Table:
                self._tables[table.name].append(table)
                self._indexed.append(table)
            else:
                self._tables[table.name].append(table.df)
                self._indexed.append(table.df)

    def __getattr__(self, name: str) -> TableType:
        return self.unique(name)

    def __getitem__(self, idx: Union[str, int]) -> TableType:
        """Get table by numerical index or by name.

        Allows dual syntax: bundle[0] as well as bundle['table_name']
        """
        if isinstance(idx, str):
            return self.unique(idx)
        if isinstance(idx, int):
            return self._indexed[idx]

        raise TypeError(f"getitem of type: {type(idx)}")

    def __iter__(self) -> Iterator[str]:
        """Iterator over tables"""
        return iter(self._indexed)

    def __len__(self):
        """Total number of tables in this bundle"""
        return sum(len(self._tables[name]) for name in self._tables)

    def unique(self, name: str) -> TableType:
        """Returns table if there is exactly only one table of this name, raises error otherwise."""
        lst = self._tables.get(name)
        if lst is not None and len(lst) == 1:
            return lst[0]
        raise LookupError()

    def all(self, name: str) -> List[TableType]:
        """Returns all tables with this name."""
        lst = self._tables.get(name)
        if lst is not None:
            return lst
        raise KeyError()
