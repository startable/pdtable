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
    Simple table store with no regard for destinations

    Ignores everything but Table blocks.
    """

    def __init__(self, block_gen: BlockGenerator, as_Table: bool = True):
        self._tables = {}
        self._indexed = []
        for block_type, block in block_gen:
            if block_type != BlockType.TABLE:
                continue
            if not hasattr(block, "name"):
                # Could be e.g. JsonData or other alternative data structure
                self._indexed.append(block)
                continue
            table = block
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

    def __getitem__(self, idx: Union[str,int]) -> TableType:
        """ Allow tb[0] as well as tb["tname"] """
        if isinstance(idx,str):
            return self.unique(idx)
        if isinstance(idx,int):
            return self._indexed[idx]

        raise NotImplementedError(f"getitem of type: {type(idx)}")

    def __iter__(self) -> Iterator[str]:
        """Iterator over tables"""
#        all = []
#        for tn in self._tables.keys():
#            all.extend(self._tables[tn])
        return iter(self._indexed)

    def __len__(self):
        size = 0
        for tn in self._tables:
            size += len(self._tables[tn])
        return size

    def unique(self, name: str) -> TableType:
        """Returns table if there is exactly only one table of this name, raises error otherwise."""
        lst = self._tables.get(name)
        if lst is not None and len(lst) == 1:
            return lst[0]
        raise NotImplementedError()

    def all(self, name: str) -> List[TableType]:
        """Returns all tables with this name."""
        raise self._tables.get(name)
