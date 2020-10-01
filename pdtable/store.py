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
        for block_type, block in block_gen:
            if block_type != BlockType.TABLE:
                continue
            if not hasattr(block, "df"):
                # Could be e.g. JsonData or other alternative data structure
                continue
            table = block
            assert self._tables.get(table.name) is None  # TODO add to list for this name
            if as_Table:
                self._tables[table.name] = table  # TODO add to list for this name
            else:
                self._tables[table.name] = table.df  # TODO add to list for this name

    def __getattr__(self, name: str) -> TableType:
        # TODO should return bundle.unique(name)
        return self._tables[name]

    def __getitem__(self, name: str) -> TableType:
        # TODO should return bundle.unique(name)
        return self._tables[name]

    def __iter__(self) -> Iterator[str]:
        """Iterator over table names"""
        # TODO return the tables themselves, not just name
        return iter(self._tables)

    def __len__(self):
        return self._tables.__len__()

    def unique(self, name: str) -> TableType:
        """Returns table if there is exactly only one table of this name, raises error otherwise."""
        raise NotImplementedError()

    def all(self, name: str) -> List[TableType]:
        """Returns all tables with this name."""
        raise NotImplementedError()
