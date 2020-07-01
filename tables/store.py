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
from typing import Iterable, Tuple, Any, Iterator, Optional
from .pdtable import PandasTable


class StarBlockType(Enum):
    """
    An enumeration of the tokens types that may be emitted by a reader.

    Design note
    Members of this enum are used to tag token type to avoid introspection.
    To aid reusable generation of metadata, it could be relevant to include
    synthetic block types FILE_BEGIN/END, SHEET_BEGIN/END.
    """
    DIRECTIVE = auto()
    TABLE = auto()          # Interface: TableType
    TEMPLATE_ROW = auto()
    METADATA = auto()
    BLANK = auto()


BlockGenerator = Iterable[Tuple[StarBlockType, Optional[Any]]]

TableType = PandasTable


class TableBundle:
    """
    Simple table store with no regard for destinations

    Ignores everything but Table-tokens.
    Both get_attr and get_item returns PandasTable instances.
    These can be wrapped in pdtable.Table facades for access to metadata (units etc.)

    For discoverability, it would be better to return Table facade objects directly,
    but the current approach has the advantage of allowing normal dataframes.
    """

    def __init__(self, ts: BlockGenerator):
        self._tables = {token.name: token.pdtable for token_type, token in ts
                        if token is not None and token_type == StarBlockType.TABLE}

    def __getattr__(self, name: str) -> TableType:
        return self._tables[name]

    def __getitem__(self, name: str) -> TableType:
        return self._tables[name]

    def __iter__(self) -> Iterator[str]:
        """Iterator over table names"""
        return iter(self._tables)

    def __len__(self):
        return self._tables.__len__()


