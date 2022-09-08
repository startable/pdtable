from __future__ import annotations
from dataclasses import dataclass, field
from typing import (
    TypeVar,
    Callable,
    Iterator,
    Iterable,
)

from pdtable.proxy import Table
from pdtable.table_origin import LoadLocation

T = TypeVar("T")


@dataclass
class LocationTreeNode:
    """
    A tree structure with each node mapping to a LoadLocation

    Leaf nodes will correspond to LocationBlock objects with the corresponding
    table available as ``.table``.
    """

    location: LoadLocation
    table: None | Table = None
    parent: None | "LocationTreeNode" = None
    children: list["LocationTreeNode"] = field(default_factory=list)

    def add_child(self, child: LocationTreeNode):
        self.children.append(child)
        child.parent = self

    def visit_all(
        self, visitor: Callable[[int, "LocationTreeNode"], T], level: int = 0
    ) -> Iterator[T]:
        """
        Visitor will be called as (level, node) on self and children

        Results available as iterator.
        """
        yield visitor(level, self)
        for child in self.children:
            yield from child.visit_all(level=level + 1, visitor=visitor)

    def __str__(self) -> str:
        def str_visitor(level, node):
            if node.table is not None:
                return f"{'  '*level}**{node.table.name}"
            else:
                return (
                    f"{'  '*level}{node.location.interactive_identifier if node.location else ''}"
                )

        return "\n".join(self.visit_all(visitor=str_visitor))


def make_location_trees(tables: Iterable[Table]) -> list[LocationTreeNode]:
    """
    Return a graph representation of the origins for given tables

    Graph is a collection of trees:
        B LocationFile
          L LocationBlock # table
          B LocationBlock # include
            B LocationFile
                    ....
                L LocationBlock  # table
        B LocationFolder
        B LocationFile
            L LocationBlock

    The implementation relies on ``load_identifier`` to be unique for a LocationFile object.

    Return:
        List of root origins
    """
    # Implementation note:
    # To extend to non-compliant readers that do not implement unique load identifiers,
    # a check for object identify could be added.

    # Create leaf nodes and immediate parents, track by load_identifier
    # Since load_identifier for LocationBlock just refers to parent file,
    # individual tables are not registered
    buf: dict[str, LocationTreeNode] = {}

    def register_node(location: LoadLocation, child: LocationTreeNode):
        """
        Add location and ancestors to buf by load_identifier

        Should not be called for individual LocationBlock
        """
        if location.load_identifier in buf:
            buf[location.load_identifier].add_child(child)
            return
        new_node = LocationTreeNode(location=location)
        new_node.add_child(child)
        buf[location.load_identifier] = new_node
        source = location.load_specification.source
        if source is not None:
            register_node(source, child=new_node)

    for t in tables:
        if t.metadata.origin is None:
            raise ValueError("Table object without origin not supported for `make_origin_trees`", t)
        location = t.metadata.origin.input_location
        if location is None:
            if t.metadata.origin.parents:
                raise NotImplementedError("Support for non-loaded tables not implemented")
            else:
                raise ValueError("Missing input_location for table", t)
        leaf = LocationTreeNode(location=location, table=t)
        register_node(location.file, child=leaf)

    # return nodes without parent as roots:
    return [v for v in buf.values() if v.parent is None]
