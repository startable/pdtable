from __future__ import annotations
from abc import abstractmethod
import logging
import typing

from typing_extensions import Protocol  # Protocol is not available in python 3.7

from pdtable.store import BlockIterator
from pdtable.table_origin import (
    InputIssueTracker,
    LoadItem,
    LoadLocation,
)

logger = logging.getLogger(__name__)


class LoadError(Exception):
    pass


class LoadOrchestrator(Protocol):
    """
    The load orchestrator provides state during the load process

    In contrast to ``Loader`` and ``Reader`` instances, which should be
    reusable across load operations, the orchestrator should only be used
    once.
    """
    @abstractmethod
    def add_load_item(self, w: LoadItem):
        pass

    @property
    @abstractmethod
    def issue_tracker(self) -> InputIssueTracker:
        pass


class Reader(Protocol):
    @abstractmethod
    def read(self, load_location: LoadLocation, orchestrator: LoadOrchestrator) -> BlockIterator:
        pass


# The ``Reader`` interface could in principle be replaced with a callable type, i.e.::
#     Reader = typing.Callable[[LoadLocation, LoadOrchestrator], BlockIterator]
# This approach was decided against because most readers turned out to need internal state
# which is easier to inspect with the explicit object form rather than a partially evaluated
# callable.
# Further, it is simple to adapt a callable to the object-model, see ``CallableReader``.


class CallableReader(typing.NamedTuple):
    """
    Adapter to wrap a callable reader for the ``Reader``-interface

    Example use::
        reader = CallableReader(my_reader_function)
    """

    read: typing.Callable[[LoadLocation, LoadOrchestrator], BlockIterator]


# class LoadProxy(Protocol):
#     @property
#     @abstractmethod
#     def reader(self) -> Reader:
#         pass


#     @property
#     @abstractmethod
#     def load_location(self) -> LoadLocation:
#         pass


#     @abstractmethod
#     def read(self, orchestrator: LoadOrchestrator) -> BlockIterator:
#         pass


class LoadProxy(typing.NamedTuple):
    load_location: LoadLocation
    reader: Reader

    def read(self, orchestrator: LoadOrchestrator) -> BlockIterator:
        yield from self.reader.read(self.load_location, orchestrator)


class Loader(Protocol):
    """
    The loader is responsible for resolving the load specification into a block iterator

    The resolution result is returned as a ``LoadProxy`` instance which holds the resolved
    ``LoadLocation`` together with a ``Reader`` instance compatible with the location.
    """

    @abstractmethod
    def resolve(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> LoadProxy:
        pass

    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        yield from self.resolve(load_item, orchestrator).read(orchestrator)
