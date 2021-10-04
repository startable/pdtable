from __future__ import annotations
from abc import abstractmethod
import logging

from typing_extensions import Protocol  # Protocol is not available in python 3.7

from pdtable.store import BlockIterator
from pdtable.table_origin import (
    InputIssueTracker,
    LoadItem,
)

logger = logging.getLogger(__name__)


class LoadError(Exception):
    pass


class LoadOrchestrator(Protocol):
    @abstractmethod
    def add_load_item(self, w: LoadItem):
        pass

    @property
    @abstractmethod
    def issue_tracker(self) -> InputIssueTracker:
        pass


# TODO: The Loader-protocol does not abstract out the mapping from `LoadItem` to `LoadLocation`
#       This makes desirable features such as loop detection and generic caching problematic, as
#       they would ideally be keyed on the ``load_identifier`` member of the ``LoadLocation``.
class Loader(Protocol):
    @abstractmethod
    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        pass
