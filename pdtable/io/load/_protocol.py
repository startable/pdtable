from __future__ import annotations
from abc import abstractmethod
import logging

# Protocol is not available in python 3.7
from typing_extensions import Protocol

# from pdtable.io.csv import read_csv
# from pdtable.io.excel import read_excel
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


class Loader(Protocol):
    @abstractmethod
    def load(self, load_item: LoadItem, orchestrator: LoadOrchestrator) -> BlockIterator:
        pass
