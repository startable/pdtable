from __future__ import annotations
from typing import Iterable
import re
from pathlib import Path

from pdtable.store import BlockIterator
from pdtable.table_origin import (
    NullInputIssueTracker,
    InputIssueTracker,
    InputError,
    LoadItem,
)
from ._protocol import (
    Loader,
)
from ._loaders import make_loader, FileReader


def queued_load(roots: list[LoadItem], loader: Loader, issue_tracker: InputIssueTracker = None):
    """
    Load `LoadItem`-objects listed in `roots`, together with any items added by the loader due
    to ``***include`` directives or similar

    This loader is single threaded. For higher performance, a multi-threaded loader should be used.
    """

    class Orchestrator:
        def __init__(self, roots, issue_tracker):
            self.load_items = roots
            self.issue_tracker = issue_tracker

        def add_load_item(self, item):
            self.load_items.append(item)

    orch = Orchestrator(
        roots, issue_tracker if issue_tracker is not None else NullInputIssueTracker()
    )
    while orch.load_items:
        yield from loader.load(orch.load_items.pop(), orch)

    if not orch.issue_tracker.is_ok:
        raise InputError(f"Load issues: {orch.issue_tracker}")


def load_files(
    files: Iterable[str],
    *,
    issue_tracker: None | InputIssueTracker = None,
    # below inputs are forwarded to make_reader -- only included for easy docs
    csv_sep: None | str = None,
    sheet_name_pattern: re.Pattern = None,
    file_reader: FileReader = None,
    root_folder: None | Path = None,
    file_name_pattern: re.Pattern = None,
    file_name_start_pattern: str = None,
    additional_protocol_loaders: dict[str, Loader] = None,
    allow_include: bool = True,
    **kwargs,
) -> BlockIterator:
    """
    Load a set of startable inputs

    Example: load all files matching `input_*`, `setup_*` in `input_folder`::

        load_files(['/'], root_folder=input_folder,
                   csv_sep=';', file_name_start_pattern="(input|setup)_")

    This function is a thin wrapper around the current best-practice loader
    and the backing implementation will be updated when best practice changes.

    `load_files` uses the ``FileSystemReader`` to resolve paths, which means that you must
    pass absolute filenames. See docs for ``FileSystemReader`` for details.

    Args:
        issue_tracker: Optional; Custom `InputIssuesTracker` instance to use.

    Any additional keyword arguments are forwarded to `make_loader` (see docs there).
    """
    loader = make_loader(
        csv_sep=csv_sep,
        sheet_name_pattern=sheet_name_pattern,
        file_reader=file_reader,
        root_folder=root_folder,
        file_name_pattern=file_name_pattern,
        file_name_start_pattern=file_name_start_pattern,
        additional_protocol_loaders=additional_protocol_loaders,
        allow_include=allow_include,
        **kwargs,
    )
    yield from queued_load(
        roots=[LoadItem(str(f), source=None) for f in files],
        loader=loader,
        issue_tracker=issue_tracker,
    )
