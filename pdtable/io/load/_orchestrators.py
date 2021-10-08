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

    This loader does not check for include-loops.
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
    visited: set[str] = set()
    while orch.load_items:
        load_proxy = loader.resolve(orch.load_items.pop(), orch)
        # check for loops/duplicates
        load_identifier = load_proxy.load_location.load_identifier
        if load_identifier in visited:
            orch.issue_tracker.add_error(
                "Load location included multiple times (this may be due to an include loop)",
                load_location=load_proxy.load_location)
            continue
        visited.add(load_identifier)

        yield from load_proxy.read(orch)


def load_files(
    roots: Iterable[str | Path] = None,
    *,
    issue_tracker: None | InputIssueTracker = None,
    # below inputs are forwarded to make_reader -- only included for easy docs
    csv_sep: None | str = None,
    sheet_name_pattern: re.Pattern = None,
    file_reader: FileReader = None,
    root_folder: None | str | Path = None,
    file_name_pattern: re.Pattern = None,
    file_name_start_pattern: str = None,
    additional_protocol_loaders: dict[str, Loader] = None,
    allow_include: bool = True,
    **kwargs,
) -> BlockIterator:
    """
    Load a complete startable inputset

    Example: load all files matching `input_*`, `setup_*` in folder `foo`::

        load_files(root_folder="foo",
                   csv_sep=';', file_name_start_pattern="^(input|setup)_")

    This function is a thin wrapper around the current best-practice loader
    and the backing implementation will be updated when best practice changes.

    `load_files` uses the ``FileSystemReader`` to resolve paths, which means that you must
    pass absolute filenames. See docs for ``FileSystemReader`` for details.

    Args:
        roots: The root load items.
            If ``root_folder`` is specified, contents must be valid root load specifiers
            which cannot be relative file names. Default value is ``["/"]``, indicating
            that the root folder is the only root load item.
            If ``root_folder`` is not specified, file-protocol roots must be provided as
            absolute paths.
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
    if roots is None and root_folder is not None:
        roots = ["/"]
    yield from queued_load(
        roots=[LoadItem(str(f), source=None) for f in roots],
        loader=loader,
        issue_tracker=issue_tracker,
    )
