import pytest
import logging

from pdtable.table_origin import NullInputIssueTracker, InputError


def test_null_input_issue_tracker_error():
    tracker = NullInputIssueTracker()
    with pytest.raises(InputError):
        tracker.add_error("This is bad")


def test_null_input_issue_tracker_warn(caplog):
    tracker = NullInputIssueTracker()
    tracker.add_warning("This is less bad")

    # entries are logging.LogRecord instances
    # https://docs.python.org/3/library/logging.html#logrecord-attributes
    warns = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert len(warns) == 1
    assert "This is less bad" in warns[0].msg
