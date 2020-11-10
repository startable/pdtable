"""Test behaviour related to optional dependencies.

This module must be located completely outside the pdtable package. Otherwise it is difficult
to control what gets imported when and in which order, since pdtable/__init__.py gets executed
before the contents of everything else in the pdtable/ dir, including the tests in pdtable/test.
"""
import sys
from unittest import mock

from pytest import raises


def test_openpyxl_absent(tmp_path):
    """Test behaviour when openpyxl is not installed

    pdtable should be fully functional, except for Excel I/O
    """
    # Mock openpyxl not being installed
    with mock.patch.dict(sys.modules, {'openpyxl': None}):
        # Can import pdtable
        import pdtable

        with raises(ImportError):
            # Fails on first use of read_excel
            # list() is used to trigger the otherwise lazy generator; else doesn't even import.
            list(pdtable.read_excel(""))

        with raises(ImportError):
            pdtable.write_excel([], tmp_path / "foo.xlsx")


def test_pint_absent():
    """Test behaviour when pint is not installed

    pdtable should be fully functional, except the courtesy pint converter
    """
    with mock.patch.dict(sys.modules, {'pint': None}):
        # Can import
        import pdtable
        with raises(ImportError):
            # Fails on first use of pint_converter
            pdtable.pint_converter(1, "m", "mm")
