import datetime

import pandas as pd
import openpyxl

try:
    from openpyxl.worksheet.worksheet import Worksheet as OpenpyxlWorksheet
except ImportError:
    # openpyxl < 2.6
    from openpyxl.worksheet import Worksheet as OpenpyxlWorksheet  # noqa: F401

from pdtable import Table
from pdtable.io.excel import write_excel
from pdtable.io._excel_openpyxl import _append_table_to_openpyxl_worksheet


def test__append_table_to_openpyxl_worksheet():
    # Make a table with content of various units
    t = Table(name="foo")
    t["place"] = ["home", "work", "beach", "wonderland"]
    t.add_column("distance", list(range(3)) + [float("nan")], "km")
    t.add_column(
        "ETA",
        pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]),
        "datetime",
    )
    t.add_column("is_hot", [True, False, True, False], "onoff")

    wb = openpyxl.Workbook()
    ws = wb.active

    # Act
    _append_table_to_openpyxl_worksheet(t, ws)

    # Assert worksheet looks as expected:
    # table header by row
    assert ws["A1"].value == "**foo"
    assert ws["A2"].value == "all"
    assert [ws.cell(3, c).value for c in range(1, 5)] == ["place", "distance", "ETA", "is_hot"]
    assert [ws.cell(4, c).value for c in range(1, 5)] == ["text", "km", "datetime", "onoff"]

    # table data by column
    assert [ws.cell(r, 1).value for r in range(5, 9)] == ["home", "work", "beach", "wonderland"]
    assert [ws.cell(r, 2).value for r in range(5, 9)] == [0, 1, 2, "-"]
    assert [ws.cell(r, 3).value for r in range(5, 9)] == list(
        pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00"])
    ) + ["-"]
    assert [ws.cell(r, 4).value for r in range(5, 9)] == [1, 0, 1, 0]


def test_write_excel(tmp_path):
    # Make a couple of tables
    t = Table(name="foo")
    t["place"] = ["home", "work", "beach", "wonderland"]
    t.add_column("distance", list(range(3)) + [float("nan")], "km")
    t.add_column(
        "ETA",
        pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]),
        "datetime",
    )
    t.add_column("is_hot", [True, False, True, False], "onoff")

    # This one is transposed
    t2 = Table(name="bar")
    t2.add_column("digit", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")
    t2.metadata.transposed = True

    # Write tables to workbook, save, and re-load
    out_path = tmp_path / "foo.xlsx"
    write_excel([t, t2], out_path)
    wb = openpyxl.load_workbook(out_path)
    ws = wb.active

    # First table is written as expected
    # - table header by row
    assert ws["A1"].value == "**foo"
    assert ws["A2"].value == "all"
    assert [ws.cell(3, c).value for c in range(1, 5)] == ["place", "distance", "ETA", "is_hot"]
    assert [ws.cell(4, c).value for c in range(1, 5)] == ["text", "km", "datetime", "onoff"]

    # - table data by column
    assert [ws.cell(r, 1).value for r in range(5, 9)] == ["home", "work", "beach", "wonderland"]
    assert [ws.cell(r, 2).value for r in range(5, 9)] == [0, 1, 2, "-"]
    for r, d in zip(
        range(5, 8), pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00"])
    ):
        # workaround openpyxl bug: https://foss.heptapod.net/openpyxl/openpyxl/-/issues/1493
        # openpyxl adds a spurious microsecond to some datetimes.
        assert abs(ws.cell(r, 3).value - d) <= datetime.timedelta(microseconds=1)
    assert ws.cell(8, 3).value == "-"
    assert [ws.cell(r, 4).value for r in range(5, 9)] == [1, 0, 1, 0]

    # Second table is there as well
    assert ws["A10"].value == "**bar*"
    assert ws["A11"].value == "all"
    # column headers (transposed)
    assert [ws.cell(r, 1).value for r in range(12, 14)] == ["digit", "spelling"]
    assert [ws.cell(r, 2).value for r in range(12, 14)] == ["-", "text"]
    # column values (transposed)
    assert [ws.cell(12, c).value for c in range(3, 6)] == [1, 6, 42]
    assert [ws.cell(13, c).value for c in range(3, 6)] == ["one", "six", "forty-two"]

    # Teardown
    out_path.unlink()


def test_write_excel__multiple_sheets(tmp_path):
    """write_excel() can write tables to multiple sheets in a workbook"""

    # Make a couple of tables
    t = Table(name="foo")
    t["place"] = ["home", "work", "beach", "wonderland"]
    t.add_column("distance", list(range(3)) + [float("nan")], "km")
    t.add_column(
        "ETA",
        pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]),
        "datetime",
    )
    t.add_column("is_hot", [True, False, True, False], "onoff")

    t2 = Table(name="bar")
    t2.add_column("digit", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")

    # Write tables to workbook, save, and re-load
    out_path = tmp_path / "foo.xlsx"
    write_excel({"sheet_one": [t, t2], "sheet_two": t2}, out_path)
    wb = openpyxl.load_workbook(out_path)

    # Workbook has the expected sheets
    assert len(wb.worksheets) == 2
    assert wb.sheetnames == ["sheet_one", "sheet_two"]
    # First sheet contains the expected tables
    assert wb.worksheets[0]["A1"].value == "**foo"
    assert wb.worksheets[0]["A10"].value == "**bar"
    # Second sheet contains the expected tables
    assert wb.worksheets[1]["A1"].value == "**bar"
    # Table details are tested elsewhere.

    # Teardown
    out_path.unlink()
