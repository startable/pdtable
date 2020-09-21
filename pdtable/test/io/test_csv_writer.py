import io
from textwrap import dedent

import pandas as pd

import pdtable
from pdtable import Table
from pdtable.io.csv import write_csv, _table_to_csv
from pdtable.table_metadata import ColumnFormat


def test__table_to_csv():
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

    # Write table to stream
    with io.StringIO() as out:
        _table_to_csv(t, out, ";", "-")
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo;
            all
            place;distance;ETA;is_hot
            text;km;datetime;onoff
            home;0.0;2020-08-04 08:00:00;1
            work;1.0;2020-08-04 09:00:00;0
            beach;2.0;2020-08-04 17:00:00;1
            wonderland;-;-;0

            """
        )


def test_write_csv__writes_two_tables():
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
    t2.add_column("number", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")

    # Write tables to stream
    with io.StringIO() as out:
        write_csv([t, t2], out)
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo;
            all
            place;distance;ETA;is_hot
            text;km;datetime;onoff
            home;0.0;2020-08-04 08:00:00;1
            work;1.0;2020-08-04 09:00:00;0
            beach;2.0;2020-08-04 17:00:00;1
            wonderland;-;-;0

            **bar;
            all
            number;spelling
            -;text
            1;one
            6;six
            42;forty-two

            """
        )


def test_write_csv__leaves_stream_open_if_caller_passes_stream():
    # Make a table
    t2 = Table(
        pd.DataFrame({"number": [1, 6, 42], "spelling": ["one", "six", "forty-two"]}),
        name="bar",
        units=["-", "text"],
    )

    # Check write_csv single table and leave stream open for business
    with io.StringIO() as out:
        write_csv(t2, out)
        out.write("Fin\n")
        assert out.getvalue() == dedent(
            """\
            **bar;
            all
            number;spelling
            -;text
            1;one
            6;six
            42;forty-two

            Fin
            """
        )


def test_write_csv__writes_to_file(tmp_path):
    # Make a table
    t2 = Table(
        pd.DataFrame({"number": [1, 6, 42], "spelling": ["one", "six", "forty-two"]}),
        name="bar",
        units=["-", "text"],
    )

    # Write to file
    out_path = tmp_path / "write_csv_to_file.csv"
    write_csv(t2, out_path)

    # Now check file contents
    assert out_path.read_text() == dedent(
        """\
        **bar;
        all
        number;spelling
        -;text
        1;one
        6;six
        42;forty-two

        """
    )
    # Teardown
    out_path.unlink()


def test__write_csv__uses_altered_default_csv_separator(monkeypatch):

    # Change the default CSV separator
    # Using monkeypatch in lieu of just 'tables.CSV_SEP = ","'
    # so that this alteration of the default is only visible in this test.
    monkeypatch.setattr(pdtable, "CSV_SEP", ",")

    # Make a table with content of various units
    t = Table(
        pd.DataFrame(
            {
                "place": ["home", "work", "beach", "wonderland"],
                "distance": list(range(3)) + [float("nan")],
                "ETA": pd.to_datetime(
                    ["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]
                ),
                "is_hot": [True, False, True, False],
            }
        ),
        name="foo",
        units=["text", "km", "datetime", "onoff"],
    )

    # Write table to stream
    with io.StringIO() as out:
        write_csv(t, out)
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo,
            all
            place,distance,ETA,is_hot
            text,km,datetime,onoff
            home,0.0,2020-08-04 08:00:00,1
            work,1.0,2020-08-04 09:00:00,0
            beach,2.0,2020-08-04 17:00:00,1
            wonderland,-,-,0

            """
        )


def test_write_csv__with_format_specs():
    # Make a table
    t2 = Table(
        pd.DataFrame(
            {"numbers": [1, 6, 42], "same_numbers": [1, 6, 42], "others": [1, 123.456, 42]}
        ),
        name="bar",
        units=["-", "-", "-"],
    )

    # Give formats to some columns, leave some without formats
    t2.column_metadata["same_numbers"].display_format = ColumnFormat(2)
    t2.column_metadata["others"].display_format = ColumnFormat("14.3e")

    with io.StringIO() as out:
        write_csv(t2, out)
        assert out.getvalue() == dedent(
            """\
            **bar;
            all
            numbers;same_numbers;others
            -;-;-
            1;1.00;     1.000e+00
            6;6.00;     1.235e+02
            42;42.00;     4.200e+01
            
            """
        )
