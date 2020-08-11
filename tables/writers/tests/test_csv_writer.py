import io
from textwrap import dedent

import pandas as pd

from tables import write_csv, Table
from .._csv import _table_to_csv


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
        _table_to_csv(t, out)
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo
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
    t2.add_column("digit", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")

    # Write tables to stream
    with io.StringIO() as out:
        write_csv([t, t2], out)
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo
            all
            place;distance;ETA;is_hot
            text;km;datetime;onoff
            home;0.0;2020-08-04 08:00:00;1
            work;1.0;2020-08-04 09:00:00;0
            beach;2.0;2020-08-04 17:00:00;1
            wonderland;-;-;0

            **bar
            all
            digit;spelling
            -;text
            1;one
            6;six
            42;forty-two

            """
        )


def test_write_csv__writes_one_table():
    # Make a table
    t2 = Table(name="bar")
    t2.add_column("digit", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")

    # Check write_csv works when given a single table
    with io.StringIO() as out:
        write_csv(t2, out)
        assert out.getvalue() == dedent(
            """\
            **bar
            all
            digit;spelling
            -;text
            1;one
            6;six
            42;forty-two

            """
        )
