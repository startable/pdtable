import io
from textwrap import dedent
from typing import List
from pathlib import Path

from pytest import fixture, raises
import pandas as pd

import pdtable
from pdtable import Table, BlockType, read_csv, write_csv
from pdtable.io.csv import _table_to_csv
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


def test__table_to_csv__writes_transposed_table():
    # Make a TRANSPOSED table with content of various units
    t = Table(name="foo")
    t["place"] = ["home", "work", "beach", "wonderland"]
    t.add_column("distance", list(range(3)) + [float("nan")], "km")
    t.add_column(
        "ETA",
        pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]),
        "datetime",
    )
    t.add_column("is_hot", [True, False, True, False], "onoff")
    t.metadata.transposed = True  # <<<< aha

    # Write transposed table to stream
    with io.StringIO() as out:
        _table_to_csv(t, out, ";", "-")
        # Stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **foo*;
            all
            place;text;home;work;beach;wonderland
            distance;km;0.0;1.0;2.0;-
            ETA;datetime;2020-08-04 08:00:00;2020-08-04 09:00:00;2020-08-04 17:00:00;-
            is_hot;onoff;1;0;1;0

            """
        )


def test__table_to_csv__writes_empty_table():
    # Make a table with content of various units
    t = Table(name="empty")

    # Write table to stream
    with io.StringIO() as out:
        _table_to_csv(t, out, ";", "-")
        # Assert stream content is as expected
        assert out.getvalue() == dedent(
            """\
            **empty;
            all




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

    # This table is transposed
    t2 = Table(name="bar")
    t2.add_column("number", [1, 6, 42], "-")
    t2.add_column("spelling", ["one", "six", "forty-two"], "text")
    t2.metadata.transposed = True

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

            **bar*;
            all
            number;-;1;6;42
            spelling;text;one;six;forty-two

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


def test_write_csv__empty_tables():
    t1 = Table(name="foo")
    t2 = Table(name="bar")
    t2.metadata.transposed = True

    with io.StringIO() as out:
        write_csv([t1, t2], out)
        assert out.getvalue() == dedent(
            """\
            **foo;
            all
            
            
            
            
            **bar*;
            all
            
            
            """
        )


@fixture
def csv_data() -> str:
    return dedent(
        """\
        author: ;XYODA     ;
        purpose:;Save the galaxy;

        ***gunk
        grok
        jiggyjag

        **places;
        all
        place;distance;ETA;is_hot;
        text;km;datetime;onoff
        home;0.0;2020-08-04 08:00:00;1
        work;1.0;2020-08-04 09:00:00;0
        beach;2.0;2020-08-04 17:00:00;1

        ::;details about various places;

        **farm_animals
        your_farm my_farm other_farm;;
        species;n_legs;avg_weight;
        text;-;kg;
        chicken;2;2;
        pig;4;89;
        cow;4;200;
        unicorn;4;NaN;

        **this_one_is_transposed*;
        all;
        diameter; cm; 1.23;
        melting_point; K; 273;
        """
    )


def test_read_csv(csv_data):
    bl = list(read_csv(io.StringIO(csv_data)))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    template_rows = [b for t, b in bl if t == BlockType.TEMPLATE_ROW]
    met = [b for t, b in bl if t == BlockType.METADATA]

    assert len(met) == 1
    assert len(tables) == 3

    # Correctly reads non-transposed table
    assert tables[0].df["place"][1] == "work"
    assert not tables[0].metadata.transposed

    # Correctly reads transposed table
    t2: Table = tables[2]
    assert t2.column_names == ["diameter", "melting_point"]
    assert len(t2.df) == 1
    assert t2.df["melting_point"][0] == 273
    assert len(template_rows) == 1
    assert t2.metadata.transposed


def test_read_csv__sep_is_comma(csv_data):
    bl = list(read_csv(io.StringIO(csv_data.replace(";", ",")), sep=","))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    template_rows = [b for t, b in bl if t == BlockType.TEMPLATE_ROW]
    met = [b for t, b in bl if t == BlockType.METADATA]

    assert len(met) == 1
    assert len(tables) == 3
    assert tables[0].df["place"][1] == "work"
    t2: Table = tables[2]
    assert t2.column_names == ["diameter", "melting_point"]
    assert t2.df["melting_point"][0] == 273
    assert len(template_rows) == 1


def test_read_csv__from_stream():
    with open(Path(__file__).parent / "input" / "bundle.csv", "r") as fh:
        bls = list(read_csv(fh))
        tables = [bl for ty, bl in bls if ty == BlockType.TABLE]
        assert tables[1].name == "spelling_numbers"

    # raises exception on common error if not text stream
    with raises(Exception):
        with open(Path(__file__).parent / "input" / "bundle.csv", "rb") as fh:  # binary stream!
            bls = list(read_csv(fh))
            tables = [bl for ty, bl in bls if ty == BlockType.TABLE]


def test_read_csv__reads_transposed_tables_with_arbitrary_trailing_csv_delimiters():
    csv_data_transposed_tables = dedent(
        """\
        **transposed*
        all
        diameter; cm; 1.23
        melting_point; K; 273

        **transposed*;
        all;
        diameter; cm; 1.23;;;;;;;
        melting_point; K; 273;

        **transposed*;
        all;
        diameter; cm; 1.23
        melting_point; K; 273;;;;;;;;;;;;;;;;;;;

        **transposed*;;;;;;
        all;;;;;;
        diameter; cm; 1.23;;;;
        melting_point; K; 273;;;;
        """
    )
    bl = list(read_csv(io.StringIO(csv_data_transposed_tables)))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    t0: Table = tables[0]
    assert t0.column_names == ["diameter", "melting_point"]
    assert len(t0.df) == 1
    for t in tables:
        assert t.equals(t0)


def test_read_csv__successfully_ignores_comments_on_column_name_row():
    csv_data_transposed_tables = dedent(
        """\
        **places;
        all
        place;distance;ETA;is_hot;;;; --> this is a perfectly legal comment <-- ;
        text;km;datetime;onoff
        home;0.0;2020-08-04 08:00:00;1
        work;1.0;2020-08-04 09:00:00;0
        beach;2.0;2020-08-04 17:00:00;1
        """
    )
    bl = list(read_csv(io.StringIO(csv_data_transposed_tables)))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    t0: Table = tables[0]
    assert t0.column_names == ["place", "distance", "ETA", "is_hot"]


def test__table_is_preserved_when_written_to_and_read_from_csv():
    table_write = Table(
            pd.DataFrame({
                "a": [1, 2, 3],
                "b": ["a", "b", "c"]
            }),
            name="test",
            destinations="a b c")

    with io.StringIO() as s:
        write_csv(tables=table_write, to=s)
        s.seek(0)
        block = next(read_csv(s))
    table_read = block[1]

    assert table_read.equals(table_write)
    assert table_read.name == table_write.name
    assert table_read.column_names == table_write.column_names
    assert table_read.units == table_write.units
    assert table_read.destinations == table_write.destinations
