import io
from textwrap import dedent
from typing import List
from pathlib import Path

from pytest import fixture, raises

from pdtable import read_csv, BlockType, Table


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
