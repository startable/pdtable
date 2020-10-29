import io
from pathlib import Path
from textwrap import dedent

from pytest import fixture

from pdtable import Table, TableDataFrame
from pdtable import TableBundle, read_csv, read_excel
from pdtable.demo.unit_converter import convert_this
from pdtable.utils import read_bundle_from_csv


def input_dir() -> Path:
    return Path(__file__).parent / "input"


@fixture
def csv_data():
    # fmt off
    return dedent(
        """\
        **farm_types1;;;
        your_farm my_farm farms_galore;;;
        species;  num;  flt;    log;
        text;       -;   kg;  onoff;
        chicken;    2;    3;      1;
        pig;        4;   39;      0;
        goat;       4;    -;      1;
        zybra;      4;    -;      0;
        cow;      NaN;  200;      1;
        goose;      2;    9;      0;

        **unrelated_table;;;
        your_farm my_farm farms_galore;;;
        species;  num;  flt;    log;
        text;       -;   kg;  onoff;
        unicorn;    2;    3;      1;
        """
    )
    # fmt on


def test_read_bundle_from_csv(csv_data):
    bundle = read_bundle_from_csv(io.StringIO(csv_data))
    # Correct number of tables read
    assert len(bundle) == 2
    # Correct values read
    assert bundle["farm_types1"]["flt"].unit == "kg"
    assert bundle["farm_types1"]["flt"].values[4] == 200
    assert bundle["unrelated_table"]["flt"].unit == "kg"
    assert bundle["unrelated_table"]["flt"].values[0] == 3


def test_read_bundle_from_csv__converts_units(csv_data):
    unit_dispatcher = {"farm_types1": {"flt": "g"}}
    bundle = read_bundle_from_csv(
        io.StringIO(csv_data), convert_units_to=unit_dispatcher, unit_converter=convert_this
    )
    # Units converted where dispatched
    assert bundle["farm_types1"]["flt"].unit == "g"
    assert bundle["farm_types1"]["flt"].values[4] == 200000
    # Units not converted where not dispatched
    assert bundle["unrelated_table"]["flt"].unit == "kg"
    assert bundle["unrelated_table"]["flt"].values[0] == 3


def test_TableBundle_from_file():
    """ Verify that TableBundle can be generated from top level API methods: read_csv, read_excel
    """
    input_file = input_dir() / "bundle.csv"
    bundle = TableBundle(read_csv(input_file), as_dataframe=True)
    assert bundle is not None
    assert len(bundle) == 3
    assert isinstance(bundle[0], TableDataFrame)

    assert bundle.unique("spelling_numbers").spelling[1] == "six"
    assert bundle[1].spelling[0] == "one"
    assert len(bundle.all("places_to_go")) == 2

    bundle = TableBundle(read_csv(input_file), as_dataframe=False)
    assert bundle is not None
    assert len(bundle) == 3
    assert isinstance(bundle[1], Table)
    assert bundle.spelling_numbers["spelling"].values[0] == "one"
    assert len(bundle.all("places_to_go")) == 2

    input_file = input_dir() / "bundle.xlsx"
    bundle = TableBundle(read_excel(input_file), as_dataframe=False)
    assert bundle is not None
    assert len(bundle) == 3
    assert isinstance(bundle[1], Table)
    assert bundle.spelling_numbers["spelling"].values[0] == "one"
    assert len(bundle.all("places_to_go")) == 2

    bundle = TableBundle(read_excel(input_file), as_dataframe=True)
    assert bundle is not None
    assert len(bundle) == 3
    assert isinstance(bundle[0], TableDataFrame)

    assert bundle.unique("spelling_numbers").spelling[1] == "six"
    assert bundle[1].spelling[0] == "one"
    assert len(bundle.all("places_to_go")) == 2
