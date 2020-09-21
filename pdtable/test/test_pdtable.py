import pandas as pd
import numpy as np

import pytest

from .. import Table, pandastable
from ..proxy import Column
from ..table_metadata import ColumnFormat


@pytest.fixture
def data_ab():
    return [{"cola": v, "colb": f"v{v}"} for v in range(4)]


@pytest.fixture
def data_cd():
    return [{"colc": v, "cold": f"d{v}"} for v in range(4)]


@pytest.fixture
def dft(data_ab):
    return pandastable.make_pdtable(data_ab, name="foo", destinations={"bar", "baz"})


def test_make_pdtable(data_ab):
    df = pandastable.make_pdtable(data_ab, name="foo")

    assert "cola" in df.columns
    assert df.cola[2] == 2

    data = pandastable.get_table_data(df)

    assert data.metadata.name == "foo"
    assert data.columns["cola"].unit == "-"
    assert data.columns["colb"].unit == "text"


def test_is_pdtable(dft, data_ab):
    df = pd.DataFrame(data_ab)
    assert not pandastable.is_pdtable(df)
    assert pandastable.is_pdtable(dft)


def test_get_table_data(dft):
    assert pandastable.get_table_data(dft).metadata.name == "foo"

    bad_table = pandastable.PandasTable()
    with pytest.raises(Exception):
        pandastable.get_table_data(bad_table)
    assert pandastable.get_table_data(bad_table, fail_if_missing=False) is None


def test_column(dft):
    c = Column(dft, "cola")
    assert c.unit == pandastable.get_table_data(dft).columns["cola"].unit
    c.unit = "m"
    assert c.unit == "m"
    assert c.unit == pandastable.get_table_data(dft).columns["cola"].unit

    # pandas docs say that indirect assignment is flaky
    # c.values[2] = 7
    # assert dft.cola[2] == 7


def test_add_column(dft):
    pandastable.add_column(dft, "colc", [f"c{v}" for v in range(20, 24)], "text")
    assert dft.colc[0] == "c20"
    assert pandastable.get_table_data(dft).columns["colc"].unit == "text"


def test_table_init():
    t2 = Table(pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"])


def test_table(dft):
    t = Table(dft)

    assert t.name == "foo"
    assert t.destinations == {"baz", "bar"}
    assert pandastable.is_pdtable(t.df)

    assert t["cola"].unit == "-"
    t["cola"].unit = "km"
    assert pandastable.get_table_data(t.df).columns["cola"].unit == "km"

    t["colc"] = range(20, 24)
    assert "colc" in t.column_names
    assert t["colc"].unit == "-"


def test_df_operations(data_ab, data_cd):
    t_ab = pandastable.make_pdtable(pd.DataFrame(data_ab), name="ab")
    t_cd = pandastable.make_pdtable(pd.DataFrame(data_cd), name="cd")

    _ = pd.concat([t_ab, t_cd], axis=0, sort=False)  # vertical concat
    r = pd.concat([t_ab, t_ab], axis=0, sort=False, ignore_index=True)  # vertical concat
    assert r.shape == (8, 2)

    t_ab2 = pandastable.make_pdtable(pd.DataFrame(data_ab), name="ab")
    Table(t_ab2)["cola"].unit = "m"

    with pytest.raises(pandastable.InvalidTableCombineError):
        # Fail on units for cola
        _ = pd.concat([t_ab, t_ab2])


def test_table_equals():
    t_ref = Table(
        pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"]
    )

    # True if...
    # identical
    assert t_ref.equals(
        Table(pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"])
    )
    # itself
    assert t_ref.equals(t_ref)
    # same numerical value but different data type (int vs. float)
    assert t_ref.equals(
        Table(
            pd.DataFrame({"c": [1, np.nan, 3], "d": [4.0, 5.0, 6.0]}),
            name="table2",
            units=["m", "kg"],
        )
    )

    # False if different...
    # name
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}), name="Esmeralda", units=["m", "kg"]
        )
    )
    # destination
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}),
            name="table2",
            units=["m", "kg"],
            destinations={"here", "there", "everywhere"},
        )
    )
    # unit
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}),
            name="table2",
            units=["football_fields", "kg"],
        )
    )
    # column name
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"level7_GHOUL": [1, np.nan, 3], "d": [4, 5, 6]}),
            name="table2",
            units=["m", "kg"],
        )
    )
    # data value
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"c": [666, np.nan, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"]
        )
    )
    # data value (ever so slightly)
    assert not t_ref.equals(
        Table(
            pd.DataFrame({"c": [1.00000000000001, np.nan, 3], "d": [4, 5, 6]}),
            name="table2",
            units=["m", "kg"],
        )
    )
    # thing entirely
    assert not t_ref.equals("a string")
    assert not t_ref.equals(42)
    assert not t_ref.equals(None)
    assert not t_ref.equals(pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}))


def test_column_format():
    assert ColumnFormat(2).specifier == ".2f"
    assert ColumnFormat("14.2e").specifier == "14.2e"

    assert str(ColumnFormat(2)) == ".2f"
    assert repr(ColumnFormat(2)) == "ColumnFormat: '.2f'"
