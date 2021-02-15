from textwrap import dedent

import pandas as pd
import numpy as np

import pytest

from .. import Table, frame
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
    return frame.make_table_dataframe(data_ab, name="foo", destinations={"bar", "baz"})


@pytest.fixture
def dft_m(data_ab):
    dft = frame.make_table_dataframe(data_ab, name="foo", destinations={"bar", "baz"})
    Column(dft, "cola").unit = "m"
    return dft


def test_make_pdtable(data_ab):
    df = frame.make_table_dataframe(data_ab, name="foo")

    assert "cola" in df.columns
    assert df.cola[2] == 2

    data = frame.get_table_info(df)

    assert data.metadata.name == "foo"
    assert data.columns["cola"].unit == "-"
    assert data.columns["colb"].unit == "text"


def test_is_pdtable(dft, data_ab):
    df = pd.DataFrame(data_ab)
    assert not frame.is_table_dataframe(df)
    assert frame.is_table_dataframe(dft)


def test_get_table_data(dft):
    assert frame.get_table_info(dft).metadata.name == "foo"

    bad_table = frame.TableDataFrame()
    with pytest.raises(Exception):
        frame.get_table_info(bad_table)
    assert frame.get_table_info(bad_table, fail_if_missing=False) is None


def test_column(dft):
    c = Column(dft, "cola")
    assert c.unit == frame.get_table_info(dft).columns["cola"].unit
    c.unit = "m"
    assert c.unit == "m"
    assert c.unit == frame.get_table_info(dft).columns["cola"].unit

    # pandas docs say that indirect assignment is flaky
    # c.values[2] = 7
    # assert dft.cola[2] == 7


def test_add_column(dft):
    frame.add_column(dft, "colc", [f"c{v}" for v in range(20, 24)], "text")
    assert dft.colc[0] == "c20"
    assert frame.get_table_info(dft).columns["colc"].unit == "text"


def test_table_init__doesnt_crash():
    Table(pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"])


def test_table(dft):
    t = Table(dft)

    assert t.name == "foo"
    assert t.destinations == {"baz", "bar"}
    assert frame.is_table_dataframe(t.df)

    assert t["cola"].unit == "-"
    t["cola"].unit = "km"
    assert frame.get_table_info(t.df).columns["cola"].unit == "km"

    t["colc"] = range(20, 24)
    assert "colc" in t.column_names
    assert t["colc"].unit == "-"


def test_table__str(dft):
    """String representation of a Table"""
    string_rep = str(Table(dft))
    lines = string_rep.split("\n")
    expected_lines = dedent("""\
        **foo
        baz bar
         cola [-] colb [text]
                0          v0
                1          v1
                2          v2
                3          v3""").split("\n")

    assert lines[0] == expected_lines[0]
    # destinations are stored as a set; order not necessarily preserved
    assert lines[1] in ["bar baz", "baz bar"]
    assert lines[2:] == expected_lines[2:]


def test_df_operations(data_ab, data_cd):
    t_ab = frame.make_table_dataframe(pd.DataFrame(data_ab), name="ab")
    t_cd = frame.make_table_dataframe(pd.DataFrame(data_cd), name="cd")

    _ = pd.concat([t_ab, t_cd], axis=0, sort=False)  # vertical concat
    r = pd.concat([t_ab, t_ab], axis=0, sort=False, ignore_index=True)  # vertical concat
    assert r.shape == (8, 2)

    t_ab2 = frame.make_table_dataframe(pd.DataFrame(data_ab), name="ab")
    Table(t_ab2)["cola"].unit = "m"

    with pytest.raises(frame.InvalidTableCombineError):
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


def test_drop_column(dft_m):
    # triggers method "reindex" in DataFrame.__finalize__ on pandas 1.1
    dft2 = dft_m.drop(columns=["colb"])
    assert dft2.shape[1] == 1

    t = Table(dft2)
    assert t["cola"].unit == "m"


def test_select_rows_by_value(dft_m):
    # triggers method "take" in DataFrame.__finalize__ on pandas 1.1
    dft2 = dft_m[dft_m.cola == 2]
    assert len(dft2) == 1

    t = Table(dft2)
    assert t["cola"].unit == "m"


def test_groupby(dft_m):
    gg = [(cola, g.loc[:, "colb"].to_numpy()) for cola, g in dft_m.groupby("cola")]
    assert gg[0][0] == 0

    # Causes warning on pandas 1.1, fix underway in https://github.com/pandas-dev/pandas/pull/37461
    gg = [
        (cola, list(g.loc[:, ("cola", "colb")].itertuples())) for cola, g in dft_m.groupby("cola")
    ]
    assert gg[0][0] == 0


def test_assign(dft_m):
    # triggers method "copy" in DataFrame.__finalize__ on pandas 1.1
    dft2 = dft_m.assign(col_new=3)
    t = Table(dft2)
    assert t["col_new"].unit == "-"
    assert t["cola"].unit == "m"
