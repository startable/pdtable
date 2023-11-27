import sys
from textwrap import dedent
import warnings

import pandas as pd
import numpy as np

import pytest

from .. import Table, frame
from ..proxy import Column
from ..table_metadata import ColumnFormat, ColumnUnitException
from .conftest import HAS_PYARROW


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


def test_make_table_dataframe__with_wrong_dtype_raises(data_ab):
    with pytest.raises(Exception) as ex:
        frame.make_table_dataframe(
            pd.DataFrame({
                'a': [1, 2, 3],
                'b': ["a", "b", "c"]
            }),
            name='test', destinations='abc', units=["-", "-"]
        )
        assert ex.value.args[0].startswith("Special column unit")


def test_make_table_dataframe__with_no_units__creates_units():
    table = frame.make_table_dataframe(
        pd.DataFrame({
            'a': [1, 2, 3],
            'b': ["a", "b", "c"]
        }),
        name='test', destinations='abc'
    )
    assert frame.get_table_info(table).columns["a"].unit == "-"
    assert frame.get_table_info(table).columns["b"].unit == "text"


@pytest.mark.skipif(not HAS_PYARROW, reason="No pyarrow")
def test_make_table_dataframe__with_no_units__creates_units_pyarrow():
    df = pd.DataFrame({
        'a': pd.Series([1, 2, 3], dtype='uint64[pyarrow]'),
        'b': pd.Series(["a", "b", "c"], dtype='string[pyarrow]')
    })
    table = frame.make_table_dataframe(df, name='test', destinations='abc')
    assert frame.get_table_info(table).columns["a"].unit == "-"
    assert frame.get_table_info(table).columns["b"].unit == "text"


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


def test_make_table_dataframe_units(data_ab):
    tdf = frame.make_table_dataframe(
        pd.DataFrame(data_ab),
        name="ab",
        units=["m", "text"],
    )
    assert frame.get_table_info(tdf).columns["cola"].unit == "m"

    tdf = frame.make_table_dataframe(
        pd.DataFrame(data_ab),
        name="ab",
        unit_map={"cola": "m"},
    )
    assert frame.get_table_info(tdf).columns["cola"].unit == "m"


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


def test_table_from_other_table_dataframe_with_different_metadata():
    t_ref = Table(
        pd.DataFrame({"c": [1, np.nan, 3], "d": [4, 5, 6]}), name="table1", units=["m", "kg"],
    )

    t2 = Table(t_ref.df, name="table2", units=["g", "mm"])

    assert t_ref.name == "table1"
    assert t_ref.units == ["m", "kg"]
    assert t2.name == "table2"
    assert t2.units == ["g", "mm"]


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

    for _, group in dft_m.groupby("cola"):
        group_t = Table(group)
        assert group_t["cola"].unit == "m"
        assert group_t["colb"].unit == "text"


def test_assign(dft_m):
    # triggers method "copy" in DataFrame.__finalize__ on pandas 1.1
    dft2 = dft_m.assign(col_new=3)
    t = Table(dft2)
    assert t["col_new"].unit == "-"
    assert t["cola"].unit == "m"


def test_table__with_no_destinations_has_correct_default():
    table = Table(name="name")
    assert table.destinations == {"all"}


def test_table__handles_destinations_of_type_set():
    table = Table(name="test", destinations={"a", "b", "c"})
    assert table.destinations == {"a", "b", "c"}


def test_table__str_destination_with_no_spaces_results_in_single_destination():
    table = Table(name="test", destinations="abc")
    assert table.destinations == {"abc"}


def test_table__str_destination_with_spaces_results_in_multiple_destinations():
    table = Table(name="test", destinations="a b c")
    assert table.destinations == {"a", "b", "c"}


def test_unit_map_with_different_order_than_columns(tmpdir):
    data_frame = pd.DataFrame.from_dict({
        'column_text': ['a', 'b', 'c'],
        'column_deg': [1, 2, 3]
    })
    table = frame.make_table_dataframe(
        df=data_frame,
        name='test_unit_map',
        unit_map={
            'column_deg': 'deg',
            'column_text': 'text'
        },
    )
    assert {'column_text': 'text', 'column_deg': 'deg'} == \
        dict(zip(table.columns, frame.get_table_info(df=table).units))


@pytest.fixture
def table_data_frame() -> frame.TableDataFrame:
    data_frame = pd.DataFrame.from_dict({
        'A': ['b', 'c', 'a', 'd', 'e'],
        'B': [1, 2, 3, 4, 5],
        'C': [True, False, True, False, True]
    })
    return frame.make_table_dataframe(
        data_frame,
        name='test',
        destinations='abc',
        unit_map={
            'A': 'text',
            'B': 'kg',
            'C': 'onoff'
        }
    )


class TestFinalize:
    def test_replace_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        with warnings.catch_warnings(record=True) as w:
            table_data_frame.replace('a', 'z')
            assert len(w) == 0

    def test_replace_not_allowed_unit(self, table_data_frame: frame.TableDataFrame) -> None:
        with pytest.raises(ColumnUnitException):
            table_data_frame.replace(True, 'a')

    def test_sort_index_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        table_data_frame.set_index('A', inplace=True)

        with warnings.catch_warnings(record=True) as w:
            table_data_frame.sort_index()
            assert len(w) == 0

    def test_transpose_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        """
        Columns metadata should be empty after transpose, 
        since after transposing, we end up with completely new set of columns.
        """
        transposed = table_data_frame.transpose()
        table_data = object.__getattribute__(transposed, frame._TABLE_INFO_FIELD_NAME)
        assert ['text'] * len(table_data_frame.index) == table_data.units

    def test_astype_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        assert isinstance(table_data_frame['B'].iloc[0], np.int64)
        
        with warnings.catch_warnings(record=True) as w:
            table_data_frame_new_type = table_data_frame.astype({'B': float})
            assert len(w) == 0

        assert isinstance(table_data_frame_new_type['B'].iloc[0], np.float64)

    @pytest.mark.skipif(
        sys.version_info < (3, 8),
        reason="test passes only with python 3.8 or newer"
    )
    def test_astype_not_allowed_type(self, table_data_frame: frame.TableDataFrame) -> None:
        with pytest.raises(ColumnUnitException):
            table_data_frame.astype({'B': str})
            
    def test_append_with_loc_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        """
        append is executed under the hood while adding a row using loc method.
        """
        with warnings.catch_warnings(record=True) as w:
            table_data_frame.loc[999] = {'A': 'y', 'B': 1, 'C': True}
            assert len(w) == 0
        
        assert 6 == table_data_frame.shape[0]

    def test_append_with_loc_not_allowed_type(self, table_data_frame: frame.TableDataFrame) -> None:
        with pytest.raises(ColumnUnitException):
            table_data_frame.loc[999] = {'A': 'y', 'B': 1, 'C': 'no'}

    def test_fillna_ok(self, table_data_frame: frame.TableDataFrame) -> None:
        table_data_frame_new_type = table_data_frame.astype({'B': float})
        table_data_frame_new_type.iloc[0, 1] = np.nan
        
        with warnings.catch_warnings(record=True) as w:
            table_data_frame_new_type.fillna(123)
            assert len(w) == 0

    @pytest.mark.skipif(
        sys.version_info < (3, 8),
        reason="test passes only with python 3.8 or newer"
    )
    def test_fillna_not_allowed_type(self, table_data_frame: frame.TableDataFrame) -> None:
        table_data_frame_new_type = table_data_frame.astype({'B': float})
        table_data_frame_new_type.iloc[0, 1] = np.nan
        
        with pytest.raises(ColumnUnitException):
            table_data_frame_new_type.fillna('123')
            
    def test_rename_columns(self, table_data_frame: frame.TableDataFrame) -> None:
        """
        Renaming columns is not suported. It can mess with current units settings.
        """
        with pytest.raises(ColumnUnitException):
            table_data_frame.rename(columns={"A": "B", "B": "C", "C": "A"})

    def test_rename_index(self, table_data_frame: frame.TableDataFrame) -> None:
        with warnings.catch_warnings(record=True) as w:
            table_data_frame.rename(index={1: 'a', 2: 'b'})
            assert len(w) == 0

    def test_unstack(self, table_data_frame: frame.TableDataFrame) -> None:
        """
        Test check how units of a table data frame change after unstacking.
        """
        multi_index = pd.MultiIndex.from_tuples([
            ("a","1"), 
            ("a","2"),
            ("b","1"),
            ("c","1"),
            ("c","2")
        ])
        table_data_frame.set_index(multi_index, inplace=True)
        
        with warnings.catch_warnings(record=True) as w:
            unstacked_table_data_frame = table_data_frame.unstack()
            assert len(w) == 0

        unstacked_col_name_to_unit = {
            name: col.unit for name, col in object.__getattribute__(
                unstacked_table_data_frame,
                frame._TABLE_INFO_FIELD_NAME
            ).columns.items()
        }
        assert {
            ('A', '1'): 'text',
            ('A', '2'): 'text',
            ('B', '1'): '-',
            ('B', '2'): '-',
            ('C', '1'): 'text',
            ('C', '2'): 'text'
        } == unstacked_col_name_to_unit

    @pytest.mark.skipif(
        sys.version_info < (3, 8),
        reason="test passes only with python 3.8 or newer"
    )
    def test_melt(self, table_data_frame: frame.TableDataFrame) -> None:
        """
        Test check how units of a table data frame change after unstacking.
        """
        with warnings.catch_warnings(record=True) as w:
            melted_table_data_frame = table_data_frame.melt(id_vars=['A'], value_vars=['B', 'C'])
            assert len(w) == 0
        
        melted_col_name_to_unit = {
            name: col.unit for name, col in object.__getattribute__(
                melted_table_data_frame,
                frame._TABLE_INFO_FIELD_NAME
            ).columns.items()
        }
        assert {
            'A': 'text',
            'variable': 'text',
            'value': 'text'
        } == melted_col_name_to_unit
