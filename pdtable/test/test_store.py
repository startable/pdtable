import pytest

from pdtable import TableBundle, Table, TableDataFrame
from pdtable.io.parsers.blocks import parse_blocks

cell_rows = [
    # fmt off
    ["**foo"],
    ["all"],
    ["column"],
    ["text"],
    ["bar"],
    ["zoo"],
    [],
    ["::", "Table foo describes"],
    [None, "the fooness of things"],
    [":.column", "Column is a column in foo"],
    [],
    ["**infs"],
    ["all"],
    ["file_bytes", "file_date", "has_table"],
    ["-", "text", "onoff"],
    [15373, "a", 0],
    [15326, "b", 1],
    []
    # fmt on
]


def test_bundle_from_csv():

    bundle = TableBundle(parse_blocks(cell_rows), as_dataframe=True)

    assert bundle.foo.column.values[0] == "bar"


def test_TableBundle_as_dataframe():
    """ Verify that as_dataframe is functioning as expected (switch TableType)
    """

    # pdtable generator
    bundle = TableBundle(parse_blocks(cell_rows, to="pdtable"), as_dataframe=True)
    assert bundle.infs.file_bytes.values[1] == 15326.0
    assert bundle is not None
    assert len(bundle) == 2
    assert isinstance(bundle[0], TableDataFrame)

    # pdtable generator
    bundle = TableBundle(parse_blocks(cell_rows, to="pdtable"), as_dataframe=False)
    assert bundle.infs["file_bytes"].values[1] == 15326.0
    assert bundle is not None
    assert len(bundle) == 2
    assert isinstance(bundle[1], Table)

    # do not error on other table types
    bundle = TableBundle(parse_blocks(cell_rows, to="cellgrid"), as_dataframe=True)
    assert bundle is not None
    assert isinstance(bundle[0], list)  # cellgrid


def test_TableBundle_iterator():
    """ Verify that iterator is functioning as expected
    """
    bundle = TableBundle(parse_blocks(cell_rows, to="pdtable"))
    count = 0
    seen = {}
    for tab in bundle:
        assert type(tab) is Table
        seen[tab.name] = tab
        count += 1
    assert count == 2
    assert len(seen) == 2
    assert seen["foo"] is not None
    assert seen["infs"] is not None

    """ Verify that we can iterate other types than pdtable
    """
    bundle = TableBundle(parse_blocks(cell_rows, to="cellgrid"))
    count = 0
    for tab in bundle:
        assert type(tab) is list
        assert tab[0][0] in {"**foo", "**infs"}
        count += 1
    assert count == 2
    assert bundle["foo"] is not None
    assert bundle["infs"] is not None

    bundle = TableBundle(parse_blocks(cell_rows, to="jsondata"))
    count = 0
    for tab in bundle:
        assert type(tab) is dict
        assert tab["name"] in {"foo", "infs"}
        count += 1
    assert count == 2
    assert bundle["foo"] is not None
    assert bundle["infs"] is not None


def test_TableBundle_unique():
    """ Verify that unique() is functioning as expected
    """
    bundle1 = TableBundle(parse_blocks(cell_rows))
    # bundle1 now contains one 'foo' and one 'infs'
    assert len(bundle1) == 2

    with pytest.raises(LookupError):
        tab = bundle1.unique("-not there-")

    tab = bundle1.unique("foo")
    assert tab.name == "foo"

    tab = bundle1.unique("infs")
    assert tab.name == "infs"

    cells2 = []
    cells2.extend(cell_rows)
    cells2.extend([])
    cells2.extend(cell_rows)

    bundle2 = TableBundle(parse_blocks(cells2))
    # bundle2 now contains two 'foo' and two 'infs'
    assert len(bundle2) == 4

    with pytest.raises(LookupError):
        tab = bundle2.unique("-not there-")

    with pytest.raises(LookupError):
        tab = bundle2.unique("foo")

    with pytest.raises(LookupError):
        tab = bundle2.unique("infs")


def test_TableBundle_getitem():
    """ Verify that unique() is functioning as expected
    """
    bundle1 = TableBundle(parse_blocks(cell_rows))
    # bundle1 now contains one 'foo' and one 'infs'
    assert len(bundle1) == 2

    with pytest.raises(LookupError):
        tab = bundle1["-not there-"]

    # verify getitem
    with pytest.raises(TypeError):
        tab = bundle1[bundle1]

    # hashed
    tab = bundle1["foo"]
    assert tab.name == "foo"

    tab = bundle1["infs"]
    assert tab.name == "infs"

    # indexed
    tab = bundle1[0]
    assert tab.name == "foo"

    tab = bundle1[1]
    assert tab.name == "infs"

    with pytest.raises(IndexError):
        tab = bundle1[2]

    cells2 = []
    cells2.extend(cell_rows)
    cells2.extend([])
    cells2.extend(cell_rows)

    bundle2 = TableBundle(parse_blocks(cells2))
    # bundle2 now contains two 'foo' and two 'infs'
    assert len(bundle2) == 4

    with pytest.raises(LookupError):
        tab = bundle2["-not there-"]

    with pytest.raises(LookupError):
        tab = bundle2["foo"]

    with pytest.raises(LookupError):
        tab = bundle2["infs"]

    # indexed
    tab = bundle2[0]
    assert tab.name == "foo"

    tab = bundle2[1]
    assert tab.name == "infs"

    tab = bundle2[2]
    assert tab.name == "foo"

    tab = bundle2[3]
    assert tab.name == "infs"

    with pytest.raises(IndexError):
        tab = bundle2[4]


def test_TableBundle_all():
    """ Verify that all() is functioning as expected
    """
    bundle1 = TableBundle(parse_blocks(cell_rows))
    # bundle1 now contains one 'foo' and one 'infs'
    assert len(bundle1) == 2

    lst = bundle1.all("-not there-")
    assert len(lst) == 0

    lst = bundle1.all("foo")
    assert len(lst) == 1
    for tab in lst:
        assert tab.name == "foo"

    lst = bundle1.all("infs")
    assert len(lst) == 1
    for tab in lst:
        assert tab.name == "infs"

    cells2 = []
    cells2.extend(cell_rows)
    cells2.extend([])
    cells2.extend(cell_rows)

    bundle2 = TableBundle(parse_blocks(cells2))
    # bundle2 now contains two 'foo' and two 'infs'
    assert len(bundle2) == 4

    lst = bundle2.all("-not there-")
    assert len(lst) == 0

    lst = bundle2.all("foo")
    assert len(lst) == 2
    for tab in lst:
        assert tab.name == "foo"

    lst = bundle2.all("infs")
    assert len(lst) == 2
    for tab in lst:
        assert tab.name == "infs"


def test_TableBundle_attribute_error():
    bundle = TableBundle([])
    with pytest.raises(AttributeError):
        bundle.invalid_attribute_name


def test_TableBundle_in_operator():
    bundle = TableBundle(parse_blocks(cell_rows))
    assert "foo" in bundle
    assert "qux" not in bundle
