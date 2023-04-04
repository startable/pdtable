from enum import Enum
import pandas as pd
import pytest
from pdtable import Table, HAS_PYARROW


class PandasBackend(Enum):
    numpy = "NUMPY"
    pyarrow = "PYARROW"


if HAS_PYARROW:
    TESING_BACKENDS = [PandasBackend.numpy, PandasBackend.pyarrow]
else:
    TESING_BACKENDS = [PandasBackend.numpy]


@pytest.fixture(scope="function", params=TESING_BACKENDS)
def places_table(request):
    # Make a table with content of various units
    t = Table(name="foo")
    if request.param == PandasBackend.numpy:
        t["place"] = ["home", "work", "beach", "wonderland"]
        t.add_column("distance", list(range(3)) + [float("nan")], "km")
        t.add_column(
            "ETA",
            pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT]),
            "datetime",
        )
        t.add_column("is_hot", [True, False, True, False], "onoff")
    elif request.param == PandasBackend.pyarrow:
        import pyarrow
        t["place"] = pd.Series(["home", "work", "beach", "wonderland"], dtype="string[pyarrow]")
        t.add_column("distance", pd.Series(list(range(3)) + [float("nan")], dtype="double[pyarrow]"), "km")
        t.add_column(
            "ETA",
            pd.to_datetime(["2020-08-04 08:00", "2020-08-04 09:00", "2020-08-04 17:00", pd.NaT])
            .astype(pd.ArrowDtype(pyarrow.timestamp('s'))),
            "datetime",
        )
        t.add_column("is_hot", pd.Series([True, False, True, False], dtype="bool[pyarrow]"), "onoff")
    return t
