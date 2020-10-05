import io

# TBC: group these types in same import
from pdtable.frame import get_table_info
from pdtable.units import UnitPolicy
from pdtable.utils import read_bundle_from_csv
from textwrap import dedent


class convert_kg(UnitPolicy):
    """ Specific UnitPolicy, convert kg to g
    """

    def convert_value_to_base(self, values, unit: str):
        if unit != "kg":
            return values, unit
        return values * 1000, "g"


def test_read_bundle_from_csv():
    """ verify that custom UnitPolicy translates kg to g
        verify that read_bundle_from_csv can be called with
          unit_policy as an instance
          unit_policy as a type
    """
    # fmt off
    instr = dedent(
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
        """
    )
    # fmt on

    # test 1]: unit_policy as an instance
    stream = io.StringIO(instr)
    bundle = read_bundle_from_csv(stream, unit_policy=convert_kg())

    assert len(bundle) > 0

    for tab in bundle:
        assert tab["flt"].unit == "g"
        assert tab["flt"].values[4] == 200000

    # test 2]: unit_policy as a type
    stream = io.StringIO(instr)
    bundle = read_bundle_from_csv(stream, unit_policy=convert_kg)
    assert len(bundle) > 0
    for tab in bundle:
        assert tab["flt"].unit == "g"
        assert tab["flt"].values[0] == 3000
