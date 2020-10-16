import io
from textwrap import dedent
from typing import List

from pytest import fixture

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
        place;distance;ETA;is_hot
        text;km;datetime;onoff
        home;0.0;2020-08-04 08:00:00;1
        work;1.0;2020-08-04 09:00:00;0
        beach;2.0;2020-08-04 17:00:00;1
        
        ::;details about various places;;
    
        **farm_animals;;;
        your_farm my_farm other_farm;;;
        species;n_legs;avg_weight;
        text;-;kg;
        chicken;2;2;
        pig;4;89;
        cow;4;200;
        unicorn;4;NaN;
        """
    )


def test_read_csv(csv_data):
    bl = list(read_csv(io.StringIO(csv_data)))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    template_rows = [b for t, b in bl if t == BlockType.TEMPLATE_ROW]
    met = [b for t, b in bl if t == BlockType.METADATA]

    assert len(met) == 1
    assert tables[0].df["place"][1] == "work"
    assert len(template_rows) == 1


def test_read_csv__sep_is_comma(csv_data):
    bl = list(read_csv(io.StringIO(csv_data.replace(";", ",")), sep=","))
    tables: List[Table] = [b for t, b in bl if t == BlockType.TABLE]
    template_rows = [b for t, b in bl if t == BlockType.TEMPLATE_ROW]
    met = [b for t, b in bl if t == BlockType.METADATA]

    assert len(met) == 1
    assert tables[0].df["place"][1] == "work"
    assert len(template_rows) == 1
