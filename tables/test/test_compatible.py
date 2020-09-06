from io import StringIO

from tables.readers.read_csv import read_stream_csv
from textwrap import dedent
from tables.store import TableBundle


def test_read_csv_compatible1():
    """
      test_read_csv_compatible

      handle '-' in cells
      handle leading and trailing wsp
    """

    cell_rows = [line.split(";") for line in dedent(
        r"""
    **test_input;
    all;
    numerical;dates;onoffs;
    -;datetime;onoff;
    123;08/07/2020;0;
     123; 08-07-2020; 1;
     123 ; 08-07-2020 ; 1 ;
    1.23;-;-;
     1.23; -; -;
     1.23 ; - ; - ;
     -1.23 ; - ; - ;
     +1.23 ; - ; - ;
    """
    ).strip().split("\n")]

    table = TableBundle(read_stream_csv(cell_rows, sep=";"))
    assert table

    assert table.test_input.onoffs[0] == False
    assert table.test_input.onoffs[1] == True
    assert table.test_input.onoffs[2] == True
    for idx in range(0, 3):
        assert table.test_input.dates[idx].year == 2020
        assert table.test_input.dates[idx].month == 7
        assert table.test_input.dates[idx].day == 8

    for idx in range(0, 3):
        assert table.test_input.numerical[idx] == 123

    assert table.test_input.numerical[3] == 1.23
    assert table.test_input.numerical[5] == 1.23
    assert table.test_input.numerical[7] == 1.23
    assert table.test_input.numerical[6] == -1.23


def test_read_csv_compatible2():
    """
      test_read_csv_compatible2

      handle leading and trailing wsp in column_name, unit
    """

    cell_rows = [line.split(";") for line in dedent(
        r"""
    **test_input;
    all;
    numerical ; dates; onoffs ;
     - ; datetime;onoff ;
    123;08/07/2020;0;
    """
    ).strip().split("\n")]

    table = TableBundle(read_stream_csv(cell_rows, sep=";"))
    assert table

    assert table.test_input.onoffs[0] == False
    assert table.test_input.dates[0].year == 2020
    assert table.test_input.numerical[0] == 123
