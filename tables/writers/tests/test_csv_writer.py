import io
from textwrap import dedent
import datetime as dt

import pandas as pd

from ...pdtable import Table
from ..csv_writer import _table_to_csv, _format_row_elements


def test_format_row_elements():
    units = ['text', 'km', 'datetime', 'onoff']
    # Standard stuff
    assert list(_format_row_elements(
        ('foo', 123, pd.to_datetime('2020-08-04 08:00'), True), units)) \
        == ['foo', '123', '2020-08-04 08:00:00', '1']

    # With NaN-like things
    assert list(_format_row_elements(
        ('foo', float('nan'), pd.NaT, 1), units)) \
        == ['foo', '-', '-', '1']
    
    # Specifying how NaN-like things should be displayed
    assert list(_format_row_elements(
        ('foo', float('nan'), pd.NaT, False), units, nan='NaN')) \
        == ['foo', 'NaN', 'NaN', '0']

    # Empty strings: replace illegal in first column, leave others
    assert list(_format_row_elements(('','',float('nan')), ['text', 'text', 'text'])) \
        == ['-', '', 'nan']


def test_table_to_csv():
    t = Table(name='foo')
    t['place'] = ['home', 'work', 'beach', 'wonderland']
    t.add_column('distance', list(range(3)) + [float('nan')], 'km')
    t.add_column('ETA', pd.to_datetime(['2020-08-04 08:00', '2020-08-04 09:00', '2020-08-04 17:00', pd.NaT]), 'datetime')
    t.add_column('is_hot', [True, False, True, False], 'onoff')

    out = io.StringIO()
    _table_to_csv(t, out)
    assert out.getvalue() == dedent("""\
        **foo
        all
        place;distance;ETA;is_hot
        text;km;datetime;onoff
        home;0.0;2020-08-04 08:00:00;1
        work;1.0;2020-08-04 09:00:00;0
        beach;2.0;2020-08-04 17:00:00;1
        wonderland;-;-;0

        """)



