import io
from textwrap import dedent

from ...pdtable import Table
from ..csv_writer import _table_to_csv


def test_table_to_csv():
    t = Table(name='foo')
    t['place'] = ['home', 'work', 'beach', 'wonderland']
    t.add_column('distance', list(range(3)) + [float('nan')], 'km')

    out = io.StringIO()
    _table_to_csv(t, out)
    assert out.getvalue() == dedent("""\
        **foo
        all
        place;distance
        text;km
        home;0.0
        work;1.0
        beach;2.0
        wonderland;nan

        """)

