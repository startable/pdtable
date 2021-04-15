
from textwrap import dedent
from pathlib import Path
from pdtable.store import BlockType

from pdtable import read_csv, Table
from pdtable.table_origin import LoadItem, TableOrigin, LocationFile
from pdtable.io.load import FilesystemLoader, load_files, LoadOrchestrator, make_location_trees

def test_include():
    input_folder = Path(__file__).parent / 'input/with_include'
    res = {b.name: b for t, b in load_files(['/'], root_folder=input_folder, csv_sep=';') if t==BlockType.TABLE}

    assert set(res.keys()) == {'bar_table', 'bar_abs_table'}
    input_location = res['bar_table'].metadata.origin.input_location

    assert all(s.startswith(ref) for s, ref in zip(
        str(input_location.file.load_specification).split(';'),
        [
            """included as "bar.csv" from """,
            """included as "input_foo.csv" from """,
            """included as "/" from "<root>""",
        ]
    ))

    location_trees = make_location_trees(res.values())
    tree_as_str = '\n'.join(str(n) for n in location_trees)
    reference_str = dedent(f"""
    <root_folder: {input_folder}>
      Row 0 of 'input_foo.csv'
        bar_abs.csv
          **bar_abs_table
        bar.csv
          **bar_table
    """).strip()
    assert tree_as_str == reference_str