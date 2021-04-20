import pytest
from textwrap import dedent
from pathlib import Path
import re

from pdtable.store import BlockType
from pdtable import read_csv, Table
from pdtable.table_origin import LoadItem, TableOrigin, LocationFile
from pdtable.io.load import FilesystemLoader, load_files, LoadOrchestrator, make_location_trees


@pytest.fixture
def input_folder():
    return Path(__file__).parent / 'input'


def test_include(input_folder):
    root_folder = input_folder /'with_include'
    res = {b.name: b for t, b in load_files(['/'], root_folder=root_folder, csv_sep=';') if t==BlockType.TABLE}

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
    <root_folder: {root_folder}>
      Row 0 of 'input_foo.csv'
        bar_abs.csv
          **bar_abs_table
        bar.csv
          **bar_table
    """).strip()
    assert tree_as_str == reference_str


def test_excel(input_folder):
    blocks = load_files(['/'], root_folder=input_folder, 
                        file_name_pattern=re.compile(r'.*\.xlsx$'),
                        sheet_name_pattern=re.compile(r'.*'))
    res = [b for t, b in blocks  if t==BlockType.TABLE]

    assert len(res) == 6