from pathlib import Path
from pdtable.store import BlockType

from pdtable import read_csv, Table
from pdtable.table_origin import LoadItem, TableOrigin, LocationFile
from pdtable.io.load import FilesystemLoader, load_all, LoadOrchestrator

def test_include():
    input_folder = Path(__file__).parent / 'input/with_include'

    def read(location_file: LocationFile, _: LoadOrchestrator):
        location_sheet = location_file.make_location_sheet()
        yield from read_csv(location_file.get_local_path(), location_sheet=location_sheet)

    roots = [LoadItem('/', source=None)]
    loader=FilesystemLoader(file_reader=read, root_folder=input_folder)
    res = [b for b in load_all(roots, loader)]

    assert {b.name for t, b in res if t==BlockType.TABLE} == {'bar_table', 'bar_abs_table'}
