import pytest
from textwrap import dedent
from pathlib import Path
import re

import pdtable
from pdtable.store import BlockType
from pdtable.io.load import load_files, make_location_trees, FileReader
from pdtable.table_origin import InputError


def test_file_reader():
    r = FileReader(sheet_name_pattern=re.compile(".*"))

    assert set(r.supported_extensions) == {"csv", "xlsx"}
    assert r.supported_filename_pattern.pattern == r".*\.(csv|xlsx)$"


@pytest.fixture
def input_folder() -> Path:
    return Path(__file__).parent / "input"


def test_include(input_folder):
    root_folder = input_folder / "with_include"
    res = {
        b.name: b
        for t, b in load_files(
            ["/"], root_folder=root_folder, csv_sep=";", file_name_start_pattern="(input|setup)_"
        )
        if t == BlockType.TABLE
    }

    assert set(res.keys()) == {"bar_table", "bar_abs_table"}
    input_location = res["bar_table"].metadata.origin.input_location

    spec_ref = list(
        zip(
            str(input_location.file.load_specification).split(";"),
            [
                """included as "bar.csv" from """,
                """included as "input_foo.csv" from """,
                """included as "/" from <root>""",
            ],
        )
    )
    spec_ref_ok = [s.startswith(ref) for s, ref in spec_ref]
    assert all(spec_ref_ok)

    location_trees = make_location_trees(res.values())
    tree_as_str = "\n".join(str(n) for n in location_trees)
    reference_str = dedent(
        f"""
    <root_folder: {root_folder}>
      Row 0 of 'input_foo.csv'
        bar_abs.csv
          **bar_abs_table
        bar.csv
          **bar_table
    """
    ).strip()
    assert tree_as_str == reference_str



def test_include_without_root(input_folder):
    root_folder = input_folder / "with_include"
    blocks = list(load_files([root_folder/"foo_relative.csv"], csv_sep=";"))

    res = {b.name: b for t, b in blocks if t == BlockType.TABLE}

    assert set(res.keys()) == {"bar_table", "bar_abs_table"}

def test_excel_all_files(input_folder):
    blocks = load_files(
        ["/"],
        root_folder=input_folder,
        # negative lookahead to avoid excel temp files...
        file_name_pattern=re.compile(r"(?!~\$).*\.xlsx$"),
    )
    res = [b for t, b in blocks if t == BlockType.TABLE]

    assert len(res) == 9


def test_issue_raised(input_folder):
    # error because only absolute path is accepted relative to root folder
    with pytest.raises(pdtable.table_origin.InputError):
        list(load_files(["somefile.xlsx"], root_folder=input_folder))


def test_excel_sheets(input_folder):
    blocks = list(
        load_files(
            ["file:/multipage.xlsx"],
            root_folder=input_folder,
            sheet_name_pattern=re.compile(r"^input.*"),
        )
    )
    res = {b.name for t, b in blocks if t == BlockType.TABLE}
    assert "setup_table" not in res
    assert res.issuperset({"places_to_go", "spelling_numbers"})

    blocks = list(
        load_files(
            ["file:/multipage.xlsx"],
            root_folder=input_folder,
            sheet_name_pattern=re.compile(r"^(input|setup).*", re.IGNORECASE),
        )
    )
    res = {b.name for t, b in blocks if t == BlockType.TABLE}
    assert res.issuperset({"places_to_go", "setup_table", "spelling_numbers"})


def test_include_loop(input_folder: Path):
    f = input_folder / "with_loop_include/load_include_loop.csv"
    with pytest.raises(InputError, match="Load location included multiple times"):
        blocks = list(load_files([f.absolute()]))


