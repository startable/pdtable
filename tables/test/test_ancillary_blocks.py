from io import StringIO
from pathlib import Path
from textwrap import dedent

from ..ancillary_blocks import Directive, MetadataBlock
from ..demo.directive_handlers import handle_includes
from ..readers.read_csv import read_stream_csv
from ..store import BlockType


def test_metadata_block():
    ml = MetadataBlock()
    ml["author"] = "XYODA"
    ml["purpose"] = "Save the galaxy"
    assert repr(ml) == dedent("""\
        author:;XYODA;
        purpose:;Save the galaxy;""")


def test_directive():
    d = Directive("foo", ["bar", "baz"])
    assert repr(d) == dedent("""\
        ***foo;
        bar
        baz""")


def test_handle_includes():
    # TODO move directive handler stuff to a /demo package or something
    cell_rows = [line.split(";") for line in dedent(r"""
        ***include
        incl_1.csv
        incl_2.csv
    
        **t0
        all
        place;distance
        text;km
        home;0
        work;14
        beach;19
        """).strip().split("\n")]

    bg = handle_includes(read_stream_csv(cell_rows, sep=";"),
                         input_dir=Path(__file__).parent / "input", recursive=True)
    bl = list(bg)
    tables = [b for t, b in bl if t == BlockType.TABLE]
    assert len(tables) == 4

