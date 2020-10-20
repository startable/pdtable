import functools
from pathlib import Path

from ..auxiliary import Directive
from ..io.csv import read_csv
from ..store import BlockIterator, BlockType


def handle_includes(bg: BlockIterator, input_dir, recursive: bool = False) -> BlockIterator:
    """Handles 'include' directives, optionally recursively.

    Handles 'include' directives.
    'include' directives must contain a list of files located in directory 'input_dir'.

    Optionally handles 'include' directives recursively. No check is done for circular references.
    For example, if file1.csv includes file2.csv, and file2.csv includes file1.csv, then infinite
    recursion ensues upon reading either file1.csv or file2.csv with 'recursive' set to True.

    Args:
        bg:
            A block generator returned by read_file_csv

        input_dir:
            Path of directory in which include files are located.

        recursive:
            Handle 'include' directives recursively, i.e. 'include' directives in files themselves
            read as a consequence of an 'include' directive, will be handled. Default is False.

    Yields:
        A block generator yielding blocks from...
        * if recursive, the entire tree of files in 'include' directives.
        * if not recursive, the top-level file and those files listed in its 'include' directive (if
          any).

    """

    deep_handler = (
        functools.partial(handle_includes, input_dir=input_dir, recursive=recursive)
        if recursive
        else lambda x: x
    )

    for block_type, block in bg:
        if block_type == BlockType.DIRECTIVE:
            directive: Directive = block
            if directive.name == "include":
                for filename in directive.lines:
                    yield from deep_handler(read_csv(Path(input_dir) / filename))
            else:
                yield block_type, block
        else:
            yield block_type, block
