from pathlib import Path

from tables.directives import Directive
from tables.readers.read_csv import read_file_csv
from tables.store import BlockGenerator, BlockType


def handle_includes(bg: BlockGenerator, input_dir) -> BlockGenerator:
    """Handles 'include' directives recursively.

    'include' directives must contain a list of files located in input_dir.
    """
    for block_type, block in bg:
        if block_type == BlockType.DIRECTIVE:
            directive: Directive = block
            if directive.name == "include":
                for filename in directive.lines:
                    yield from handle_includes(read_file_csv(Path(input_dir) / filename), input_dir)
            else:
                yield block_type, block
        else:
            yield block_type, block