import pandas as pd
from typing import Iterable, TextIO

from ..pdtable import Table


def _table_to_csv(table: Table, stream: TextIO, sep: str = ';', nan: str = '-') -> None:
    """
    Write Table to stream in CSV format.
    :param stream: Output stream, usually something returned by open()
    :param sep: CSV column separator character
    """
    units = table.units
    stream.write(f'**{table.name}\n')
    stream.write(' '.join(str(x) for x in table.metadata.destinations) + '\n')
    stream.write(sep.join(str(x) for x in table.column_names) + '\n')
    stream.write(sep.join(str(x) for x in units) + '\n')
    for row in table.df.itertuples(index=False, name=None):
        # TODO: apply format string specified in ColumnMetadata
        stream.write(sep.join(_format_row_elements(row, units, nan)) + '\n')
    stream.write('\n')


def _format_row_elements(row: Iterable, col_units: Iterable, nan: str = '-'):
    for col, (val, unit) in enumerate(zip(row, col_units)):
        if unit != 'text' and pd.isna(val):
            # Format NaN-like things, except leave them be in text columns
            yield nan
        elif unit == 'onoff':
            # Format obvious booleans as 0's and 1's
            if val in [True, 1]:
                yield '1'
            elif val in [False, 0]:
                yield '0'
            else:
                # If it isn't an obvious boolean, leave it be
                yield str(val)
        elif unit == 'text' and val == '' and col == 0:
            # Prevent illegal empty string in first column
            yield '-'  # some reasonable placeholder non-empty string
        else:
            # Leave everything else be as it is
            yield str(val)
