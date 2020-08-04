from tables.store import TableBundle
import pandas as pd
from typing import Iterable, TextIO, Union
from pathlib import Path

from ..pdtable import Table


def write_csv(tables: Union[Table, Iterable[Table], TableBundle], out: Union[str, Path, TextIO], sep: str = ';', na_rep: str = '-'):
    """Writes one or more tables to CSV file or stream.

    Writes table blocks in CSV format to a file or stream. Values are formatted to comply with the StarTable standard where necessary and possible; 
    otherwise they are simply str()'ed. 

    Args:
        tables: 
            Table(s) to write. Can be a single Table or an iterable of Tables. 
        out:
            File path or text stream to which to write. 
        sep:
            Optional; CSV field delimiter.
        na_rep:
            Optional; String representation of missing values (NaN, None, NaT). If overriding the default '-', it is recommended to use another value compliant with the StarTable standard.
    """

    if isinstance(tables, Table):
        # For convenience, pack single table in an iterable
        tables = [tables]
    
    # TODO Surely there's a better pattern than this? This one forces duplicate code... which I've packed away in a dummy function
    if isinstance(out, str) or isinstance(out, Path):
        # out is a file path. Open a stream and close it when done.
        with open(out, 'w') as f:
            _tables_to_csv_propagator(tables, f, sep, na_rep)
    else:
        # out is a stream opened by caller. Leave it open. 
        _tables_to_csv_propagator(tables, out, sep, na_rep)


def _tables_to_csv_propagator(tables: Union[Table, Iterable[Table], TableBundle], stream: TextIO, sep: str = ';', na_rep: str = '-') -> None:
    """Serves no purpose other than housing the duplicate code from the isinstance(out,...) conditional above
    """
    for table in tables:
        _table_to_csv(table, stream, sep, na_rep)


def _table_to_csv(table: Table, stream: TextIO, sep: str = ';', na_rep: str = '-') -> None:
    """Writes a single Table to stream as CSV. 
    """
    units = table.units
    stream.write(f'**{table.name}\n')
    stream.write(' '.join(str(x) for x in table.metadata.destinations) + '\n')
    stream.write(sep.join(str(x) for x in table.column_names) + '\n')
    stream.write(sep.join(str(x) for x in units) + '\n')
    for row in table.df.itertuples(index=False, name=None):
        # TODO: apply format string specified in ColumnMetadata
        stream.write(sep.join(_format_row_elements(row, units, na_rep)) + '\n')
    stream.write('\n')


def _format_row_elements(row: Iterable, col_units: Iterable, na_rep: str = '-'):
    """Formats row elements to comply with the StarTable standard when written to CSV. 
    """
    for col, (val, unit) in enumerate(zip(row, col_units)):
        if unit != 'text' and pd.isna(val):
            # Format NaN-like things, except leave them be in text columns
            yield na_rep
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
