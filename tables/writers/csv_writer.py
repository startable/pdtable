import pandas as pd
from typing import Optional, TextIO

from ..pdtable import Table


def _table_to_csv(table: Table, stream: TextIO, sep: str = ';') -> None:
    """
    Write Table to stream in CSV format.
    :param stream: Output stream, usually something returned by open()
    :param sep: CSV column separator character
    """
    df = table.df
    # df = _prepare_df_for_write(df)
    stream.write(f'**{table.name}\n')
    stream.write(' '.join(str(x) for x in table.metadata.destinations) + '\n')
    stream.write(sep.join(str(x) for x in table.column_names) + '\n')
    stream.write(sep.join(str(x) for x in table.units) + '\n')
    for row in df.itertuples(index=False, name=None):
        # TODO: ensure NaNs, NaTs get converted to valid StarTable NaN marker
        # TODO: apply format string specified in ColumnMetadata
        # TODO: ensure onoff converted to 0's and 1's
        stream.write(sep.join(map(str, row)) + '\n')
    stream.write('\n')


# def _prepare_df_for_write(df: pd.DataFrame) -> pd.DataFrame:
#     df = self.df.fillna(NO_DATA_MARKER_ON_WRITE)
#     for col, col_spec in self._col_specs.items():
#         if col_spec.format_str:
#             df[col] = df[col].apply(
#                 lambda x: x if x == NO_DATA_MARKER_ON_WRITE else col_spec.format_str.format(x))
#     return df
