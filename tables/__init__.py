# flake8: noqa

CSV_SEP = ";"  # User can overwrite this default


def csv_sep():
    """Returns value of CSV_SEP as available at runtime"""
    return CSV_SEP


from .proxy import Table
from .table_metadata import TableOrigin
from .store import TableBundle
from .units import UnitPolicy
from .utils import read_bundle_from_csv, normalized_table_generator
from .writers._csv import write_csv
from .writers._excel import write_excel
