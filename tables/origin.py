class TableOrigin:
    """
    A TableOrigin instance uniquely defines the source of a Table instance.

    Subclasses should take care to define __str__.
    If possible, as_html() should be defined to include backlink to original input.
    """
    def as_html(self) -> str:
        return str(self)


class TableOriginCSV(TableOrigin):
    def __init__(self, file_name: str = '', row: int = 0):
        self._file_name = file_name
        self._row = row

    def __str__(self) -> str:
        return f'"{self._file_name}" row {self._row}'

    def __repr__(self) -> str:
        return f'TableOriginCSV({self})'