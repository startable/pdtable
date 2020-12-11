import sys
from typing import List, Any

import numpy as np
import pandas as pd


class ParseFixer:
    """ base class for auto-correcting errors and irregularities when parsing StarTable data

        * Possible specialization:

        class fixErrors(ParseFixer):
            # augment existing method
            def fix_illegal_cell_value(self, vtype, value):
                db_store(self.TableName,(self.TableColumn,self.TableRow))
                dfval = ParseFixer.fix_illegal_cell_value(self, vtype, value)
                return dfval

    """

    # Store legend of what's fixed + API
    def __init__(self):
        self._dbg = False
        self._errors = 0
        self._warnings = 0
        self._stop_on_errors = 1
        self.messages = []
        # Context info
        self.origin = None
        self.table_name = None
        self.column_name = None
        self.table_row = None

    @property
    def verbose(self):
        """
        if verbose: print debug info in fix_* methods
        """
        return self._dbg

    @verbose.setter
    def verbose(self, value: bool):
        self._dbg = value

    @property
    def stop_on_errors(self):
        """
        Raise exception if errors detected in input
        """
        return self._stop_on_errors

    @stop_on_errors.setter
    def stop_on_errors(self, value: bool):
        if value:
            self._dbg = True
        self._stop_on_errors = value

    @property
    def fixes(self):
        """ Number of warnings and errors fixed in input """
        return self._errors + self._warnings

    def reset_fixes(self):
        """ reset warning and error count """
        self._errors = 0
        self._warnings = 0

    def fix_duplicate_column_name(self, column_name: str, input_columns: List[str]) -> str:
        """
            The column_name already exists in  input_columns
            This method should provide a unique replacement name

        """
        msg = f"Duplicate column '{column_name}' at position {self.column_name} " \
              f"in table '{self.table_name}'."
        self.messages.append(msg)
        if self.verbose:
            print(msg)

        self._errors += 1
        for sq in range(1000):
            test = f"{column_name}_fixed_{sq:03}"
            if test not in input_columns:
                return test

        return "{column_name}-fixed"

    def fix_missing_rows_in_column_data(
        self, row: int, row_data: List[str], num_columns: int
    ) -> List[str]:
        """
            The row is expected to have num_columns values
            This method should return the entire row of length num_columns
            by providing the missing default values
        """
        msg = f"Missing data in row {row} of table '{self.table_name}'"
        self.messages.append(msg)
        if self.verbose:
            print(msg)
        row_data.extend(["NaN" for cc in range(num_columns - len(row_data))])
        self._errors += 1
        return row_data

    def fix_illegal_cell_value(self, vtype: str, value: str) -> Any:
        """
            The string value can not be converted to type vtype
            This method should return a suitable default value of type vtype

            Supported vtypes in { "onoff", "datetime", "-", "float" }
        """
        # TODO value can be something else than a string if it comes from e.g. Excel/openpyxl
        # TODO should not try to fix things that are illegal by design e.g. illegal empty cells
        defaults = {"onoff": False, "datetime": pd.NaT, "float": np.NaN, "-": np.NaN}
        msg = f"Illegal value '{value}' for unit '{vtype} ' in table '{self.table_name}'."
        self.messages.append(msg)
        if self.verbose:
            print(msg)
        default_val = defaults.get(vtype)
        self._warnings += 1
        if default_val is not None:
            return default_val
        else:
            return defaults["-"]

    def report(self):
        """ Inform user on stdout, stderr of any warnings / errors
        """
        if self.fixes > 0 and self.stop_on_errors:
            txt = f"Stopped parsing after {self.fixes} errors in table '{self.table_name}' " \
                  f"with messages:\n"
            txt += '\n'.join(self.messages)
            raise ValueError(txt)

        if hasattr(self, "_called_from_test"):
            return

        if self._warnings > 0:
            print(
                f"\nWarning: {self._warnings} data errors fixed while parsing "
                f"table '{self.table_name}'\n"
            )

        if self._errors > 0:
            sys.stderr.write(
                f"\nError: {self._errors} column errors fixed while parsing "
                f"table '{self.table_name}'\n"
            )
