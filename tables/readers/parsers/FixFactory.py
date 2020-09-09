from typing import List, Tuple, Any, Dict
import pandas as pd
import numpy as np

import sys


class FixFactory:
    """ base class for auto-correcting startable.csv input files

        * Possible specialization:

        class fixErrors(FixFactory):
            # augment existing method
            def fix_illegal_cell_value(self, vtype, value):
                db_store(self.TableName,(self.TableColumn,self.TableRow))
                dfval = FixFactory.fix_illegal_cell_value(self, vtype, value)
                return dfval



    """

    # Store legend of what's fixed + API
    def __init__(self):
        self.ctx = {}
        self._dbg = False

    @property
    def FileName(self):
        """
           Position context, current origin / input filename
        """
        return self.ctx.get("FileName")

    @FileName.setter
    def FileName(self, val):
        self.ctx["FileName"] = val

    @property
    def TableName(self):
        """
           Position context, current table
        """
        return self.ctx.get("TableName")

    @TableName.setter
    def TableName(self, val):
        self.ctx["TableName"] = val

    @property
    def TableColumn(self):
        """
           Position context, current column
        """
        return self.ctx.get("TableColumn")

    @TableColumn.setter
    def TableColumn(self, val):
        self.ctx["TableColumn"] = val

    @property
    def TableRow(self):
        """
           Position context, current row
        """
        return self.ctx.get("TableRow")

    @TableRow.setter
    def TableRow(self, val):
        self.ctx["TableRow"] = val

    @property
    def Verbose(self):
        """
        if Verbose: print debug info in fix_* methods
        """
        return self._dbg

    @Verbose.setter
    def Verbose(self, value: bool):
        self._dbg = value

    def fix_duplicate_column_name(self, column_name: str, input_columns: List[str]) -> str:
        """
            The column_name already exists in  input_columns
            This method should provide a unique replacement name

        """
        if self.Verbose:
            print(
                f"FixFacory: fix duplicate column ({self.TableColumn}) {column_name} in table: {self.TableName}"
            )

        for sq in range(1000):
            test = f"{column_name}_fixed_{sq:03}"
            print(f"test: {test}")
            if not test in input_columns:
                return test

        return "{column_name}-fixed"

    def fix_missing_column_name(self, col: int, input_columns: List[str]) -> str:
        """
            The column_name: input_columns[col] is empty
            This method should provide a unique replacement name
        """
        if self.Verbose:
            print(
                f"FixFacory: fix missing column ({col}) {input_columns} in table: {self.TableName}"
            )
        # TTT : check
        return "-missing-"

    def fix_missing_rows_in_column_data(
        self, row: int, row_data: List[str], num_columns: int
    ) -> List[str]:
        """
            The row is expected to have num_columns values
            This method should return the entire row of length num_columns
            by providing the missing default values
        """
        if self.Verbose:
            print(f"FixFacory: fix missing data in row ({row}) in table: {self.TableName}")
        row_data.extend(["NaN" for cc in range(num_columns - len(row_data))])
        return row_data

    def fix_illegal_cell_value(self, vtype: str, value: str) -> Any:
        """
            The string value can not be converted to type vtype
            This method should return a suitable default value of type vtype

            Supported vtypes in { "onoff", "datetime", "-", "float" }
        """
        defaults = {"onoff": False, "datetime": pd.NaT, "float": np.NaN, "-": np.NaN}
        if self.Verbose:
            print(f'FixFacory: illegal {vtype} value "{value}" in table {self.TableName}')
        dfval = defaults.get(vtype)
        if not dfval is None:
            return dfval
        else:
            return defaults["-"]
