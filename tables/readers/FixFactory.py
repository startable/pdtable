from typing import List, Optional, Tuple, Any, TextIO
import pandas as pd
import numpy as np

import sys


class FixFactory:
    """ base class for auto-correcting input files

    """

    # Store legend of what's fixed + API
    def __init__(self):
        self.ctx = {}
        self.dbg = False

    @property
    def FileName(self):
        """
           Position context, origin / input filename
        """
        return self.ctx.get("FileName")

    @FileName.setter
    def FileName(self, val):
        self.ctx["FileName"] = val

    @property
    def TableName(self):
        """
           Position context in current table
        """
        return self.ctx.get("TableName")

    @TableName.setter
    def TableName(self, val):
        self.ctx["TableName"] = val

    @property
    def TableColumn(self):
        """
           Position context in current table
        """
        return self.ctx.get("TableColumn")

    @TableColumn.setter
    def TableColumn(self, val):
        self.ctx["TableColumn"] = val

    @property
    def TableRow(self):
        """
           Position context in current table
        """
        return self.ctx.get("TableRow")

    @TableRow.setter
    def TableRow(self, val):
        self.ctx["TableRow"] = val

    # named parameters w. type kwargs
    def fix_illegal_cell_value(self, vtype, value) -> bool:
        """
            Handle illegal value in cell

            vtype in { "onoff", "datetime", "-" }
        """
        defaults = {
            "onoff": False,
            "datetime": pd.NaT,
            "float": np.NaN,
            "-": np.NaN
            }
        if self.dbg:
            print(
                f'FixFacory: illegal {vtype} value "{value}" in table {self.TableName}'
            )
        dfval = defaults.get(vtype)
        if not dfval is None:
            return dfval
        else:
            return defaults["-"]
