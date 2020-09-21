# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.5.2
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Demo of `pdtable`
#
# The `pdtable` module allows working with StarTable tables as pandas dataframes.
#
# This is implemented by providing both `Table` and `PandasTable` (dataframe) interfaces to the same object.
#
# This notebook provides a demo of how this works, see the `pdtable` docs for a discussion of the implementation.
#
# ## Idea
#
# The central idea is that as much as possible of the table information is stored as a pandas dataframe,
# and that the remaining information is stored as a `TableData` object attached to the dataframe as registered metadata.
# Further, access to the full table datastructure is provided through a facade object (of class `Table`). `Table` objects
# have no state (except the underlying decorated dataframe) and are intended to be created when needed and discarded
# afterwards:
#
# ```
# dft = make_pdtable(...)
# unit_height = Table(dft).height.unit
# ```
#
# Advantages of this approach are that:
#
# 1. Code can be written for (and tested with) pandas dataframes and still operate on `pdtable` objects.
#    This avoids unnecessary coupling to the startable project.
# 2. The table access methods of pandas are available for use by consumer code. This both saves the work
#    of making startable-specific access methods, and likely allows better performance and documentation.

# %%
# %load_ext autoreload
# %autoreload 2
import pandas as pd

# %%
# Workaround: notebooks don't have repo root in sys.path
# Better solutions are welcome...
import sys
from pathlib import Path

if Path.cwd().name == "doc":
    # Maybe we're in a notebook in the /reporting/doc folder
    repo_root_dir = str(Path.cwd().parent)
    if repo_root_dir not in sys.path:
        sys.path.append(repo_root_dir)
        print(f"Added repo root dir to sys.path: {repo_root_dir}")

# %% [markdown]
# ## The `Table` aspect
#
# The table aspect present the table with a focus on surrounding metadata.
# Data manipulation functions exist, and more are easily added -- but focus is on metadata.

# %%
from pdtable import pandastable, Table

# %%
t = Table(name="mytable")

# Add columns explicitly...
t.add_column("a", range(5), "km")

# ...or via item access
t["b"] = ["the foo"] * 5

# Modify column metadata through column proxy objets:
t["a"].unit = "m"

# Table renders as annotated dataframe
t

# %%
t.column_names

# %%
# Each column has associated metadata object that can be manipulated:
t["b"]

# %%
t.units

# %%
t.metadata

# %%
# Creating table from pandas dataframe:
t2 = Table(pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"])
t2

# %% [markdown]
# ## The dataframe aspect
#
# Both the table contents and metadata displayed and manipulated throught the `Table`-class is stored as a `PandasTable` object, which is a normal pandas dataframe with two modifications:
#
# * It has a `_table_data` field registered as a metadata field, so that pandas will include it in copy operations, etc.
# * It will preserve column and table metadata for some pandas operations, and fall back to returning a normal dataframe if this is not possible/implemented.
#

# %%
# After getting a reference to the dataframe backing t, it is safe to delete t:
df = t.df
del t
Table(df)

# %%
# Interacting with table metadata form  without the Table proxy is slightly verbose
df2 = pandastable.make_pdtable(
    pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"]
)

# %%
# Example: combining columns from multiple tables
df_combinded = pd.concat([df, df2], sort=False, axis=1)
Table(df_combinded)

# %% [markdown]
# ## Fundamental issues with the facade approach
#
# The key problem with not integrating more tightly with the dataframe datastructure is that some operations will not trigger `__finalize__` and thus have to be deciphered based on the dataframe and metadata states.
#
# One example of this is column names:

# %%
df_renamed = df.copy()
df_renamed.columns = ["a_new", "b_new"]
print(f"Metadata columns before update triggered by access:\n{df_renamed._table_data.columns}\n")
print(Table(df_renamed))
print(f"Metadata columns after update:\n{df_renamed._table_data.columns}\n")

# %%
