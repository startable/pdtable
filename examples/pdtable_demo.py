# -*- coding: utf-8 -*-
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
# The `pdtable` package allows working with StarTable tables as pandas dataframes while consistently keeping track of StarTable-specific metadata such as table destinations and column units.
#
# This is implemented by providing two interfaces to the same backing object:
# - `PandasTable` is derived from `pandas.DataFrame`. It carries the data and is the interface of choice for interacting with the data using the pandas API, with all the convenience that this entails.  It also carries StarTable metadata, but in a less accessible way.
# - `Table` is a stateless façade for a backing `PandasTable` object. `Table` is the interface of choice for convenient access to StarTable-specific metadata such as column units, and table destinations and origin. Some data manipulation is also possible in the `Table` interface, though this functionality is limited.
#
# This notebook provides a demo of how this works. See the `pdtable` docstring for a discussion of the implementation.
#
# ## Idea
#
# The central idea is that as much as possible of the table information is stored as a pandas dataframe,
# and that the remaining information is stored as a `TableData` object attached to the dataframe as registered metadata.
# Further, access to the full table datastructure is provided through a facade object (of class `Table`). `Table` objects
# have no state (except the underlying decorated dataframe) and are intended to be created when needed and discarded afterwards:
#
# ```
# dft = make_pdtable(...)
# unit_height = Table(dft).height.unit
# ```
#
# Advantages of this approach are that:
#
# 1. Code can be written for (and tested with) pandas dataframes and still operate on `PandasTable` objects.
#    This frees client code from necessarily being coupled to the startable project.
#    A first layer of client code can read and manipulate StarTable data, and then pass it on as a (`PandasTable`-flavoured) `pandas.DataFrame` to a further layer having no knowledge of `pdtable`.
# 2. The access methods of pandas `DataFrames`, `Series`, etc. are available for use by consumer code via the `PandasTable` interface. This both saves the work
#    of re-implementing similar access methods on a StarTable-specific object, and likely allows better performance and documentation.

# %%
# %load_ext autoreload
# %autoreload 2
from textwrap import dedent

import pandas as pd

# %%
# Workaround for this demo in notebook form:
# Notebooks don't have repo root in sys.path
# Better solutions are welcome...
import sys
from pathlib import Path

if Path.cwd().name == "examples":
    # Maybe we're in a notebook in the /examples folder
    repo_root_dir = str(Path.cwd().parent)
    if repo_root_dir not in sys.path:
        sys.path.append(repo_root_dir)
        print(f"Added repo root dir to sys.path: {repo_root_dir}")

# %% [markdown]
# ## The `Table` aspect
#
# The table aspect presents the table with a focus on StarTable metadata.
# Data manipulation functions exist, and more are easily added -- but focus is on metadata.

# %%
from pdtable import pandastable
from pdtable.proxy import Table

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
str(t)

# %%
t.metadata

# %%
# Creating table from pandas dataframe:
t2 = Table(pd.DataFrame({"c": [1, 2, 3], "d": [4, 5, 6]}), name="table2", units=["m", "kg"])
t2

# %% [markdown]
# ## The `PandasTable` / `pd.DataFrame` aspect
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

# %% [markdown]
# ## I/O
# StarTable data can be read from, and written to, CSV files and Excel workbooks.


# %% [markdown]
# ### Reading
# The functions `read_excel()` and `read_csv()` do what it says on the tin.
# `read_csv()` can read from files as well as text streams.
#
# These generate tuples of (`BlockType`, `block`) where
# * `block` is a StarTable block
# * `BlockType` is an enum that indicates which type of StarTable block `block` is. Possible values: `DIRECTIVE`, `TABLE`, `TEMPLATE_ROW`, `METADATA`, and `BLANK`.

# %% [markdown]
# Let's make some CSV StarTable data, put it in a text stream, and read that stream. (Could just as well have been a CSV file.)

# %%
from io import StringIO
from pdtable import read_csv

csv_data = StringIO(
    dedent(
        """\
    author: ;XYODA     ;
    purpose:;Save the galaxy

    ***gunk
    grok
    jiggyjag

    **places;
    all
    place;distance;ETA;is_hot
    text;km;datetime;onoff
    home;0.0;2020-08-04 08:00:00;1
    work;1.0;2020-08-04 09:00:00;0
    beach;2.0;2020-08-04 17:00:00;1

    **farm_animals;;;
    your_farm my_farm other_farm;;;
    species;n_legs;avg_weight;
    text;-;kg;
    chicken;2;2;
    pig;4;89;
    cow;4;200;
    unicorn;4;NaN;
    """
    )
)

# Read the stream. Syntax is the same if reading CSV file.
# Reader function returns a generator; unroll it in a list for convenience.
block_list = list(read_csv(csv_data))
assert len(block_list) == 6  # includes blanks


# %% [markdown]
# The reader generates tuples of `(BlockType, block)`. Note that blank/comment lines are read but not parsed.

# %%
for bt, b in block_list:
    print(bt, type(b))

# %% [markdown]
# Here's one of the tables.

# %%
t = block_list[3][1]
print(t)
assert t.name == 'places'

# %% [markdown]
# We can pick the tables (and leave out blocks of other types) like this:

# %%
from pdtable import BlockType
tables = [b for bt, b in block_list if bt == BlockType.TABLE]
assert len(tables) == 2

# %% [markdown]
# ### Writing
# Use the aptly named `write_csv()` and `write_excel()`. Note that `write_csv()` can write to files as well as to text streams. 
#
# Let's write the tables we have to a text stream. 

# %%
from pdtable import write_csv

with StringIO() as s:
    write_csv(tables, s)
    
    print(s.getvalue())


# %% [markdown]
# ### JSON support
# StarTable data can be converted to and from a `JsonData` i.e. a JSON-ready data structure of nested dicts ("objects"), lists ("arrays"), and JSON-native values. This `JsonData` can then be serialized and deserialized directly using the standard library's `json.dump()` and `json.load()`. 
#
# A `JsonData` representation is currently only defined for table blocks. 
#
# Let's re-read the CSV stream we created earlier, but with tables as JSON data. 

# %%
csv_data.seek(0)
block_gen = read_csv(csv_data, to='jsondata')
json_ready_tables = [b for bt, b in block_gen if bt == BlockType.TABLE]

json_ready_tables[0]

# %% [markdown]
# This data structure is now easily serialized as JSON:

# %%
import json
table_json = json.dumps(json_ready_tables[0])
print(table_json)

# %% [markdown]
# There are also utilities to convert back and forth between `JsonData` and `Table`. 

# %%
from pdtable.io import json_data_to_table, table_to_json_data
t = json_data_to_table(json_ready_tables[0])
t  # It's now a Table

# %%
table_to_json_data(t)  # Now it's back to JsonData

# %% [markdown]
# ![Diagram of pdtable.io highlights](..\docs\diagrams\img\io\io.svg)

# %%
