# pdtable

![run-tests](https://github.com/startable/pdtable/workflows/run-tests/badge.svg)

The `pdtable` Python package offers interfaces to read, write, and manipulate StarTable data.

## Documentation

The pdtable documentation is available at [pdtable.readthedocs.io](https://pdtable.readthedocs.io/). 

## Examples 

Demo: see the [pdtable_demo notebook](examples/pdtable_demo.ipynb) or, if you a Jupyter notebook doesn't do it for you, the notebook's [paired script](examples/pdtable_demo.py).

## Installation

pdtable is available from pypi.org
```commandline
pip install pdtable
```
and from conda-forge
```commandline
conda install pdtable -c conda-forge
```

## Data and metadata: storage and access

Table blocks are stored as `TableDataFrame` objects, which inherit from `pandas.DataFrame` but include additional, hidden metadata. This hidden metadata contains all the information from Table blocks that does not fit in a classic Pandas dataframe object: table destinations, column units, table origin, etc.

Data in `TableDataFrame` objects can be accessed and manipulated using the Pandas API as it the object were a vanilla Pandas dataframe, with all the convenience that this entails.

The StarTable-specific metadata hidden in a `TableDataFrame`'s metadata *can* in principle be accessed directly; however a much more ergonomic interface is offered via a `Table` facade object, which is a thin wrapper around `TableDataFrame`.  `Table` also supports some limited data manipulation, though with the advantage of more easily supporting StarTable-specific metadata; for example, easily specifying column units when adding new columns.

## I/O

Readers and writers are available for CSV and Excel, both as files and as streams. Parsing is efficient and, by default, lenient, though this is readily customized.

Reading can also be filtered early, such that only certain block types or tables with certain names get fully parsed. This can reduce reading time substantially when reading e.g. only a few tables from an otherwise large file or stream.

Directive blocks are parsed by the readers, and presented to the client code for application-specific interpretation.

Import from and export to JSON is also supported.
