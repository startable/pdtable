# pdtable: StarTables as Pandas dataframes

![run-tests](https://github.com/startable/pdtable/workflows/run-tests/badge.svg)

PandasTable (`pdtable`) store startable objects as Pandas dataframes with hidden metadata.
The metadata can be accessed via a `Table` facade that can be initialized (and dropped) at any point.

```
    my_table = Table(my_pdtable)
    my_table['my_column'].unit = 'km'
```

For a full demo, see the [pdtable_demo notebook](examples/pdtable_demo.ipynb) or, if you don't have Jupyter handy, the notebook's [paired script](examples/pdtable_demo.py).
