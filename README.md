# StarTables as Pandas dataframes

PandasTable (`pdtable`) store startable objects as Pandas dataframes with hidden metadata.
The metadata can be accessed via a `Table` facade that can be initialized (and dropped) at any point.

```
    my_table = Table(my_pdtable)
    my_table['my_column'].unit = 'km'
```
