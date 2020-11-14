Unit conversion
===================================

Each column of a ``Table`` has a unit attribute. Pdtable provides a framework to convert a table's values to a different set of units. 

To convert units on a ``Table``, you have to specify two things: 

* What units you want to convert to; and 
* A *unit converter* describing how to do the conversion from the existing units to the new units.

This unit converter is a ``Callable`` of your choice with this signature:

::

   def some_converter(value, from_unit, to_unit):
      # convert value
      ...
      return converted_value

For convenience, ``pdtable`` ships with a Pint-based unit converter that you
can use as-is or inherit from and configure at will. ``pint`` is an
optional dependency; it doesn't need to be installed unless and until you use the
Pint-based converter.

Consider this example table:

.. code:: python

    t = Table(name="places")
    t.add_column("where", ["home", "work", "beach"], unit="text")
    t.add_column("distance", [0, 14, 18], unit="km")
    t["is_hot"] = [False, False, True]
    t["ETA"] = [datetime(2020, 11, 2), pd.to_datetime("2020-11-02 9:00:00"), datetime(2020, 11, 7)]
    t["ETA"].unit = 'datetime'
    t.df.loc[len(t.df)] = ["unicornland", np.nan, False, pd.NaT]
    t    

.. parsed-literal::

    \*\*places
    all
      where [text]  distance [km]  is_hot [onoff]      ETA [datetime]
    0         home            0.0           False 2020-11-02 00:00:00
    1         work           14.0           False 2020-11-02 09:00:00
    2        beach           18.0            True 2020-11-07 00:00:00
    3  unicornland            NaN           False                 NaT


Here's an example where we explicitly specify which columns to convert
to what unit, using the bundled pint-based unit converter. In this example, we 
only ask to convert a single column. 

.. code:: python

    t.convert_units(to={"distance": "lightyear"}, 
                    converter=pdtable.pint_converter)


.. parsed-literal::

    \*\*places
    all
      where [text]  distance [lightyear]  is_hot [onoff]      ETA [datetime]
    0         home          0.000000e+00           False 2020-11-02 00:00:00
    1         work          1.479801e-12           False 2020-11-02 09:00:00
    2        beach          1.902602e-12            True 2020-11-07 00:00:00
    3  unicornland                   NaN           False                 NaT



If your converter implements the concept of “base unit” then you can bulk convert all columns to their base units with ``to='base'``. The built-in pint-based converted does support this. For instance, the base unit of "km" is "meter". 

.. code:: python

    t.convert_units(to="base", converter=pdtable.pint_converter)


.. parsed-literal::

    \*\*places
    all
      where [text]  distance [meter]  is_hot [onoff]      ETA [datetime]
    0         home               0.0           False 2020-11-02 00:00:00
    1         work           14000.0           False 2020-11-02 09:00:00
    2        beach           18000.0            True 2020-11-07 00:00:00
    3  unicornland               NaN           False                 NaT



Specifying which converter to use every single time can get tedious. You can set
a default converter once and for all:

.. code:: python

    pdtable.units.default_converter = pdtable.pint_converter
    t.convert_units(to="base")


.. parsed-literal::

    \*\*places
    all
      where [text]  distance [meter]  is_hot [onoff]      ETA [datetime]
    0         home               0.0           False 2020-11-02 00:00:00
    1         work           14000.0           False 2020-11-02 09:00:00
    2        beach           18000.0            True 2020-11-07 00:00:00
    3  unicornland               NaN           False                 NaT

