Reading
=======

The functions ``read_excel()`` and ``read_csv()`` do what it says on the
tin. They can read StarTable data from files as well as text streams.

They generate tuples of (``block_type``, ``block``) where 

* ``block`` is a StarTable block 
* ``block_type`` is a ``BlockType`` enum that
  indicates which type of StarTable block ``block`` is. Possible values:
  ``DIRECTIVE``, ``TABLE``, ``TEMPLATE_ROW``, ``METADATA``, and ``BLANK``.

Let’s make some CSV StarTable data, put it in a text stream, and read
that stream. (Reading from a CSV file is no different.)

.. code:: ipython3

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
    assert len(block_list) == 4

The reader generates tuples of ``(BlockType, block)``. Note that
blank/comment lines are read but not parsed.

.. code:: ipython3

    for bt, b in block_list:
        print(bt, type(b))


.. parsed-literal::

    BlockType.METADATA <class 'pdtable.auxiliary.MetadataBlock'>
    BlockType.DIRECTIVE <class 'pdtable.auxiliary.Directive'>
    BlockType.TABLE <class 'pdtable.proxy.Table'>
    BlockType.TABLE <class 'pdtable.proxy.Table'>
    

Here’s one of the tables.

.. code:: ipython3

    t = block_list[2][1]
    assert t.name == "places"
    print(t)


.. parsed-literal::

    \*\*places
    all
      place [text]  distance [km]      ETA [datetime]  is_hot [onoff]
    0         home            0.0 2020-08-04 08:00:00            True
    1         work            1.0 2020-08-04 09:00:00           False
    2        beach            2.0 2020-08-04 17:00:00            True
    

We can pick the tables (and leave out blocks of other types) like this:

.. code:: ipython3

    from pdtable import BlockType
    
    tables = [b for bt, b in block_list if bt == BlockType.TABLE]
    assert len(tables) == 2

Filtered reading
----------------

Blocks can be filtered prior to parsing, by passing to a callable as
``filter`` argument to the reader functions. This can reduce reading
time substantially when reading only a few tables from an otherwise
large file or stream.

The callable must accept two arguments: block type, and block name. Only
those blocks for which the filter callable returns ``True`` are fully
parsed. Other blocks are parsed only superficially i.e. only the block’s
top-left cell, which is just enough to recognize block type and name to
pass to the filter callable, thus avoiding the much more expensive task
of parsing the entire block, e.g. the values in all columns and rows of
a large table.

Let’s design a filter that only accepts tables whose name contains the
word ``'animal'``.

.. code:: ipython3

    def is_table_about_animals(block_type: BlockType, block_name: str) -> bool:
        return block_type == BlockType.TABLE and "animal" in block_name

Now let’s see what happens when we use this filter when re-reading the
same CSV text stream as before.

.. code:: ipython3

    csv_data.seek(0)
    block_list = list(read_csv(csv_data, filter=is_table_about_animals))
    
    assert len(block_list) == 1
    block_list[0][1]




.. parsed-literal::

    \*\*farm_animals
    my_farm, your_farm, other_farm
      species [text]  n_legs [-]  avg_weight [kg]
    0        chicken         2.0              2.0
    1            pig         4.0             89.0
    2            cow         4.0            200.0
    3        unicorn         4.0              NaN



Non-table blocks were ignored, and so were table blocks that weren’t
animal-related.

Handling directives
-------------------

StarTable Directive blocks are intended to be interpreted and handled at
read time, and then discarded. The client code (i.e. you) is responsible
for doing this. Handling directives is done outside of the ``pdtable``
framework. This would typically done by a function that consumes a
``BlockGenerator`` (the output of the reader functions), processes the
directive blocks encountered therein, and in turn yields a processed
``BlockGenerator``, as in:

::

   def handle_some_directive(bg: BlockGenerator, \*args, \*\*kwargs) -> BlockGenerator:
       ...

Usage would then be:

::

   block_gen = handle_some_directive(read_csv('foo.csv'), args...)

Let’s imagine a directive named ``'include'``, which the client
application is meant to interpret as: include the contents of the listed
StarTable CSV files along with the contents of the file you’re reading
right now. Such a directive could look like:

::

   \*\*\*include
   bar.csv
   baz.csv

Note that there’s nothing magical about the name “include”; it isn’t
StarTable syntax. This name, and how such a directive should be
interpreted, is entirely defined by the application. We could just as
easily imagine a ``rename_tables`` directive requiring certain table
names to be amended upon reading.

But let’s stick to the “include” example for now. The application wants
to interpret the ``include`` directive above as: read two additional
files and throw all the read blocks together. Perhaps even recursively,
i.e. similarly handle any ``include`` directives encountered in
``bar.csv`` and ``baz.csv`` and so on. A handler for this could be
designed to be used as

::

   block_gen = handle_includes(read_csv('foo.csv'), input_dir='./some_dir/', recursive=True)

and look like this:

.. code:: ipython3

    import functools
    from pdtable import BlockIterator, Directive
    
    
    def handle_includes(bg: BlockIterator, input_dir, recursive: bool = False) -> BlockIterator:
        """Handles 'include' directives, optionally recursively.
    
        Handles 'include' directives.
        'include' directives must contain a list of files located in directory 'input_dir'.
    
        Optionally handles 'include' directives recursively. No check is done for circular references.
        For example, if file1.csv includes file2.csv, and file2.csv includes file1.csv, then infinite
        recursion ensues upon reading either file1.csv or file2.csv with 'recursive' set to True.
    
        Args:
            bg:
                A block generator returned by read_csv
    
            input_dir:
                Path of directory in which include files are located.
    
            recursive:
                Handle 'include' directives recursively, i.e. 'include' directives in files themselves
                read as a consequence of an 'include' directive, will be handled. Default is False.
    
        Yields:
            A block generator yielding blocks from...
            * if recursive, the entire tree of files in 'include' directives.
            * if not recursive, the top-level file and those files listed in its 'include' directive (if
              any).
    
        """
    
        deep_handler = (
            functools.partial(handle_includes, input_dir=input_dir, recursive=recursive)
            if recursive
            else lambda x: x
        )
    
        for block_type, block in bg:
            if block_type == BlockType.DIRECTIVE:
                directive: Directive = block
                if directive.name == "include":
                    # Don't emit this directive block; handle it.
                    for filename in directive.lines:
                        yield from deep_handler(read_csv(Path(input_dir) / filename))
                else:
                    yield block_type, block
            else:
                yield block_type, block

This is only an example implementation. You’re welcome to poach it, but
note that no check is done for circular references! E.g. if ``bar.csv``
contains an ``include`` directive pointing to ``foo.csv`` then calling
``handle_includes`` with ``recursive=True`` will result in a stack
overflow. You may want to perform such a check if relevant for your
application.

