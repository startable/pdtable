Reading
=======

The functions ``read_excel()`` and ``read_csv()`` do what it says on the
tin. They can read StarTable data from files as well as text streams.

They generate tuples of (``block_type``, ``block``) where 

* ``block`` is a StarTable block 
* ``block_type`` is a ``BlockType`` enum that
  indicates which type of StarTable block ``block`` is. Possible values:
  ``DIRECTIVE``, ``TABLE``, ``TEMPLATE_ROW``, ``METADATA``, and ``BLANK``.

Reading example
---------------

Let’s make some CSV StarTable data, put it in a text stream, and read
that stream. (Reading from a CSV file is no different.)

.. code:: python

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

>>> for bt, b in block_list:
...     print(bt, type(b))
...
BlockType.METADATA <class 'pdtable.auxiliary.MetadataBlock'>
BlockType.DIRECTIVE <class 'pdtable.auxiliary.Directive'>
BlockType.TABLE <class 'pdtable.proxy.Table'>
BlockType.TABLE <class 'pdtable.proxy.Table'>
    

Here’s one of the tables.

>>> t = block_list[2][1]
>>> assert t.name == "places"
>>> print(t)
**places
all
    place [text]  distance [km]      ETA [datetime]  is_hot [onoff]
0         home            0.0 2020-08-04 08:00:00            True
1         work            1.0 2020-08-04 09:00:00           False
2        beach            2.0 2020-08-04 17:00:00            True
 

We can pick the tables (and leave out blocks of other types) like this:

.. code:: python

    from pdtable import BlockType
    
    tables = [b for bt, b in block_list if bt == BlockType.TABLE]
    assert len(tables) == 2

Filtered reading
----------------

Blocks can be filtered prior to parsing, by passing a callable as
``filter`` argument to the reader functions. This can reduce reading
time substantially when reading only a few tables from an otherwise
large file or stream.

The callable must accept two arguments: block type, and block name. Only
those blocks for which the filter callable returns ``True`` are fully
parsed. Other blocks are parsed only superficially, meaning that only the block’s
top-left cell is parsed, which is just enough to recognize block's type and name to
pass to the filter callable, thus avoiding the much more expensive task
of parsing the entire block, e.g. the values in all columns and rows of
a large table.

Let’s design a filter that only accepts tables whose name contains the
word ``'animal'``.

.. code:: python

    def is_table_about_animals(block_type: BlockType, block_name: str) -> bool:
        return block_type == BlockType.TABLE and "animal" in block_name

Now let’s see what happens when we use this filter when re-reading the
same CSV text stream as before.

>>> csv_data.seek(0)
>>> block_list = list(read_csv(csv_data, filter=is_table_about_animals)) 
>>> assert len(block_list) == 1
>>> block_list[0][1]
**farm_animals
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
``BlockIterator`` (the output of the reader functions), processes the
directive blocks encountered therein, and in turn yields a processed
``BlockIterator``, as in:

::

   def handle_some_directive(bi: BlockIterator, *args, **kwargs) -> BlockIterator:
       ...

Usage would then be:

::

   block_it = handle_some_directive(read_csv('foo.csv'), args...)

Let’s imagine a directive named ``include``, which the client
application is meant to interpret as: include the contents of the listed
StarTable CSV files along with the contents of the file you’re reading
right now. Such a directive could look like:

::

   ***include
   bar.csv
   baz.csv

Note that there’s nothing magical about the name “include”; it isn’t
StarTable syntax. This name, and how such a directive should be
interpreted, is entirely defined by the application. We could just as
easily imagine a ``rename_tables`` directive requiring certain table
names to be amended upon reading.

But let’s stick to the “include” example for now. The application wants
to interpret the ``include`` directive above as: read all the blocks from the two additional CSV
files listed and throw them in the pile. Perhaps even recursively,
i.e. similarly handle any ``include`` directives encountered in
``bar.csv`` and ``baz.csv`` and so on. A handler for this could be
designed to be used as::

   block_it = handle_includes(read_csv('foo.csv'), input_dir='./some_dir/', recursive=True)

and look like this:

.. code:: python

    import functools
    from pdtable import BlockIterator, Directive
    
    
    def handle_includes(bi: BlockIterator, input_dir, recursive: bool = False) -> BlockIterator:
        """Handles 'include' directives, optionally recursively.
    
        Handles 'include' directives.
        'include' directives must contain a list of files located in directory 'input_dir'.
    
        Optionally handles 'include' directives recursively. No check is done for circular references.
        For example, if file1.csv includes file2.csv, and file2.csv includes file1.csv, then infinite
        recursion ensues upon reading either file1.csv or file2.csv with 'recursive' set to True.
    
        Args:
            bi:
                A block iterator returned by read_csv
    
            input_dir:
                Path of directory in which include files are located.
    
            recursive:
                Handle 'include' directives recursively, i.e. 'include' directives in files themselves
                read as a consequence of an 'include' directive, will be handled. Default is False.
    
        Yields:
            A block iterator yielding blocks from...
            * if recursive, the entire tree of files in 'include' directives.
            * if not recursive, the top-level file and those files listed in its 'include' directive (if
              any).
    
        """
    
        deep_handler = (
            functools.partial(handle_includes, input_dir=input_dir, recursive=recursive)
            if recursive
            else lambda x: x
        )
    
        for block_type, block in bi:
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

Alternatively, look into the section on `Loading input sets`_ below, which includes a more robust implementation
of the ``include``-directive.


Loading input sets
------------------

The term "reading" has been used above to describe the low-level process of reading a single input location.
We will use the term "loading" for the process of reading a full input set, possibly from multiple storage backends,
while recording information about the input locations for each table. Support for "loading" in this sense is
implemented in the ``io.load`` and ``table_origin`` modules. These modules are built as layers on top of the
fundamental readers, and a given project can choose to not use either, to use only ``table_origin`` or to use both.
The design is intended to be flexible enough to support most scenarios, while still being convenient to use
for the most basic use-case of reading a folder of files.

As an example of using the modules, this is how you would use the convenience function ``load_files`` to load
an filesystem input-set based on a root folder and a file name pattern::

    bundle = pdtable.TableBundle(pdtable.load_files(
        root_folder=inputs,
        file_name_start_pattern="(input|setup)_"))

The included load information can be accessed via the ``.metadata``-field as::

    input_location = bundle['bar_table'].metadata.origin.input_location

The resulting object can be inspected via the string-representation::

    >>> print(input_location.interactive_identifier)
    >>> print('\n'.join(str(input_location.load_specification).split(';')))
    Row 0 of 'bar.csv'
    included as "bar.csv" from Row 0 of 'input_foo.csv'"
    included as "input_foo.csv" from <root_folder: C:\Code\github_corp\pdtable\pdtable\test\io\input\with_include>"
    included as "/" from <root>"



Alternatively, the function ``make_location_trees`` can be used to generate a tree-representaion of the load
process for an entire table bundle::

    >>> for tree in pdtable.io.load.make_location_trees(bundle):
    ...     print(tree)
    <root_folder: C:\Code\github_corp\pdtable\pdtable\test\io\input\with_include>
      Row 0 of 'input_foo.csv'
        bar_abs.csv
          **bar_abs_table
        bar.csv
          **bar_table

For detailed information about the module, please refer to the source code and the ``docs/diagrams`` folder in the
repository.

