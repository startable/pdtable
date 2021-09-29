"""
The ``load``-module is responsible for reading input sets

Compared to the single-file reader functions, the load functionality adds:

  - Support of `***include` directives
  - Support for multiple input protocols (records systems, blobs, etc)
  - Support for tracking input origin

The ``load``-module is slightly opinionated. It is worth pointing out that the features of the ``table_origin`` module
does not rely on the ``load``-module: The ``table-origin`` functionality can be used from alternative loader systems.

Example of loading all files in a given folder::

    inputs = load_files(['/'], root_folder=root_folder, csv_sep=';')
    bundle =  TableBundle(inputs)

The Table objects returned by load will have an `origin` attribute describing their load herritage.
An example of how this can be used is the `make_location_trees`-function, which builds a tree-representation of the load process::

    location_trees = make_location_trees(iter(bundle))
    print('\n'.join(str(n) for n in location_trees))
"""
# flake8: noqa

from ._protocol import (
    LoadOrchestrator,
    Loader,
    LoadError,
)
from ._loaders import (
    FileReader,
    ProtocolLoader,
    FileSystemLoader,
    IncludeLoader,
)
from ._orchestrators import (
    load_files,
)
from ._tree import (
    LocationTreeNode,
    make_location_trees,
)