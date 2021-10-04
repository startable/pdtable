"""
The ``io.load``-module is responsible for reading input sets

The ``io.load`` module is implemented as a layer on top of the format-specific single-file readers in 
the ``io``-module. The added functionality include:

  - Support of `***include` directives
  - Support for reading entire directories
  - Support for tracking input origin
  - Support for multiple file types identified by extension
  - Support for multiple input protocols (records systems, blobs, etc)
  - Future support for multithreaded readers

The ``io.load``-module relies heavily on the features of the ``table_origin`` module, but there is no inverse
dependency, i.e. the ``table-origin`` functionality can be used from alternative loader systems without any 
reference to the ``io.load``-module.

Example of loading all files of supported formats in a given folder::

    inputs = load_files(['/'], root_folder=root_folder, csv_sep=';')
    bundle =  TableBundle(inputs)

The Table objects returned by load will have an `origin` attribute describing their load herritage.
An example of how this can be used is the `make_location_trees`-function, which builds a tree-representation of the load process::

    location_trees = make_location_trees(iter(bundle))
    print('\n'.join(str(n) for n in location_trees))

The functionality of the ``load``-module is implemented in a composable form to allow it to be integrated with 
project-specific storage systems. In the simplest form, this can be done by passing loaders for additional protocols
to ``load_files``. More involved customizations can be obtained by manually composing the master loader instance. 
The implementation for ``load_files`` uses a composition of ``ProtocolLoader``, ``FileSystemLoader`` and ``IncludeLoader``.
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
