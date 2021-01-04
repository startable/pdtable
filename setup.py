#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

from setuptools import setup, find_packages
from os import path
import re

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


def find_version(*file_paths):
    """
    Reads version from a file. Version must be specified explicitly in the file as:
    __version__ = "<the version number string>"
    """
    version_file = open(path.join(*file_paths), "r").read()
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="pdtable",
    version=find_version("pdtable", "__init__.py"),
    description="Reads and writes data stored in StarTable format; and stores table data in"
    "memory as a Pandas data frame for easy manipulation.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/startable/pdtable/",
    author="Jean-François Corbett",
    author_email="jeaco@orsted.dk",
    license="BSD-3-Clause",
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
    ],
    # What does your project relate to?
    keywords="startable data-structure file-format table dataframe json",
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(exclude=["contrib", "doc", "docs", "test", "tests"]),
    python_requires=">=3.7",
    install_requires=["numpy", "pandas"],
    tests_require=["pytest", "openpyxl", "pint"],
    # Same for developer dependencies
    # extras_require={
    #     'dev': ['check-manifest'],
    #     'test': ['coverage'],
    # },
)
