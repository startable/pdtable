# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

No unreleased changes yet.

## [0.0.6] - 2021-01-06

### Fixed

- [#87](https://github.com/startable/pdtable/issues/87) Parser fails on tables with zero rows. (Edge case not explicitly ruled out in the StarTable specification.)

## [0.0.5] - 2021-01-04

### Added

- For convenience, `Column` proxy is is now iterable. For example, `list(table["some_col"])` now works, whereas previously you had to access the backing `TableDataFrame` as `list(table.df["some_col"])`. Saving 3 chars FTW. 
- CI pipeline now auto releases to PyPI on pushed tags starting with a "v". 

### Fixed

- \#82: `read_excel()` fails on formulas 

## [0.0.4] - 2020-12-11

### Added

- Major ergonomics improvement: The reader functions now output helpful error messages when parsing stops due to errors in the input table blocks.

### Changed

- `write_excel()` parameter `path` renamed to `to`, reflecting the fact that it can write not only to file but also to binary stream; and by the same token, harmonizing its API with `read_csv()`. 

### Fixed

- 77: Harmful assert in `read_excel()` prevented reading from certain kinds of streams; now removed.


## [0.0.3] - 2020-11-13

### Added

- Read transposed tables 

### Fixed

- More robust propagation of `ComplementaryTableInfo` (i.e. StarTable-specific metadata) when operating on `TableDataFrame`s via the pandas API.  

## [0.0.2] - 2020-10-30

First public release. 

