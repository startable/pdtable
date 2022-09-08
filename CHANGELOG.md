# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- Can now read empty tables (tables with only name and origin, no column or unit information).
- Fixes deprecation warning related to numpy type
- Fixes warning when calling `groupby` method

## [0.0.13]

### Fixed
- Issue in `make_table_dataframe` when using `unit_map`.

## [0.0.11]

### Added
- [#106] Support for detailed origin information in table metadata.
- [#106] Support for loading complete input sets.

### Changed
- Table origin information has been moved from `TableMetadata` to an aggregate object.
- The error type raised by the `fixer` has been changed to integrate with `load` framework.

### Fixed
- In response to a Numpy deprecation warning, changed one `dtype` argument from deprecated `numpy.str` to its designated successor, built-in `str`.

## [0.0.10] - 2021-03-16

### Changed

- `write_excel()` parameter `style` renamed to `styles`.
- Optionally specify custom styles in `write_excel()` by passing to parameter `styles` a JSON-like structure of dicts.
- When `write_excel()` parameter `styles` is truthy, transposed tables' column units and values are horizontally centered by default. This default is overridden by any horizontal alignment specified explicitly in `styles`. 
- `write_excel()` parameter `num_blank_rows_between_tables` renamed to `sep_lines` for conciseness.

### Fixed
- Replaced the one remaining reference to `numpy.bool` (now deprecated from numpy) with its designated successor, plain ol' built-in `bool`.

## [0.0.9] - 2021-02-23

### Added
- [PR #95](https://github.com/startable/pdtable/pull/95) `write_excel()` with new parameter `style=True` applies hard-coded styles to table blocks in output workbook.  

## [0.0.8] - 2021-02-15

### Added
- [#92](https://github.com/startable/pdtable/issues/92) `write_excel()` can write to multiple sheets in a workbook. 
- [#85](https://github.com/startable/pdtable/issues/85) CSV and Excel writers can write transposed tables.

### Changed
- [#42](https://github.com/startable/pdtable/issues/42) Don't show dummy index when Table rendered as string.

## [0.0.7] - 2021-01-18

### Fixed
- Invalid attribute access to `TableBundle` (e.g. `bundle["nonexistent_table"]`) raised a dubious `KeyError`; now raises a more appropriate `AttributeError`.

### Added
- Enable `in`-operator for TableBundle. Example: `assert "some_table_name" in bundle`

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

