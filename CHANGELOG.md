# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

