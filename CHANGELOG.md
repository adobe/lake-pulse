# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2](https://github.com/adobe/lake-pulse/compare/v0.2.1...v0.2.2) - 2026-01-20

### Added

- Better formatting for bytes related values

### Dependencies

- Fix matching on Iceberg table version
- *(deps)* update iceberg requirement from 0.7.0 to 0.8.0
- *(deps)* update apache-avro requirement from 0.17 to 0.21

### Fixed

- Badge case to lower case

## [0.2.1](https://github.com/adobe/lake-pulse/compare/v0.2.0...v0.2.1) - 2026-01-12

### Documentation

- Fix the table formats badges
- Minor updates
- Minor improvements on docs

## [0.2.0](https://github.com/adobe/lake-pulse/compare/v0.1.4...v0.2.0) - 2026-01-10

### Added

- [**breaking**] Add support for HTTP/WebDAV
- [**breaking**] Add HDFS support

### Documentation

- Document supported storages

## [0.1.4](https://github.com/adobe/lake-pulse/compare/v0.1.3...v0.1.4) - 2026-01-08

### Documentation

- Fix table formats badge texts

## [0.1.3](https://github.com/adobe/lake-pulse/compare/v0.1.2...v0.1.3) - 2026-01-08

### Documentation

- Fix Lake Pulse logo for lib.rs

## [0.1.2](https://github.com/adobe/lake-pulse/compare/v0.1.1...v0.1.2) - 2026-01-08

### Documentation

- Minor cosmetic fixes
- Add downloads badge to Readme

## [0.1.1](https://github.com/adobe/lake-pulse/compare/v0.1.0...v0.1.1) - 2026-01-06

### Fixed

- Minor fix crate root page

## [0.1.0] - 2025-11-11

### Added
- Initial release of Lake Pulse
- Support for analyzing Delta Lake tables
- Support for analyzing Apache Iceberg tables
- Support for analyzing Apache Hudi tables
- Support for analyzing Lance tables
- Multi-cloud storage support (AWS S3, Azure Data Lake, GCP)
- Local filesystem support
- Comprehensive health metrics including:
  - File organization and compaction opportunities
  - Metadata analysis and schema evolution
  - Partition statistics
  - Time travel/snapshot metrics
  - Storage efficiency insights
- Builder pattern API for analyzer configuration
- Pretty-printed report output
- Examples for all supported table formats and storage providers

[Unreleased]: https://github.com/adobe/lake-pulse/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/adobe/lake-pulse/releases/tag/v0.1.0

