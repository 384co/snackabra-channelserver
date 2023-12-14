# Changelog

Follow format from [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning 2.0.0](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- adding CHANGELOG.md

- adding VERSION

- disabling getLastMessageTimes on channel server, needs
  a few issues to be resolved (see source)

- adding "/api/info" which can be queried for general
  information about the channel server, including:

- adding wrangler env variable STORAGE_SERVER. channel
  server will replace "name" and return that as info,
  eg, it will provide the storage server that (always)
  corresponds to any (new) storage requests

## [1.1.0] - 2023-03-01

### Added

- This is template for future version tracking

[Unreleased]: https://github.com/384co/snackabra-jslib/compare/v0.6.5...development
[1.1.0]: https://github.com/384co/snackabra-jslib/compare/v1.0.1...v1.1.0
