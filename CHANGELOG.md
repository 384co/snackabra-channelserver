# Changelog

Channel Server changelog.

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

- removed blockedMessages code

- all SSO / Admin code removed

- all api calls changed to allow binary or string (json)

- almost all api calls require a body including separately
  wrapped and signed 'apiBody'

- lock mechanism changed: now essentially more of a firewall

- 'verifiedGuest' concept removed (moving to app layers)

- 'channelKeys' mostly removed; keys are now ChannelData,
  and strictly speaking only needs Owner public key

- all crypto conversions/formats mostly removed, now
  use the jslib 'SB384' class

- characteristics like 'motd' remoed (moved to app level)

- added 'ttl' mechanism to messages

- added 'subChannel' concept (look for 'i2')

- now tracking 'motherChannel' throughout

- creating channels can now be done through either 'budd'
  or an explicit storage token

- 'LEDGER_KEY' concept removed

- 'personalRoom' concept removed

- 'joinRequests' and joining is all removed, a locked
  channel now will block hard if userId not pre-accepted

- no concept of 'first message' anymore - userId must
  identify and authenticate on every api call and message

- signing no longer HMAC, now using ECDSA

- any concepts of 'cookies' removed

- 'sendTo' userId now allowed; Owner gets copies

- special case of TTL '0' supported - ephemeral messages
  that will not be stored

- no global cursor, they are on per-session basis

- several data structures that were clunky arrays
  are now either Set() or Map()

- added internal cache of SB384 objects matching visitors

- all admin-related api endpoints removed

- no concept of 'owner rotation' - owner controls
  encryption keys directly above (outside) channel

- well-defined concept of ChannelMessage, with
  validator, as well as well-defined 'stripped'

- broke out 'env.ts' and 'workers.ts'.  the latter
  is code that is shared verbatim with storage servers

- much code that was explicit / utils have been
  removed and is imported from jslib

- channel identifiers are now a32, meaning base62x43
  (256 bits)

- a32 discontinued and it's all now 'base62'

- standardized "you're not allowed to do that" messages
  to all use the same (ANONYMOUS_CANNOT_CONNECT_MSG)

- added class SBMessageCache. not activated yet.

- all api calls now forced to be binary

- budd() semantics changed

- swept over all api endpoint calls, one change is they
  are now all in consistent lower camel case

- cloudflare worker name is now 'channel' (dev) or 'c' (prod)

- debug output on/off now in toml file

- KEYS and RECOVERY namespaces no longer needed

- toml file in general simplified and cleaned up

- 'template.wrangler.toml' is (public) reference template

- adding 'Pages' feature. includes direct access to IMAGES

- refactored into new function consumeStorage(); prepared for API budget usage

- added 'return304' as helper, added Etag support for Pages

- adding '.env' file to set 'IS_LOCAL'

- stopping use of local storage (eg DO KV) for messages

## [1.1.0] - 2023-03-01

### Added

- This is template for future version tracking

[Unreleased]: https://github.com/384co/snackabra-jslib/compare/v0.6.5...development
[1.1.0]: https://github.com/384co/snackabra-jslib/compare/v1.0.1...v1.1.0
