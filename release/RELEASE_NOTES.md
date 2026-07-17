# Dirextalk Message Server release notes

## v1.0.4

This release adds the owner-only HTTP `release.v2.status` and
`release.v2.apply` flow for centrally selected direct upgrades. The updater
pins the fixed target image to an immutable registry digest before host
mutation. Replay-only recovery rotates a job ticket without consulting mutable
central metadata or creating a second job. The middle-platform `server` record
owns the target version and minimum compatible client version.

Docker builds now default to the canonical bootstrap version `v1.0.0` instead
of a development-only identifier. Formal releases still inject their exact
target `vX.Y.Z` into both build stages and reject the image unless the embedded
server version matches, keeping client version detection and update reminders
actionable.

Native Agent prompts now include the server-authoritative current user identity
and successful chats may return deterministic room and channel-post navigation
references derived only from successful built-in Dirextalk tool results. Group,
channel, MCP, and Matrix write-policy owner handling now uses the authoritative
`m.room.create` sender and exact full Matrix ID, with deterministic join-order
fallback for unresolved legacy rooms.

The release also includes the durable, strict-input foundation for the legacy
Matrix Agent gateway. Production activation remains deliberately unavailable
until the exclusive-consumer cutover and completion projection gates are
implemented. Server schema version 2 and the supported client-version range
remain unchanged; PostgreSQL storage adds the internal v38 invocation
reservation migration.

## v1.0.3

This release makes group joins, contact-request decisions, and channel join
approvals recoverable and idempotent across request cancellation, response
loss, process restart, and concurrent server instances. Matrix membership
remains the final joined fact; ProductCore projections and callbacks now use
durable operation leases and generation-aware compare-and-swap persistence.

The ProductCore action names and existing success payloads remain compatible.
Recovery responses add optional top-level `operation_id`, `status`,
`current_room_id`, and structured `error_code` fields to HTTP and WebSocket
results. Database schema version 2 adds durable operation recovery metadata,
including base-generation fencing for cross-instance retries;
schema version 1 remains readable, and upgrades require the normal retained
backup.

## v1.0.2

This release keeps the exact `v1.0.1` formal image as its stable upgrade
source. Server schema, updater API, Product actions, and client compatibility
remain unchanged. Legacy hosts must first be moved to a formal baseline under
an operator-controlled backup; unindexed legacy images remain fail closed.

## v1.0.1

This security patch updates `golang.org/x/crypto` to `v0.52.0`. It does not
change the server schema, updater API, Product action contract, or supported
client-version range.

The trusted release index permits only the exact tested `v1.0.0` image digest
to upgrade to `v1.0.1`. Other source images continue to fail closed.

## v1.0.0

This is the first formal, immutable server release. The release version is
reported as `v1.0.0`; its source commit and build time remain separate build
metadata.

### Compatibility

- Server schema version: `1`.
- Oldest readable server schema version: `1`.
- The original v1.0.0 publication used GitHub manifest/index assets. Current
  releases use the middle-platform server record for target and minimum-client
  selection and do not publish those assets.

### Backup and rollback

An upgrade requires a backup. Rollback restores the single retained backup
created before the current deployment attempt; it does not reuse an arbitrary
older backup.

Current publication uses the project-local
`dirextalk-message-server-release` Skill and
`scripts/release/{prepare,verify,publish}.sh`. The scripts publish the fixed
version image and asset-free GitHub Release before they move `latest` to the
same digest.
