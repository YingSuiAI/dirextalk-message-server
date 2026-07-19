# Dirextalk Message Server release notes

## v1.0.6

This release aligns the existing Agent Cloud v2 plan, quote, approval, and
service-operation forwarding with the Worker-control PrivateLink scope. Message
Server forwards and validates the additive private-connectivity fields and the
private-endpoint service, security-group-source, and endpoint-type enum values
for the S3 Gateway, Secrets Manager Interface, and Worker Control Interface
operations.

The server pins the exact published Agent module
`v0.1.0-alpha.20260719.6-7ac10ce17ae5` with no local module replacement.
Server schema version 2 and readable schema version 1 are unchanged.

New-device recovery now includes a deterministic, metadata-only
`sync.bootstrap.read_markers` snapshot. Durable read markers advance only for
strictly newer server-resolved Matrix timeline positions; optional client
timestamps are non-authoritative. Equal, missing, invalid, and skewed event
timestamps, delayed requests, replay, and concurrent writes cannot regress
unread recovery boundaries across restarts. Event resolution is bound to the
authenticated owner and applies the existing Matrix event-visibility checks
before exposing or persisting a boundary.

## v1.0.5

This release moves Agent Chat, immutable runtime-profile selection, the
owner-only Knowledge action family, and typed Cloud control/query surfaces
behind the independently authenticated Agent gRPC boundary. ProductCore
remains the compatibility façade while Agent owns durable task, deployment,
service, Knowledge, Worker, Recipe, and cloud-resource facts; strict owner,
revision, idempotency, and response-validation fences keep credentials,
backend endpoints, raw Knowledge content, and internal errors out of public
responses.

The Cloud workflow now covers typed connection bootstrap, planning and
approval, provisioning, managed-service preparation and lifecycle, Worker
recovery, health, retained backup, restore, and bounded ProductCore
projections. Remote Knowledge uploads are limited to 64 MiB and 256 canonical
chunks, use immutable binding/source revision fences, and never fall back to
local state when the remote Agent backend is enabled.

The server pins the exact published Agent module needed by this contract and
the production Docker build resolves it without a sibling checkout or local
module replacement. Server schema version 2 and readable schema version 1 are
unchanged.

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
