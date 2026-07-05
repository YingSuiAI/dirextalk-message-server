# Ops Official Plugin Development Tracker

This document tracks the first-version implementation of the official Ops plugin `io.dirextalk.ops`.
Items are marked complete only after matching code, tests, and verification exist.

## Business Background

Single-node private Dirextalk deployments need an operator-facing plugin for understanding server health, backups, migration readiness, and safe cleanup. The plugin is official-only because it requires higher runtime privileges than normal plugins, including Docker container inspection and a backup volume.

The first version prioritizes safe operations:

- Every destructive cleanup must support dry-run planning before execution.
- High-risk cleanup must require a recent backup and explicit confirmation.
- Chat history cleanup must not directly delete Matrix event tables.
- Referenced media must not be removed by the Ops plugin.
- Integration testing must not restart or mutate the live `x1.dirextalk.ai` node; use local single-node or local-domain stacks.

## Requirements Checklist

- [x] Catalog exposes `io.dirextalk.ops` as an official plugin using `docker.io/dirextalk/ops-plugin:latest`.
- [x] Server action allowlist includes all public Ops actions.
- [x] Docker runner supports per-plugin volume mounts.
- [x] Only `io.dirextalk.ops` can mount Docker socket and the Ops backup volume.
- [x] Ops runtime receives `OPS_BACKUP_ROOT`, `OPS_MAX_BACKUPS`, `OPS_MESSAGE_SERVER_CONTAINER`, and `OPS_POSTGRES_CONTAINER`.
- [x] Non-Agent plugins do not receive `DIREXTALK_AGENT_TOKEN`.
- [x] Compose files define the Ops backup volume and runtime env without touching live x1 volumes.
- [x] Ops plugin implements status overview for host, Docker, Postgres, message-server, plugin containers, disk, and backups.
- [x] Ops plugin implements backup creation with manifest and chunked download.
- [x] Ops plugin implements migration export and restore planning without automatic cross-server restore.
- [x] Ops plugin implements cleanup plan/run with confirmation.
- [x] Ops plugin room cleanup supports `chat_cache`, `chat_hide`, `chat_archive`, and `media_cache` planning without physical Matrix event deletion.
- [x] Ops plugin media orphan scan supports preview first and avoids referenced media deletion by default.
- [x] Flutter plugin management exposes Ops entry from the plugin list.
- [x] Flutter Ops page includes overview, containers, backups, cleanup, migration, and room cleanup flows.
- [x] Flutter room cleanup exposes room selection, time range, cleanup type, dry-run impact, and confirmation input.
- [x] Multi-language strings are added for user-visible Ops labels.

## Operation Flow

1. Owner installs or enables `io.dirextalk.ops` from the plugin catalog.
2. Server starts the official Ops container with restricted high-privilege mounts.
3. Client opens the Ops page and calls `plugins.invoke` with Ops actions.
4. Status and backup actions return direct operational data.
5. Cleanup actions first call a plan action and show estimated impact.
6. Execution actions require `plan_id` and exact confirmation text.
7. Restore remains a plan/instructions action in the first version.

## Safety Boundaries

- The Ops plugin must never receive owner access token or Agent token.
- The Ops plugin must not perform direct SQL deletion of Matrix events.
- Physical purge of Matrix event history is out of scope for version one.
- Media cleanup may remove caches and clear confirmed orphan files only after backup and confirmation.
- Integration verification must avoid live `x1.dirextalk.ai` service restarts, volume cleanup, or mount mutation.

## Public Interfaces

- `ops.status.get`
- `ops.containers.list`
- `ops.logs.tail`
- `ops.backups.list`
- `ops.backup.create`
- `ops.backup.download_chunk`
- `ops.backup.delete`
- `ops.cleanup.plan`
- `ops.cleanup.run`
- `ops.rooms.cleanup.plan`
- `ops.rooms.cleanup.run`
- `ops.media.orphans.plan`
- `ops.migration.export`
- `ops.restore.plan`

## Verification Log

- [x] Server red tests added for catalog, runtime env, and privileged mount isolation.
- [x] Server implementation passes focused plugin tests.
- [x] Ops plugin tests cover status, backup manifest/chunking, cleanup dry-run/confirm, room cleanup plan, and media orphan preview.
- [x] Flutter tests or analysis cover Ops page routing and localization.
- [x] Local integration uses isolated single-node/temporary Ops container flow, not `x1.dirextalk.ai`.
