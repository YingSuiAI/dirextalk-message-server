---
name: direxio-storage-migration-guard
description: Design and validate Direxio durable state changes involving SQL schema, migrations, indexes, storage interfaces, read models, database selection, restart recovery, PostgreSQL/SQLite behavior, Docker database config, or persisted product state.
---

# Direxio Storage Migration Guard

Use this skill whenever behavior depends on durable state. Work at the owning package boundary, then check cross-package callers and runtime database selection.

## Storage Map

- Use `mcp__codegraph.codegraph_explore` for storage interfaces, constructors, callers, and tests. Use `rg` for migrations, SQL strings, docs, compose, and config keys.
- Identify the database family: setup config, roomserver, syncapi, userapi, mediaapi, federationapi, relayapi, appservice, or Direxio product read model.
- Check whether the owning package has both PostgreSQL and SQLite implementations.
- Check `internal/sqlutil` helpers and package tests for migration ordering, idempotency, indexes, and restart/reopen recovery.

## Direxio Rules

- Do not use memory-only state for persisted behavior.
- Update storage interface, PostgreSQL/SQLite implementations, tests, and callers together.
- Keep durable adapters behind one-way dependencies. If storage moves to a business subpackage, shared product records should come from a small domain package rather than importing the service package.
- Preserve restart behavior. Add reopen/restart coverage for user-visible state restored from storage.
- Keep migrations additive and idempotent. Destructive rewrites require explicit product intent and tests.
- Add indexes for introduced query patterns, not speculative indexes.
- Keep P2P/Direxio product read models as projections unless a domain rule explicitly makes the table source-of-truth state.
- If database selection changes, verify setup config, Docker config, bootstrap credentials paths, and fallback tests.
- Account deletion uses local deprovision rather than a schema migration: after critical Matrix leave/dissolve/deactivate steps succeed, the monolith deprovisioner clears every unique configured local database connection and schedules process shutdown. It must also overwrite the portal credentials file with a non-secret deprovision marker.

## Verification Targets

- Owning package storage tests for changed tables and interfaces.
- `go test ./internal/sqlutil -count=1` when migration helpers change.
- `go test ./setup -count=1` when database selection or monolith wiring changes.
- `docker compose -f docker-compose.p2p.yml config` or `docker compose -f docker-compose.p2p-dual.yml config` when compose database config changes.
- Restart or reopen tests when recovery matters.

Finish by selecting the final check set with `direxio-targeted-verification`.
