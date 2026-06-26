---
name: direxio-storage-migration-guard
description: Design and validate durable state changes across the Direxio server. Use before changing SQL schema, migrations, indexes, storage interfaces, read models, database selection, restart recovery, PostgreSQL/SQLite behavior, Docker database config, or state that must survive process restart.
---

# Direxio Storage Migration Guard

Use this skill whenever behavior depends on durable state. Work at the owning package boundary, but check cross-package callers and runtime database selection.

## Discovery

1. Use `codebase-memory-mcp` to locate storage interfaces, constructors, callers, and tests.
2. Identify the database family: global setup config, roomserver, syncapi, userapi, mediaapi, federationapi, relayapi, appservice, or Direxio product read model.
3. Check whether both PostgreSQL and SQLite implementations exist for the package.
4. Check migration helpers in `internal/sqlutil` and existing tests for migration ordering, idempotency, indexes, and restart recovery.

## Design Rules

- Do not use memory-only state for persisted behavior.
- Keep storage interfaces honest: update interface, implementation, tests, and callers together.
- Keep durable adapters behind one-way dependencies. If storage moves to a business subpackage, shared product records should come from a small domain package rather than importing the service package.
- Preserve restart behavior. Add regression coverage for state restored after reopening the store when the behavior is user-visible.
- Keep migrations additive and idempotent. Avoid destructive data rewrites unless explicitly required and tested.
- Add indexes for query patterns introduced by new behavior, not speculative indexes.
- Keep P2P/Direxio product read models as projections unless a domain rule explicitly makes the table a source of truth.
- If database selection changes, verify setup config, Docker config, bootstrap files, and tests that assert fallback behavior.

## Validation Targets

- Package storage tests for changed tables and interfaces.
- `go test ./internal/sqlutil -count=1` when migration helpers change.
- `go test ./setup -count=1` when database selection or monolith wiring changes.
- `docker compose -f docker-compose.p2p.yml config` or `docker compose -f docker-compose.p2p-dual.yml config` when compose database config changes.
- Restart or reopen tests when recovery matters.

Finish with `direxio-targeted-verification`.
