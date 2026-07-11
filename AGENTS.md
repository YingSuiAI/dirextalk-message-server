# Dirextalk Message Server

This Go monolith is the backend contract authority for Dirextalk: Matrix homeserver APIs, ProductCore actions, policy, projection, storage, remote MCP, and runtime wiring are one system.

## Read By Scope

- Public ProductCore actions: `docs/product-action-contract.json` (generated from `p2p/serviceapi.ActionSpecs`) and `docs/api-interface-change-record.md`.
- Agent/MCP behavior: `docs/agent-mcp-current-contract.md`.
- Current architecture and product facts: `docs/current-project-documentation.md`.
- Stable releases: `.codex/skills/dirextalk-message-server-release/SKILL.md` and the target `release/vX.Y.Z.json`.
- Contract, Matrix state, projection, or storage changes: `.codex/skills/dirextalk-backend-contract-state-storage/SKILL.md`.

Read only the references needed by the touched behavior. Current code, generated contracts, and tests override stale narrative notes.

## Architecture

- `cmd/dirextalk-message-server` is the production entry point; `setup/monolith.go` wires Matrix and Dirextalk routes.
- `p2p/action_registry.go` and `p2p/service_*.go` own ProductCore orchestration.
- `internal/productpolicy` gates Matrix Client-Server writes into product rooms.
- `internal/dirextalktransport` defines product-originated Matrix writes; its Dendrite adapter performs room/member/state/message/redaction operations.
- `internal/dirextalkmatrix`, `internal/dirextalkprojection`, `internal/dirextalkstate`, and `internal/dirextalkdomain` own shared Matrix reads, projection helpers, state builders, and domain records.
- `p2p/consumer.go` and `p2p/projector.go` turn roomserver output into Dirextalk projections and product events.
- `internal/dirextalkmcp` is the shared registry and invocation layer for Native Agent tools and `POST /mcp`.

Trace changes through entry point, auth, policy, durable state, Matrix writes, roomserver output, projection, sync/federation visibility, and client contract. Keep behavior in the owning package and preserve existing public interfaces unless the task changes them.

## Stable Invariants

- Matrix APIs remain Matrix-native. Product APIs use the existing action envelope; `POST /mcp` is the standard Streamable HTTP exception.
- Ordinary messages, media, history, search, unread, read markers, and redaction stay on Matrix Client-Server APIs. Do not copy them into a second ProductCore message store.
- Product read models are projections unless a documented domain rule makes a table authoritative.
- Product-originated Matrix writes use the transport boundary; normal Matrix client writes remain subject to product policy.
- User-visible facts that must survive restart use durable PostgreSQL storage and migrations. PostgreSQL is the only supported server database; configuration must fail closed for SQLite/file DSNs.
- Product roles are `owner` and `member`. Matrix `m.room.member membership=join` is the final joined fact.
- Direct, group, channel, Agent, and system identities are real Matrix rooms. Typed UI behavior is derived from room state or timeline content rather than parallel room/message models.
- Channels are unified post+chat rooms with shared history for new rooms. Legacy `channel_type` metadata must not create separate current behavior.
- Remote public lookup validates Matrix IDs and uses the request-provided `remote_node_base_url`; never derive an outbound URL from a room ID.
- Keep secrets and bearer tokens out of storage records, logs, errors, command arguments, docs, and tests unless the contract explicitly stores a protected hash/reference.

## Change And Verification

1. Reproduce the affected path and identify the owning packages and contract source.
2. Add or update a focused regression/contract test when behavior changes.
3. Make the smallest change and update generated contracts or contract-critical docs in the same commit.
4. Run focused package tests first. Add projection, restart, federation, or multi-node coverage only when the changed path crosses those boundaries.
5. Format touched Go files, review `git diff`, run `git diff --check`, and commit only current-task changes.

Typical completion checks:

```text
go test ./p2p ./internal/productpolicy -count=1
go test ./internal/httputil ./setup -count=1
go build ./cmd/dirextalk-message-server
git diff --check
```

Use `gopls check` for touched Go files when available. Validate the relevant Compose files when setup, storage, image, or runtime configuration changes. Inherited Dendrite demo and upgrade-test packages remain behind their existing build tags.
