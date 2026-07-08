---
name: dirextalk-backend-change-orchestrator
description: Use when changing Dirextalk Message Server behavior, product actions, Matrix contracts, storage, projection, authorization, docs, verification, Docker runtime, or project-local skills.
---

# Dirextalk Backend Change Orchestrator

## Start Here

Read `AGENTS.md`, check `git status --short --branch`, and treat this repository as one Dirextalk server. Do not reason as isolated P2P, Matrix, or Dendrite layers.

Use CodeGraph before `rg` or file reads when the repository has `.codegraph/` and code understanding is needed. Use `rg` for exact strings, docs, JSON, compose files, and generated examples.

## Impact Map

Map only the touched surfaces:

- Startup and routes: `cmd/dirextalk-message-server`, `setup/monolith.go`, `setup/config`.
- Product API: `p2p/action_registry.go`, `p2p/service_*.go`, `p2p/transport.go`, `p2p/transportapi`, `p2p/dendrite`.
- Policy and Matrix writes: `internal/productpolicy`, Client-Server routes, roomserver input/output.
- Projection and sync: `p2p/consumer.go`, `p2p/projector.go`, sync/federation/userapi consumers.
- Durable state: storage interfaces, migrations, PostgreSQL/SQLite implementations, restart behavior.
- Agent/MCP: `internal/dirextalkmcp`, `p2p/mcp`, `p2p/routing_mcp.go`, `p2p/serviceapi/actions.go`, Agent-token authorization.

## Routing

- Use `dirextalk-backend-contract-state-storage` for API routes/actions, request/response fields, auth, Matrix events/state, product projections, storage, migrations, and restart-visible behavior.
- Use `dirextalk-backend-verification` before reporting completion.

## Backend Defaults

- Keep Dirextalk product APIs on the small body-action surface unless a current product rule requires a Matrix, well-known, or standard MCP route. The current MCP route exception is `POST /_p2p/mcp`.
- Product-originated Matrix room/member/state/message/redaction writes go through `p2p.Transport`.
- Matrix Client-Server writes must satisfy `internal/productpolicy`.
- Product read models are projections unless a domain rule explicitly makes storage source-of-truth.
- Ordinary Matrix timeline messages are not copied into a second product ordinary-message store.
- Agent and system notification rooms are real Matrix rooms. Prefer normal room/timeline events with typed content over new special sync models.
