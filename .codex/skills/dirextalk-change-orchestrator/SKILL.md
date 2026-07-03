---
name: dirextalk-change-orchestrator
description: Global Dirextalk Message Server impact map before changing server behavior, routes, authorization, product policy, storage, Matrix event handling, sync/federation flows, docs, tests, Docker, runtime wiring, or project-local skills.
---

# Dirextalk Change Orchestrator

Treat this repository as one Dirextalk server, not separate P2P, Matrix, and Dendrite layers. Use this skill to build the project-specific impact map before edits that can cross packages or affect user-visible behavior.

## Impact Map

1. Restate the requested behavior and user-visible result.
2. Use `mcp__codegraph.codegraph_explore` for indexed Go symbols, routes, callers, callees, and blast radius. Use `rg` for exact strings, docs, JSON, shell, compose, and generated examples.
3. Map only the touched Dirextalk surfaces:
   - Startup and route wiring: `cmd/dirextalk-message-server`, `setup/monolith.go`, `setup/base`, `setup/config`.
   - HTTP/API routes: `clientapi/routing`, `federationapi/routing`, `mediaapi/routing`, `syncapi/routing`, `relayapi`, `p2p`.
   - Product facade and policy: `internal/productpolicy`, `p2p/action_registry.go`, `p2p/service_*.go`, `p2p/transport.go`, `p2p/transportapi`, `p2p/dendrite`.
   - Matrix event/state flow: `roomserver`, `syncapi`, `federationapi`, `userapi`, `appservice`, and Dirextalk projection in `p2p`.
   - Durable state: owning storage interfaces, migrations, tables, and restart/reopen tests.
   - Agent/MCP surface: `p2p/mcp`, MCP actions, `p2p/serviceapi/actions.go`, and Agent-token authorization.
4. Route to follow-up skills by changed surface:
   - Public route, body action, request/response, auth, Postman, or docs contract: `dirextalk-contract-sync`.
   - Matrix event, membership, profile, redaction, sync, federation, product policy, or projected read model: `dirextalk-event-state-tracer`.
   - SQL schema, migrations, indexes, database selection, restart recovery, or durable read models: `dirextalk-storage-migration-guard`.
   - Formatting, tests, build, JSON, compose, skill, or lint selection: `dirextalk-targeted-verification`.

## Dirextalk Guardrails

- Follow the full path from API entry to authorization, policy, storage, roomserver output, consumers/projection, sync/federation visibility, docs examples, and verification.
- Keep Matrix protocol APIs under their Matrix/Synapse/Dendrite namespaces. Keep Dirextalk product APIs behind the small body-action surface unless a current product contract explicitly says otherwise.
- Product-originated Matrix room/member/state/message/redaction writes go through `p2p.Transport`; Matrix client behavior uses the owning Client-Server route and `internal/productpolicy`.
- Persist behavior that must survive restart. Do not add memory-only state for durable product facts.
- Do not silently change client-visible fields, action names, routes, or auth rules; sync contracts and examples in the same change.
- Group product code by business responsibility. Add a new directory/package only when dependencies remain one-way; otherwise keep focused files in the owning package.
- Current Matrix-native product state is `m.room.create.content.type`, `io.dirextalk.room.profile`, `io.dirextalk.member.policy`, and `io.dirextalk.join_request`. Removed legacy product state must not be generated, read, or projected as current behavior.
- Ordinary Matrix timeline messages stay Matrix-native. Dirextalk read models store product projections, not a second ordinary-message source of truth.
- Remote public lookup is read-only and must use request-provided `remote_node_base_url`; never derive outbound remote-node URLs from Matrix room IDs.
- Do not mark channel membership as joined until Matrix membership has reached `join`.
