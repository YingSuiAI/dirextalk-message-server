---
name: direxio-change-orchestrator
description: Global first-pass impact map for this Direxio Message Server monolith before modifying server behavior, routes, authorization, product policy, storage, Matrix event handling, sync/federation flows, CLI integration, docs, tests, Docker, or runtime wiring. Use this instead of layer-specific P2P/Matrix/Direxio Message Server framing whenever a change can cross packages or affect user-visible behavior.
---

# Direxio Change Orchestrator

Use this skill before code or contract changes. Treat the repository as one Direxio server: Matrix protocol APIs, Direxio product actions, event projection, policy checks, storage, CLI tooling, docs, and deployment files are one system.

## First Pass

1. Restate the requested behavior and the user-visible outcome.
2. Use `codebase-memory-mcp` first for symbol, route, caller, callee, and architecture discovery. Run `index_repository` only if the project is not indexed. Use `rg` for exact strings, configs, docs, JSON, shell, and compose files.
3. Identify entry points:
   - Service startup and route wiring: `cmd/direxio-message-server`, `setup/monolith.go`, `setup/base`, `setup/config`.
   - HTTP/API routes: `clientapi/routing`, `federationapi/routing`, `mediaapi/routing`, `syncapi/routing`, `relayapi`, `p2p`.
   - Matrix event/state flow: `roomserver`, consumers in `syncapi`, `federationapi`, `userapi`, `appservice`, and Direxio projection in `p2p`.
   - Product policy and action facade: `internal/productpolicy`, `p2p/service.go`, `p2p/transport.go`, `p2p/dendrite_transport.go`.
   - Durable state: storage interfaces, migrations, tables, and tests in the owning package.
   - Agent/CLI surface: `cmd/direxio-cli`, `internal/agentclient`, `docs/agent-skills`.
4. Classify follow-up skills:
   - Public route, body action, request/response, auth, Postman, CLI, or docs contract: use `direxio-contract-sync`.
   - Matrix event, membership, profile, redaction, sync, federation, product policy, or projected read model: use `direxio-event-state-tracer`.
   - SQL schema, migrations, indexes, database selection, restart recovery, or durable read models: use `direxio-storage-migration-guard`.
   - Formatting, tests, build, JSON, compose, lint, or regression selection: use `direxio-targeted-verification`.

## Global Guardrails

- Do not split reasoning into isolated "P2P vs Matrix vs Direxio Message Server" buckets. Map the full path from API entry to durable state, event output, sync/federation visibility, CLI/docs examples, and verification.
- Keep Matrix protocol APIs under their existing Matrix/Synapse/Direxio Message Server namespaces. Keep Direxio product APIs behind the small body-action surface unless there is a documented compatibility reason.
- Do not bypass the established write path for room, membership, state, message, or redaction behavior. Product-originated Matrix writes go through `p2p.Transport`; Matrix client behavior uses the owning Client-Server route and product policy.
- Do not add memory-only state for behavior that must survive restart.
- Do not silently change client-visible request/response fields, action names, route behavior, auth rules, or CLI output.
- When splitting large product code, group by business responsibility. Use a new directory/package only when dependencies remain one-way; otherwise keep focused files in the owning package until a clear module seam exists.
- Preserve Matrix-native product state rules: current product state is based on `m.room.create.content.type`, `io.direxio.room.profile`, `io.direxio.member.policy`, and `io.direxio.join_request`. Do not reintroduce removed legacy product state.
- Ordinary Matrix timeline messages remain Matrix-native. Direxio read models store product projections, not a second ordinary-message source of truth.
- Remote public lookup is read-only and must use a request-provided `remote_node_base_url`; do not derive outbound URLs from Matrix room IDs.
- Do not mark channel membership as joined until Matrix membership has reached `join`.

## Implementation Loop

1. Build a small impact map with files, symbols, tests, docs, and runtime checks.
2. Edit the owning path with the smallest coherent change. Avoid opportunistic refactors outside the impact map.
3. Update contracts, migrations, docs, examples, and skills in the same change when behavior or workflow rules change.
4. Run `direxio-targeted-verification` and report executed commands, skipped checks, and residual risk.
