---
name: direxio-contract-sync
description: Keep Direxio public contracts synchronized when changing HTTP routes, Matrix extension routes, body actions, params, response fields, auth/public/Agent-token behavior, Postman examples, API change records, docs, or project-local contract rules.
---

# Direxio Contract Sync

Use this skill when a change can affect clients, agents, external nodes, operators, Postman, or current docs.

## Contract Surfaces

- Matrix-compatible routes stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Direxio product routes are `GET /_p2p/health`, `POST /_p2p/query`, `POST /_p2p/command`, `GET /_p2p/events`, and `GET /.well-known/portal/owner.json`.
- Direxio action requests use `{ "action": "...", "params": { ... } }`.
- Protected product actions require bearer `access_token`. `agent_token` is accepted only for fixed `mcp.*` actions and `GET /_p2p/events`.
- Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.
- `channels.public.join_result` is an internal node-to-node callback, not a normal client workflow entry.
- Dynamic Agent permission actions are removed. Do not reintroduce Agent-token action management unless the product contract changes explicitly.
- Ordinary send, history, unread, search, and redaction use Matrix Client-Server APIs. Local history hiding uses `POST /_matrix/client/v1/io.direxio/rooms/{roomID}/local_delete`.

## Sync Targets

- Locate route/action owners with `mcp__codegraph.codegraph_explore`; use `rg` for docs, JSON, examples, and exact strings.
- When adding/removing/renaming actions or changing fields/auth, update focused tests, `docs/api-interface-change-record.md`, `docs/current-project-documentation.md`, Postman, `AGENTS.md`, and affected `.codex/skills/*/SKILL.md`.
- When only correcting stale examples or skill text without behavior changes, do not add an API change-record entry.
- If product action auth changes, update `p2p/serviceapi/actions.go`, route authorization tests, MCP allowlist docs, and Postman auth notes together.
- Keep Postman importable JSON. Do not keep empty folders or removed action examples as current capabilities.
- Do not reference removed first-party CLI modules or removed CLI agent skills in current docs/examples.

## Field Rules

- This project is in initial development: choose the current optimal contract unless an explicit current business rule requires compatibility.
- Preserve explicit compatibility rules already documented in current behavior, such as `channels.update` ignoring `channel_type`.
- Fail closed for missing visibility, join policy, comments settings, malformed Matrix IDs, malformed remote URLs, private/invite-only access, and untrusted local/private remote hosts outside trusted local dev.
- Keep remote room IDs, domains, ports, and `server_names` intact for remote-room joins.
- Product mutations that open conversations should return a stable conversation view plus an operation summary when the conversation record exists.
- Public lookup must not create placeholder product rows.

Finish by selecting checks with `direxio-targeted-verification`.
