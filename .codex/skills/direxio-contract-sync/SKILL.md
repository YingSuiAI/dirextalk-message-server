---
name: direxio-contract-sync
description: Keep Direxio public contracts synchronized across the whole monolith. Use when changing HTTP routes, Matrix extension routes, Direxio body actions, request params, response fields, auth/public/Agent-token behavior, Postman examples, API change records, or manual examples.
---

# Direxio Contract Sync

Use this skill when a change can affect clients, agents, external nodes, operators, or docs. Contracts include HTTP paths, action envelopes, Matrix extension endpoints, auth behavior, Postman, and public documentation.

## Contract Surfaces

- Matrix-compatible routes stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Direxio product routes are:
  - `GET /_p2p/health`
  - `POST /_p2p/query`
  - `POST /_p2p/command`
  - `GET /_p2p/events`
  - `GET /.well-known/portal/owner.json`
- Direxio action requests use `{ "action": "...", "params": { ... } }`.
- Protected product actions require bearer access token. Agent token is accepted for fixed `mcp.*` actions and for `GET /_p2p/events` so agent gateways can passively receive `agent_room.message` events. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.
- `channels.public.join_result` is an internal node-to-node callback, not a normal client workflow entry.
- Agent permission management endpoints are removed. Do not reintroduce dynamic Agent action permissions unless the product contract changes explicitly.
- Ordinary message send, history, unread, search, and redaction use Matrix Client-Server APIs. Local history hiding uses `POST /_matrix/client/v1/io.direxio/rooms/{roomID}/local_delete`.

## Sync Checklist

1. Locate the route or action owner with `codebase-memory-mcp` and exact string search.
2. If adding, removing, renaming, or changing a request/response field, update focused tests and `docs/api-interface-change-record.md`.
3. If product action auth changes, update public-action allowlists, MCP Agent action allowlists, and authorization tests.
4. Do not reference removed first-party CLI modules or removed CLI agent skills in current docs.
5. If Postman examples change, keep `docs/postman/direxio-message-server.postman_collection.json` importable JSON.
6. If docs describe current behavior, update `docs/current-project-documentation.md`, `AGENTS.md`, and affected `.codex/skills/*/SKILL.md` together.
7. If cross-node behavior changes, update multi-node regression coverage in `scripts/p2p-three-node-regression.py` when practical, using the command syntax for the current platform.

## Field Rules

- Preserve compatibility unless the user explicitly accepts a breaking change and it is documented.
- Fail closed for missing visibility, join policy, comments settings, malformed Matrix IDs, malformed remote URLs, private/invite-only access, and untrusted local/private remote hosts outside trusted local dev.
- Keep remote room IDs, domains, ports, and `server_names` intact for remote-room joins.
- Product mutations that open conversations should return a stable conversation view plus an operation summary when the conversation record exists.
- Public lookup must not create placeholder product rows.

## Validation

Run `direxio-targeted-verification` after editing. Always include `python3 -m json.tool docs/postman/direxio-message-server.postman_collection.json >/dev/null` when the Postman collection changes and `git diff --check` for docs/examples.
