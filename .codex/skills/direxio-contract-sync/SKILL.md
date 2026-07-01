---
name: direxio-contract-sync
description: Keep Direxio public contracts synchronized when changing HTTP routes, Matrix extension routes, body actions, params, response fields, auth/public/Agent-token behavior, Postman examples, API change records, docs, or project-local contract rules.
---

# Direxio Contract Sync

Use this skill when a change can affect clients, agents, external nodes, operators, Postman, or current docs.

## Contract Surfaces

- Matrix-compatible routes stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Direxio product routes are `GET /_p2p/health`, `POST /_p2p/query`, `POST /_p2p/command`, `GET /_p2p/ws`, and `GET /.well-known/portal/owner.json`.
- Direxio action requests use `{ "action": "...", "params": { ... } }`.
- Protected product actions require bearer `access_token`. `agent_token` is accepted only for `agent.matrix_session.create` and fixed `mcp.*` HTTP actions; it cannot create realtime WS tickets. `GET /_p2p/ws` authenticates only short-lived single-use owner tickets created with the owner `access_token`. MCP actions and `agent.matrix_session.create` are HTTP body actions and must not be migrated into WS `client.request`.
- Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.
- `rooms.reactivate` and `channels.public.join_result` are internal node-to-node callbacks, not normal client workflow entries. `rooms.reactivate` restores a group/private-channel pending invite on a rebuilt member node after the owner node has sent a fresh Matrix invite; it must not silently join the user.
- Dynamic Agent permission actions are removed. Do not reintroduce Agent-token action management unless the product contract changes explicitly.
- `agent.config.get/update` owns Agent display config and MCP room blacklist state. Keep `avatar_url` and `mcp_blocked_room_ids` in the action response/request docs and examples. MCP handlers must not use rooms listed in `mcp_blocked_room_ids`: search filters them out, and direct access to blocked rooms returns 403.
- Owner-visible Agent online state comes from native Matrix room state in the real `agent_room_id`: event type `io.direxio.agent.status`, state key `@agent:<server>`, and content field `online`. The running local Agent bridge writes this state through its Matrix `@agent:<server>` session; the server must not drive `online=true` from Agent config, `/sync`, or WS lifetime. Startup/repair and `agent.config.update enabled=false` may write `online=false`. `sync.bootstrap` only returns `agent_room_id`; do not add `agent_online` back, do not emit `agent.presence`, and do not drive this state from Matrix `m.presence`.
- Direxio realtime sync uses owner `realtime.ws_ticket.create` plus `GET /_p2p/ws` as the primary non-MCP product-event, request/response, and session-state path. Clients send `client.lifecycle`, `client.focus`, and `client.ack`; lifecycle may include `state`, `hidden`, and `flags`, and focus may include `focused` and `flags`. The server timestamps session state with server time. Owner WS sessions send non-MCP product actions as `client.request` only after `server.ready`; if WS is not ready or disconnected at click time, owner clients should send the same body-action envelope through HTTP `/query` or `/command` immediately and let the realtime WS reconnect in the background. Transport failure before a response may also use owner HTTP fallback for safe repeated actions, but business errors returned by WS should not be retried over HTTP. `client.command` is only a compatibility alias to the WS response path. Agent room message intake, previews, edits, and final replies use Matrix Client-Server APIs, not `agent_room.message`, `client.agent_stream`, or `server.agent_stream`. A connected foreground, non-hidden WS session suppresses push only for the same focused room. Global Matrix account data `io.direxio.push.context` remains a 60-second server-clock fallback only when no fresh WS session exists. Do not add additional lifecycle/focus body actions unless the product contract changes explicitly.
- The real `agent_room_id` defaults to no system push for the portal owner through a room-level Matrix push rule with empty actions; existing explicit same-room push rules are preserved.
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
