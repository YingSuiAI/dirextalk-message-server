---
name: dirextalk-backend-contract-state-storage
description: Use when backend work affects Dirextalk public contracts, body actions, auth, Matrix events or state, product projection, sync visibility, durable storage, migrations, reports, agent rooms, system rooms, groups, or channels.
---

# Dirextalk Backend Contract State Storage

## Contract Surfaces

- Matrix-compatible routes stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Product routes are `GET /_p2p/health`, `POST /_p2p/query`, `POST /_p2p/command`, `POST /mcp`, `GET /_p2p/ws`, and `GET /.well-known/portal/owner.json`.
- Product requests use `{ "action": "...", "params": { ... } }`.
- Protected product actions require owner bearer `access_token`.
- `POST /mcp` is the standard MCP Streamable HTTP endpoint and uses JSON-RPC `initialize`, `tools/list`, and `tools/call`, not the product action envelope.
- `agent_token` is accepted only for `agent.matrix_session.create` through the product body-action surface and `POST /mcp`.
- `POST /mcp` requires `Authorization: Bearer <agent_token>`, rejects owner access tokens and query-string tokens, validates `Origin`, returns 405 for GET/SSE while streaming is unused, and must not forward inbound bearer tokens downstream.
- Fixed `mcp.*` HTTP body actions are removed from `/_p2p/query` and `/_p2p/command`; keep `mcp.*` identifiers only as internal capability action IDs inside `internal/dirextalkmcp` and p2p adapters.
- `GET /_p2p/ws` authenticates only a short-lived single-use owner WS ticket.
- Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.
- MCP read actions use readable RFC3339/RFC3339Nano `from_time`/`to_time`, opaque stable snapshot `cursor`, and response fields such as `created_at`, `last_message_at`, and string `joined_at`; do not document or reintroduce old MCP `from_ts`/`to_ts`, `ts`, or `last_ts` fields.

When adding, removing, renaming, or changing fields/auth, update focused tests plus the contract-critical docs/Postman/project-local skills in the same change. Do not rewrite long-form audit or implementation documents for every small step; consolidate those at phase boundaries unless the user explicitly asks for immediate narrative updates.

## Matrix State And Timeline

- Product room type lives in `m.room.create.content.type`.
- Product metadata uses `io.dirextalk.room.profile`.
- Member policy uses `io.dirextalk.member.policy`.
- Public channel approval uses `io.dirextalk.join_request`.
- Matrix `m.room.member membership=join` is the joined fact.
- Group rooms use `m.room.history_visibility=joined`; channel rooms use `shared` for current unified post/chat behavior.
- Local delete hides for one user; recall/redaction propagates as Matrix redaction.
- Ordinary send, media, history, unread, search, and recall stay on Matrix Client-Server APIs.

For report/system/agent notifications, prefer normal Matrix timeline events in the durable room. Put business type in event content, for example `msg_type=report`, and let clients render cards from that content.

## Current Business Rules

- Owner-directed group/channel reports use ProductCore `reports.submit`; the owner node stores report records, sends a `msg_type=report` Matrix notice into `system_room_id`, and exposes that room through auth/bootstrap/conversations once messages exist.
- The real `agent_room_id` and `system_room_id` must not use legacy pseudo ids.
- Agent online state is native Matrix room state `io.dirextalk.agent.status` keyed by `@agent:<server>` with content field `online`.
- Native Agent runtime config is stored in portal Agent config JSON; old hidden `io.dirextalk.agent` plugin config is only a sanitized, idempotent startup migration source and must not reappear in plugin management surfaces.
- Native Agent built-in Dirextalk tools and `POST /mcp` share the `internal/dirextalkmcp` registry, schemas, pagination, room authorization, DTOs, errors, and invocation service. `p2p` adapts store/transport/history/profile/blocklist dependencies; do not duplicate MCP business logic in `nativeagent` or the MCP HTTP transport.
- Do not mirror agent messages through `agent_room.message`, `client.agent_stream`, or `server.agent_stream`.
- Channel approval must not report joined until requester-node Matrix join succeeds.
- Remote public lookup must use request-provided `remote_node_base_url`; do not derive outbound URLs from Matrix room IDs.

## Durable State

- Persist behavior that must survive restart. Do not add memory-only state for user-visible product facts.
- Update storage interfaces, PostgreSQL/SQLite implementations, migrations, tests, and callers together.
- Keep migrations additive and idempotent unless explicit product intent requires destructive reset.
- Add indexes only for introduced query patterns.
- Add restart/reopen coverage when recovery matters.
- Validate Docker/setup config when database selection or runtime storage changes.
