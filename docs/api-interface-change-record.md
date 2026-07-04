# API Interface Change Record

Last updated: 2026-07-04

## 2026-07-04 Official Plugin Manager And Agent MCP Boundary

Added protected owner-only plugin management actions on the existing body-action surface:

- `plugins.catalog.list`
- `plugins.installed.list`
- `plugins.install`
- `plugins.enable`
- `plugins.disable`
- `plugins.uninstall`
- `plugins.config.get`
- `plugins.config.update`
- `plugins.job.get`
- `plugins.health`
- `plugins.logs.tail`
- `plugins.invoke`
- `plugins.invoke.stream`

These actions require owner `access_token`. `agent_token` cannot call plugin management or plugin invoke actions. The first official catalog entry is `io.dirextalk.agent`; plugin install/enable/disable/uninstall jobs are persisted and must use official catalog metadata whose Docker image belongs to the official `dirextalk` Docker Hub organization. Digest metadata is optional and is not required for first-version installs. Agent plugin config may declare non-secret runtime settings such as `provider`, `model`, `base_url`, `system_prompt`, `enabled_tools`, `skills`, `mcp_servers`, `skills_registry_url`, and `mcp_registry_url`. Model API keys, local model aliases, default model selection, and per-model parameters are client-local settings. Clients pass the selected `model_profile` with fields such as `provider`, `model`, `base_url`, `api_key`, `temperature`, `top_p`, `top_k`, `max_output_tokens`, `context_window`, and `reasoning_mode` only on `plugins.invoke` or WS `client.plugin_stream` calls. The message-server must not persist, return, or inject model API keys into the plugin container environment.

The official Agent plugin currently advertises owner-invoked actions for model discovery, registry discovery, structured config patch proposals, and context compression: `agent.models.list`, `agent.skills.list`, `agent.skills.registry.search`, `agent.mcp.servers.list`, `agent.mcp.registry.search`, `agent.config.propose_patch`, and `agent.context.compress`. The Agent container owns standard MCP client/server orchestration and ships package-manager launch support for third-party MCP servers installed from registry metadata (`npx` for npm packages and `uvx` for Python packages). This does not expand `agent_token` permissions; third-party MCP servers run inside the Agent plugin boundary, while the message-server continues to expose only the fixed backend `mcp.*` capability actions.

`plugins.invoke` calls an enabled official plugin over the first-version Docker HTTP runner and returns `{ "plugin_id", "action", "result" }`. Stream actions are not valid through HTTP invoke. `plugins.invoke.stream` is registered only to return `400 action requires websocket`; streaming runs through owner realtime WS frames:

```json
{
  "type": "client.plugin_stream",
  "id": "stream-1",
  "plugin_id": "io.dirextalk.agent",
  "action": "agent.chat.stream",
  "params": {
    "prompt": "Summarize this conversation"
  }
}
```

The server emits `server.plugin_stream.event` frames carrying `event` values such as `delta` and `done`, `server.plugin_stream.error` on failure, and accepts `client.plugin_stream.cancel` for cancellation. `client.request` remains unavailable for `plugins.*`.

The backend remains the Dirextalk capability boundary for Agent/MCP access: existing fixed `mcp.*` HTTP actions stay available for `agent_token` and keep owner-scoped access control, Matrix transport writes, product projections, and `mcp_blocked_room_ids` enforcement. `mcp.contacts.list` and `mcp.contacts.search` expose accepted direct contacts to local Agent tooling without requiring a room search fallback. Standard MCP protocol serving, external MCP client connections, skills, model/provider configuration, and Agent orchestration move to the official Agent plugin rather than being embedded in message-server. `POST /_p2p/mcp` is not a current backend route.

## 2026-07-03 Unified Channel Post+Chat

Channels are now a unified post+chat surface in one Matrix room. `channels.create` defaults missing or invalid `channel_type` to `post`, but current server behavior does not branch on legacy `chat` vs `post` values. New channel rooms, including existing-room channel bindings, write `m.room.history_visibility=shared`. Joined channel conversations expose post/comment/reaction capabilities according to room role and comments settings instead of `channel_type`.

Product post/comment/reaction events remain identified by Matrix content metadata such as `p2p_kind`; ordinary channel chat messages stay as Matrix timeline events and do not update post/comment/reaction projections. Conversation activity for channels is updated by ordinary chat messages, while post/comment projection events do not pollute ordinary conversation activity.

HTTP Push Gateway delivery is suppressed for all channel room events. This is Push Gateway suppression only; Matrix sync, room timelines, unread state, read markers, and local client navigation still operate normally.

## 2026-07-02 Owner Blocks

Added protected owner actions `blocks.add`, `blocks.list`, and `blocks.remove` for the user contact blacklist. `blocks.add` accepts `target_type: "contact"` with `peer_mxid`/`mxid`; group, channel, and room targets are not part of the current product contract. Each block stores a `display_name` and `avatar_url` display snapshot; when omitted, the server fills it from existing contact metadata or falls back to the target ID. `blocks.list` returns a `contacts` array for the user settings blacklist. `blocks.remove` cancels a contact block using the same identifiers.

When an owner tries to send a friend request to an already blocked contact, the action fails before Matrix writes with:

```json
{
  "error": "already blocked"
}
```

The HTTP/WS response status is `403`. These actions require owner `access_token`; they are not public actions and are not available to `agent_token`.

Inbound Matrix direct invites from blocked contacts are ignored by projection and do not appear as pending friend requests.

## 2026-07-01 Agent Config Avatar And MCP Room Blacklist

`agent.config.get` and `agent.config.update` now include two owner-managed fields:

```json
{
  "avatar_url": "mxc://example/agent",
  "mcp_blocked_room_ids": ["!room:example.com"]
}
```

`avatar_url` is a display-only Agent profile setting for clients. `mcp_blocked_room_ids` is a durable room blacklist under Agent config. `agent.config.update` replaces the blacklist with the supplied normalized list; omitted fields keep their previous values.

Fixed MCP actions remain HTTP-only and owner-scoped. Rooms in `mcp_blocked_room_ids` are not returned by `mcp.rooms.search`; direct MCP access to blocked rooms through ordinary message send/list, member list, channel post list, channel comment list, or channel comment creation is rejected with `403 room is blocked for MCP`.

## 2026-06-30 Owner HTTP Fallback For Product Actions

Logged-in client product actions now use ready-WS first instead of WS-only. Clients should use owner `GET /_p2p/ws` `client.request` only after the realtime transport has received `server.ready`. When WS is not ready or disconnected at click time, clients should send the same body-action envelope to `POST /_p2p/query` or `POST /_p2p/command` with `Authorization: Bearer <access_token>` immediately and let realtime WS reconnect in the background. Transport failure before a response may also use owner HTTP fallback for safe repeated actions.

Business errors returned by WS, such as permission or validation failures, should not be retried over HTTP. Clients should also de-duplicate identical in-flight user actions such as `contacts.requests.accept` or `groups.join` so duplicate taps do not send duplicate mutations or show duplicate success UI. If a WS request was already sent and the response is lost, clients should only HTTP-fallback actions that are safe to repeat, such as contact decisions, joins, read markers, and product queries.

`agent_token` permissions do not change: it remains limited to `agent.matrix_session.create` and fixed `mcp.*` HTTP actions. `agent.matrix_session.create` and fixed MCP actions remain HTTP-only and still return `action requires http` if sent over WS.

Realtime WS owner tickets now advertise `expires_in_ms: 120000` to tolerate mobile weak-network upgrade delays. A failed HTTP request to `GET /_p2p/ws?ticket=...` that never completes the WebSocket upgrade no longer consumes the ticket; accepted WebSocket upgrades remain single-use.

## 2026-06-30 Retained Room Reactivation For Rebuilt Members

Added internal public action `rooms.reactivate` for node-to-node recovery when a group or private-channel member node has been rebuilt and lost local product/Matrix projections while the owner node still retains the member in the Matrix room. It is not a normal client workflow entry.

When `groups.invite` or `channels.invite` sees Matrix report the target user as already joined, the owner node removes that stale joined membership, sends a new Matrix invite, and calls the target node `rooms.reactivate` instead of treating the invite as failed. The target node records an invite/pending room card only; it does not silently join. The user must still confirm by calling `groups.join` or `channels.join`, and joined state is recorded only after Matrix join succeeds. Public channels continue to recover through `channels.public.join_request` and the normal open/approval flow; if the owner node still has stale joined membership for that public requester, it removes it and sends the fresh Matrix invite required before returning `channels.public.join_result`.

For rebuilt direct-contact nodes, `contacts.request` still first asks the retained peer to re-invite the old accepted direct room. If the retained room cannot be rejoined because the rebuilt node lost its old Matrix room/key state, including a missing local room version after database loss, the requester creates a replacement direct request room. The retained peer accepts that replacement only from the real Matrix invite sender and preserves local contact remarks; old direct-room history is not copied into the replacement.

## 2026-06-30 Contact Re-Request Replacement Room

When both sides of a direct contact have left the retained old direct room, or the peer node no longer retains the old relationship, a new `contacts.request` creates a replacement direct request room instead of binding the pending request to the old room. The returned contact `room_id` may therefore differ from the previous direct room. Clients should use the latest `room_id` from `contacts.request`, `contacts.list`, `sync.bootstrap.contacts`, or contact mutation responses when accepting or opening the conversation.

For historical pending requests that still point at an unrejoinable old direct room, `contacts.requests.accept` falls back to creating a replacement direct room and returns the accepted contact with the new `room_id`.

## 2026-06-30 Contact Display Name Override

`contacts.update` now marks the supplied `display_name` as a local contact remark. Contact records returned from `contacts.update`, `contacts.list`, and `sync.bootstrap.contacts` may include `display_name_override: true` when the displayed name is owner-managed.

Remote Matrix member profile updates still refresh peer avatar metadata, but they no longer overwrite an accepted contact's `display_name` while `display_name_override` is true. Contacts without a local override keep the previous Matrix-native behavior and continue to follow the peer's latest Matrix member display name.

## 2026-06-30 Agent Bridge Transport Returns To Matrix

Agent bridge online display remains Matrix-native room state in the real `agent_room_id`: event type `io.dirextalk.agent.status`, state key `@agent:<server>`, and content field `online`. The running local bridge writes `online=true/false` through its `@agent:<server>` Matrix session. The server no longer treats `agent.config.enabled=true` or an agent-token WS session as online; startup/agents-room repair and `agent.config.update enabled=false` only publish `online=false` as a fallback.

`agent_token` no longer creates realtime WS tickets. `realtime.ws_ticket.create` is owner-token only; `agent_token` remains limited to `agent.matrix_session.create` and fixed `mcp.*` HTTP actions.

`agent.matrix_session.create` remains a retained HTTP body action and may be called with either owner `access_token` or `agent_token`. It returns a Matrix Client-Server session for the local `@agent:<server>` bridge user so dirextalk-connect can bootstrap Matrix-native Agent room sync/send/edit without owner credentials. It must not be migrated into Product WS and must not evict owner devices.

Agent room messages, previews, edits, and final replies are transported through Matrix Client-Server APIs as `@agent:<server>`. `agent_room.message`, `client.agent_stream`, and `server.agent_stream` are no longer current protocol frames/events.

No response fields change: `sync.bootstrap` still returns only `agent_room_id` for Agent room discovery and does not return `agent_online`; `agent.status`/`agents.status` remain removed.

## 2026-06-30 MCP HTTP Boundary And WS Client State Flags

Fixed MCP actions remain HTTP body actions on `POST /_p2p/query` or `POST /_p2p/command`; they are not migrated into WS `client.request`. Owner `access_token` and fixed `agent_token` may call the existing MCP allowlist over HTTP. `agent.matrix_session.create` is also HTTP-only. If an owner or agent WS session sends a `client.request` for `mcp.*` or `agent.matrix_session.create`, the server returns:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "mcp.rooms.search",
  "ok": false,
  "status": 400,
  "error": "action requires http"
}
```

WS lifecycle and focus frames now accept extra client-state fields for future push decisions while preserving the existing `foreground` and `room_id` fields:

```json
{
  "type": "client.lifecycle",
  "foreground": false,
  "state": "hidden",
  "hidden": true,
  "flags": {
    "hidden": true,
    "background": true
  }
}
```

```json
{
  "type": "client.focus",
  "room_id": "!room:server",
  "focused": true,
  "flags": {
    "focused": true
  }
}
```

Push suppression requires a fresh foreground WS session that is not hidden and has the same focused room as the push candidate. Hidden/background/disconnected/expired/different-room state keeps normal push behavior.

## 2026-06-30 WS Product API Full Migration

Logged-in Dirextalk client/product actions now use `GET /_p2p/ws` request/response frames instead of HTTP body-action calls. HTTP `/query` and `/command` remain for portal bootstrap/auth/status/password, `agent.matrix_session.create`, fixed MCP actions, `realtime.ws_ticket.create`, and node-to-node public/callback actions.

This WS-only HTTP rejection rule was superseded later on 2026-06-30 by the owner HTTP fallback contract above. Current clients are WS-first, not WS-only.

Client request frame:

```json
{
  "type": "client.request",
  "id": "req-1",
  "action": "contacts.list",
  "params": {}
}
```

Successful response frame:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "contacts.list",
  "ok": true,
  "result": {}
}
```

Error response frame:

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "contacts.list",
  "ok": false,
  "status": 401,
  "error": "M_UNKNOWN_TOKEN"
}
```

`client.command` is retained only as a one-release compatibility alias and maps to the same `server.response` path. New Flutter code sends only `client.request`.

`GET /_p2p/events` is removed. The P2P outbox remains durable because WS `server.event` replay and cursor recovery still use it. Cursor retention gaps are reported only through WS `server.cursor_reset`; clients must recover by issuing `sync.bootstrap` over WS.

Owner WS sessions may call protected logged-in product actions except `realtime.ws_ticket.create`, `agent.matrix_session.create`, and fixed MCP actions. Agent-token callers cannot create WS sessions; Agent bridge bootstrap and MCP actions stay on HTTP. Matrix Client-Server remains the protocol source for ordinary timeline, media, history, search, redaction, local delete, and Agent bridge room traffic.

HTTP `/query` and `/command` now return an explicit error for non-retained logged-in client actions:

```json
{
  "error": "action requires websocket"
}
```

## 2026-06-30 Transitional Realtime WS Commands And Agent Stream Frames

This transitional contract was superseded later the same day by the WS Product API full migration above. During the transition, `GET /_p2p/ws` accepted owner-session `client.command` frames for lightweight product commands. The initial allowlist was:

- `sync.read_marker`
- `channels.read_marker`

Frame shape:

```json
{
  "type": "client.command",
  "id": "cmd-1",
  "action": "sync.read_marker",
  "params": {
    "room_id": "!room:server",
    "event_id": "$event",
    "origin_server_ts": 1710000000000
  }
}
```

Successful commands returned `server.command_result` with `id`, `action`, and `result`. Validation, auth, and action errors returned `server.command_error` with `id`, `status`, and `error`. Current servers map `client.command` to the `client.request` handler and return `server.response`; new clients must use `client.request`. Agent-token WS sessions cannot call owner commands.

This transitional agent stream contract was removed later the same day. Current Agent bridge previews and final replies use Matrix Client-Server messages/edits from `@agent:<server>`; current clients must not emit agent stream WS frames and current servers must not expose Agent bridge traffic on Product WS.

## 2026-06-29 WebSocket Realtime Sync

Added protected body action `realtime.ws_ticket.create`, normally sent to `POST /_p2p/query` with an empty `params` object:

```json
{
  "action": "realtime.ws_ticket.create",
  "params": {}
}
```

The action accepts owner `access_token` only. It returns:

```json
{
  "ticket": "ws_ticket_...",
  "expires_in_ms": 120000
}
```

The ticket is server-local, short-lived, and single-use. It is consumed only after `GET /_p2p/ws?ticket=<ticket>` completes WebSocket upgrade. The WS route does not accept bearer tokens directly.

The first client text frame must be `client.hello` with optional `since`, `client`, and `platform` fields. Subsequent client frames are:

- `client.lifecycle`: `{ "foreground": true|false }`
- `client.focus`: `{ "room_id": "!room:server" }`, or empty `room_id` to clear focus
- `client.ack`: `{ "seq": 123 }`
- `client.ping`

Server frames are:

- `server.ready`
- `server.event` with the existing P2P event payload in `event`
- `server.cursor_reset` with the same recovery payload shape as the SSE `p2p.cursor_reset` event
- `server.pong`
- `server.error`

Owner WS sessions receive the normal product event stream. The initial implementation also allowed agent-token WS/SSE streams for `agent_room.message`; that path was later removed in favor of Matrix Client-Server bridge sync/send/edit.

Push suppression now prefers fresh WS session state. A connected foreground WS session suppresses unread notification insertion and HTTP push gateway delivery only when its focused room matches the room that produced the push candidate. Background, disconnected, expired, no-focus, or different-room state keeps normal background push behavior. The server timestamps and expires WS session state with server time; clients do not send expiry timestamps.

## 2026-06-29 Matrix Account-Data Foreground Fallback And Agent Room Defaults

Dirextalk clients that have not established a fresh WS session may still suppress foreground system pushes by writing global Matrix account data type `io.dirextalk.push.context` through the existing Matrix account data route. The expected body is:

```json
{
  "foreground": true
}
```

The Matrix account data write path stamps foreground writes with a server-clock 60-second expiry. While the stamped foreground state is fresh and no fresh WS session exists for the user, the userapi roomserver consumer does not create an unread notification row and does not call the HTTP push gateway for matching Matrix push-rule notifications. Missing, malformed, expired, or `foreground=false` context fails open and keeps normal background push behavior. This is a migration fallback only; the server does not infer foreground/background from `/sync`, read receipts, or pusher registration.

Clients should prefer WS `client.lifecycle` and `client.focus`. During migration, clients may continue refreshing this account data every 30 seconds with `{"foreground": true}` and write `{"foreground": false}` when entering background; if that write is missed, the previous foreground state naturally expires after the server-stamped 60 seconds.

Backend startup now also ensures the portal owner has a room-level Matrix push rule for the real `agent_room_id` with empty actions, so new or repaired agents rooms default to no system push. Existing explicit room push rules for the same room are preserved.

## 2026-06-29 P2P Reports Submit Removed

Removed `reports.submit` from the message-server P2P action surface. User-facing report submission remains on the signed imadmin public API, so this server no longer registers the P2P report action or persists P2P report rows.

## 2026-06-29 P2P Event Cursor Reset Signal

`GET /_p2p/events` now detects a non-zero `since` cursor that is older than the retained `p2p_events` window. The stream stays HTTP 200 and replays retained events, but it first emits an SSE control event `event: p2p.cursor_reset` without advancing the SSE event id.

The control payload contains `type`, `since`, `min_seq`, `max_seq`, `count`, and `recovery: "bootstrap_required"`. The response also sets `X-Dirextalk-P2P-Events-Cursor-Reset: true`, `X-Dirextalk-P2P-Events-Min-Seq`, `X-Dirextalk-P2P-Events-Max-Seq`, and `X-Dirextalk-P2P-Events-Count` before streaming begins.

Clients should treat this as a product cache gap: clear local product projections, call `sync.bootstrap` once, persist the newest handled event `seq`, and then continue normal WS delta consumption. SSE fallback clients continue with `GET /_p2p/events?since=<seq>`.

## 2026-06-29 MCP Room Member Identities

Added protected MCP action `mcp.room_members.list` on `POST /_p2p/query`. Owner `access_token` and fixed MCP `agent_token` may call it. The action accepts `room_id` or `channel_id`, optional `status`/`membership`, optional `role`, and optional `limit`; it returns `room_id`, `name`, `count`, and concise member identities with `user_id`, `user_mxid`, `localpart`, `domain`, `display_name`, `avatar_url`, `membership`, `role`, and `joined_at`.

`mcp.room_members.list` is owner-scoped and only reads known Dirextalk product rooms or conversations. It may enrich stale product projections from current Matrix `m.room.member` state and Matrix profile fallback data, but it rejects unknown room IDs instead of exposing arbitrary roomserver state through the MCP surface.

`mcp.messages.list` message summaries now expose sender identity fields: `sender_mxid`, `sender_display_name`, `sender_domain`, and `sender_localpart`. The legacy `sender` field is preserved and is upgraded to a readable display name when Matrix member/profile data is available.

`mcp.rooms.search` may use current Matrix member state to display fresher group/channel member counts when product read-model counts are stale.

## 2026-06-27 MCP Owner-Scoped Message History

MCP actions remain a fixed `agent_token` allowlist, but their product behavior is owner-scoped: room search, default ordinary message send, ordinary message list, channel post/comment list, and channel comment create operate from the portal owner view instead of exposing the local Agent Matrix account as an independent product user.

`mcp.messages.list` now reuses the current owner `access_token` for Matrix history reads. It does not call `agent.matrix_session.create`, does not create a `DIREXTALK_MATRIX_HISTORY` device, and does not refresh the portal owner's Matrix session, so MCP history reads cannot evict the owner's phone or browser session.

Default owner-scoped `mcp.messages.send` now rejects the configured `agent_room_id`. Agent-room replies remain supported only through the internal gateway marker path (`agent_gateway=true` or `gateway_source`), where the local `@agent:<server>` user sends the reply and marks the event to prevent gateway loops.

## 2026-06-26 Agent Matrix Session Identity

`agent.matrix_session.create` now creates and returns a Matrix Client-Server session for the local agent user `@agent:<server>` instead of the portal owner. The response fields remain `access_token`, `device_id`, `user_id`, and `homeserver`; `user_id` is now the local agent MXID. Current servers accept either owner `access_token` or fixed `agent_token` for this HTTP-only bootstrap action.

The helper still uses `revokeExistingDevices=false`, so creating a cc-connect or local gateway Matrix session does not evict the portal owner's phone or browser sessions.

## 2026-06-26 Agent Matrix Room State Status

Owner clients now receive Agent bridge online state from native Matrix room state in the real `agent_room_id`: event type `io.dirextalk.agent.status`, state key `@agent:<server>`, and content field `online`.

The server writes this state when creating or repairing the agents room and when `agent.config.update` changes `enabled`. This was later narrowed: the server only writes `online=false` fallbacks, while the running local bridge writes true/false through Matrix. `sync.bootstrap` still returns the real `agent_room_id` so clients can locate the room, but it no longer returns `agent_online` or any `agent_presence` mirror. `agent.status` and `agents.status` are removed.

Matrix `m.presence` is not part of the Agent online contract, and Dirextalk monolith startup no longer enables Matrix outbound presence for this path. New generated, sample, and Helm configs default both inbound and outbound presence to `false`.

## 2026-06-25 Agent Token Event Stream Access

`GET /_p2p/events` previously accepted bearer `agent_token` as well as owner `access_token` as a narrow passive gateway exception. This path was later removed with SSE and the Agent bridge returned to Matrix Client-Server transport.

Non-MCP protected body actions still reject `agent_token` except the HTTP-only `agent.matrix_session.create` bridge bootstrap action; the fixed MCP action allowlist is unchanged.

## 2026-06-25 Immutable Channel Type

`channels.update` now ignores `channel_type`. Channel type is creation-time metadata and cannot be changed after a channel exists. Requests that include `channel_type` continue to apply other mutable fields but leave the stored `channel_type` unchanged.

Clients may send `channel_type` only in `channels.create`; missing or invalid values now default to `post`. Since 2026-07-03, all new channel rooms get shared Matrix history visibility at creation or when binding an existing room as a channel, regardless of legacy `channel_type`.

## 2026-06-25 Agent Token And CLI Cleanup

Agent-token dynamic permission management is removed. `apis.list` and `apis.status` are no longer P2P actions and calls to those action names return `unknown action`.

Protected product actions require bearer `access_token`. `agent_token` is accepted only for `agent.matrix_session.create` and fixed MCP actions: `mcp.rooms.search`, `mcp.messages.send`, `mcp.messages.list`, `mcp.channel_posts.list`, `mcp.channel_comments.list`, and `mcp.channel_comments.create`. `GET /_p2p/events` was a route-level exception for passive gateway listening at the time and was later removed; other protected body actions reject `agent_token`.

The first-party CLI module and its helper package are removed: `cmd/dirextalk-cli`, `internal/agentclient`, CLI build scripts, CLI agent-skill docs, and the project-local `dirextalk-cli` Codex skill.

## 2026-06-25 Matrix Push Gateway Metadata

Matrix event pushes sent to HTTP push gateways now include optional Dirextalk display/routing metadata when the room has Dirextalk product room state. Normal direct and group message pushes can include `notification.title`, `notification.push_type=message`, `notification.room_id`, `notification.event_id`, and short `notification.room_type` (`direct` or `group`). The gateway owns the visible body text and sets it to `Send you a new message`.

Channel rooms (`notification.room_type=channel`) are not sent to HTTP push gateways. Matrix `m.call.invite` events in Dirextalk rooms use `push_type=call` and add `call_id` plus `call_kind=voice`; product `calls.create` / `calls.incoming` actions remain P2P event/call-record flows unless represented as Matrix call invite events.

## 2026-06-24 Portal Single-Device Login

`portal.bootstrap`, `portal.auth`, and `portal.password` now create an exclusive Matrix device session for the portal owner. After the new session is created, the server deletes the owner's other Matrix devices while preserving the current `device_id`, so previous phones receive Matrix `M_UNKNOWN_TOKEN` on later authenticated requests and must ask the user to log in manually.

`agent.matrix_session.create` remains an internal Matrix session helper and does not evict the portal user's phone session. As of 2026-06-26, it returns a session for the local `@agent:<server>` user. Current servers accept either owner `access_token` or fixed `agent_token` for this HTTP-only bridge bootstrap action.

## 2026-06-24 User Public Channel Lookup

`users.public_channels` now returns only public channels owned by the target user. Public channels where the target user is only a normal member are no longer included in the "user's channels" list.

`users.public_channels` also accepts optional `remote_node_base_url` and forwards the public query to that owner node, matching remote public channel discovery flows. The forwarded request strips `remote_node_base_url` before reaching the target node.

## 2026-06-24 Channel Room Projection Guard

Matrix room state is now treated as a channel projection source only when `io.dirextalk.room.profile.room_type` is explicitly `io.dirextalk.room.channel` and `channel_id` is an explicit product channel id. Empty profiles, group/direct room profiles, missing `channel_id`, and Matrix-room-id-shaped `channel_id` values are ignored by channel refresh logic.

`groups.join` no longer calls the channel room refresh path after Matrix join. Group member refresh still runs for the joined group, but it cannot create or update a `channels` read-model row. This prevents group chats with empty profile state from appearing in `channels.list` or `sync.bootstrap.channels`.

## 2026-06-24 Channel Reaction History Snapshots

`channels.my_reactions` still returns `{ "reactions": [...] }`, but each item is now a display history snapshot object instead of a bare reaction row. The item contains:

- `reaction`: the original reaction record with `target_type`, `target_id`, `channel_id`, `post_id`, `comment_id`, `reaction`, `user_id`, `active`, and `created_at`.
- `channel`: the current channel snapshot when available, including `name`, `avatar_url`, `channel_type`, `member_count`, and normal channel metadata.
- `post`: the parent post snapshot when available, enriched with comment/reaction counts and `reacted_by_me`.
- `comment`: the comment snapshot for comment reactions when available, enriched with reaction count and `reacted_by_me`.

Clients must not synthesize fake channel/post display data from a bare reaction row. If a snapshot is missing, show an unavailable or syncing state instead of fallback labels such as `频道`, `文字`, or `频道帖子`.

`channels.public.get`, `channels.public.search`, and `users.public_channels` refresh public channel `member_count`/`pending_join_count` from persisted ProductCore membership before returning a channel when membership rows are available. This keeps public detail and public list views aligned with the owner node's joined member facts.

## 2026-06-23 Realtime Call Lifecycle

`calls.event` now accepts `rejected` in addition to `connected`, `ended`, `missed`, and `failed`. Call records persist `answered_at`, `ended_at`, `ended_by_mxid`, `end_reason`, and `duration_ms` in `p2p_calls`, so call start/end timing survives restart.

Every `calls.create`, `calls.incoming`, and `calls.event` write appends a `call.changed` event to `GET /_p2p/events`. The event payload contains the current call record under `payload.call`, allowing clients to update active call UI immediately when the other party rejects or hangs up.

Terminal call states are not reopened by later stale `calls.create`, `calls.incoming`, or non-terminal `calls.event` writes with the same `call_id`. Clients that arrive late after `missed`, `ended`, `rejected`, or `failed` receive the terminal snapshot and must not join that call.

## 2026-06-23 Agents Room Gateway

This section records the original gateway behavior from June 23. Current behavior supersedes it: Agent bridge traffic no longer uses SSE/P2P outbox events and is transported through Matrix Client-Server sync/send/edit as `@agent:<server>`.

Backend startup now creates a real private Matrix agents room when the stored `agent_room_id` is empty or still uses the legacy pseudo form `!agent:<server>`. The real room id is persisted in portal state and written to the bootstrap credentials file as `agent_room_id`. The room contains the portal owner and the local agent user `@agent:<server>`; existing real agents rooms are repaired on startup by joining the local agent user if needed.

`portal.bootstrap`, `portal.auth`, and `sync.bootstrap` expose the current real `agent_room_id` so app and gateway clients can restore the Agent conversation from either login/session metadata or first-screen metadata.

`GET /_p2p/events` can now emit `agent_room.message` for ordinary `m.room.message` events in the configured agents room only. Payload fields are `room_id`, `event_id`, `sender_mxid`, `body`, `msgtype`, and `origin_server_ts`. Ordinary messages in other non-product rooms still do not produce P2P events or P2P message records.

`mcp.messages.send` accepts internal optional gateway marker params, including `agent_gateway=true` and `gateway_source`. Marked replies are sent by the local agent user, written as Matrix messages with `io.dirextalk.agent_gateway` metadata, and are not re-emitted as inbound `agent_room.message` events, preventing gateway reply loops. `mcp.messages.list` returns the agents room name as `Agents` and displays messages from `@agent:<server>` using the configured agent `display_name`.

## 2026-06-23 Channel Join Request Approval Retry

`channels.join_request.approve` now treats an existing `join_failed` or `approved` channel join request as retryable approval state instead of returning `404 join request not found`. This lets a channel owner retry approval after the requester-node `channels.public.join_result` callback temporarily failed. Ordinary channel invites still are not accepted by the join-request approval action.

## 2026-06-22 Dirextalk Local MCP Backend Actions

Added six protected MCP-oriented P2P actions for the local Dirextalk MCP adapter:

- `mcp.rooms.search` on `POST /_p2p/query`
- `mcp.messages.list` on `POST /_p2p/query`
- `mcp.channel_posts.list` on `POST /_p2p/query`
- `mcp.channel_comments.list` on `POST /_p2p/query`
- `mcp.messages.send` on `POST /_p2p/command`
- `mcp.channel_comments.create` on `POST /_p2p/command`

All six require bearer auth. Owner `access_token` can call them, and `agent_token` is accepted for these fixed MCP actions. Current servers also allow `agent_token` for the HTTP-only `agent.matrix_session.create` bootstrap action. The response contracts are intentionally concise for MCP tooling and do not expose full `conversationView`, `channelPostRecord`, `channelCommentRecord`, Matrix event payloads, projection state, capability maps, or internal Matrix tokens.

Ordinary MCP message send/list remains separate from channel post/comment product content. `mcp.messages.send` writes a plain `m.room.message` without `p2p_kind`; `mcp.messages.list` reads ordinary Matrix timeline messages through a server-side Matrix reader and filters out events carrying product `p2p_kind`. No P2P ordinary-message store was added.

The live P2P body-action count is now 91.

## 2026-07-03 Account Deletion

Added protected owner HTTP-only command `portal.account.delete` on `POST /_p2p/command`.

Request:

```json
{
  "action": "portal.account.delete",
  "params": {
    "confirm": "delete_account"
  }
}
```

Behavior:

- Requires owner `access_token`; `agent_token` is rejected.
- Cannot be called through `GET /_p2p/ws` `client.request`; WS returns `action requires http`.
- Before database reset, the server publishes `io.dirextalk.room.profile` direct-room account-deleted dissolve state for accepted direct contacts so peers hide the deleted account, leaves accepted direct-contact rooms, dissolves groups/channels owned by the portal owner, leaves groups/channels where the owner is only a member, and deactivates local owner/agent Matrix accounts.
- If a critical leave/dissolve/deactivation step fails, the server returns an error and does not clear databases.
- On success, the server writes a non-secret deprovision marker to the portal credentials file, clears configured local databases, clears in-memory product/session state, and schedules local message-server shutdown. It does not destroy AWS/cloud instances.

Response includes `status: "deprovisioned"`, operation counts such as `contacts_left`, `groups_dissolved`, `channels_dissolved`, `accounts_deactivated`, and `database_reset: true`.

## 2026-06-22 Matrix-First Cleanup

This pass removes the remaining ambiguous compatibility surface from current code, examples, skills, and Postman.

Breaking removals and contract changes:

- `portal.setup` is no longer a P2P action. Portal initialization is automatic; clients use `portal.bootstrap`, `portal.auth`, `portal.status`, and `portal.password`.
- `P2P_BOOTSTRAP_CREDENTIALS_FILE` is no longer a compatibility alias. Use `P2P_PORTAL_CREDENTIALS_FILE`.
- Removed legacy Matrix product state is no longer generated, read, or projected. Current product state is `io.dirextalk.room.profile`, `io.dirextalk.member.policy`, and `io.dirextalk.join_request`.
- Public channel approval no longer exposes Matrix invite as the product workflow. Approval writes `io.dirextalk.join_request status=approved`; the requester homeserver performs Matrix join.
- New public internal action `channels.public.join_result` carries owner-node approval results to the requester node. Params: `room_id`, `channel_id`, `user_id`, `status`, `reason`, `server_names`, and `request_id`.
- Public channel join response status is one of `pending`, `rejected`, `approved`, `joining`, `joined`, or `join_failed`.
- Added protected action `agent.matrix_session.create` on `POST /_p2p/command`. It initially required bearer `access_token`; current servers accept owner `access_token` or `agent_token`. It returns a Matrix Client-Server session: `access_token`, `device_id`, `user_id`, and `homeserver`.
- `portal.bootstrap`, `portal.auth`, and `portal.password` return one setup state field: `initialized`. It is `false` while the generated initial password is still in use and becomes `true` after `portal.password` changes that password. Clients should store `access_token` and route by `initialized`; profile completion is independent.

The live P2P body-action count is 91. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.

## Current Pass

This pass completes the Matrix-only ordinary message migration for Dirextalk product rooms. There is now one ordinary message source of truth: Matrix Client-Server event storage and timelines. P2P product APIs keep product metadata, contact/group/channel state, channel post/comment projections, calls, favorites, follows, Agent configuration, and bootstrap metadata.

Breaking removals from the P2P body-action surface:

- `sync.messages`
- `sync.unread`
- `search`
- `rooms.send`
- `rooms.send_media`
- `rooms.messages.delete`
- `rooms.messages.delete_batch`
- `rooms.messages.delete_range`
- `rooms.messages.recall`
- `contacts.export`
- `contacts.download`
- `contacts.import`

The removed actions are absent from `p2p.Service.Handle`, Postman, and the dual-node smoke business flow. Calls to those names are treated as unknown P2P actions. Clients must not use them as deprecated compatibility paths.

## Matrix Message Contract

Ordinary private chat, group chat, and channel chat use Matrix Client-Server APIs:

- Send text/media: `PUT /_matrix/client/v3/rooms/{roomID}/send/m.room.message/{txnID}`
- Incremental sync and unread data: `GET /_matrix/client/v3/sync`
- Offline/history reads: `GET /_matrix/client/v3/rooms/{roomID}/messages`
- Search: `POST /_matrix/client/v3/search`
- Distributed recall: Matrix redaction routes
- Per-user local hide/clear: `POST /_matrix/client/v1/io.dirextalk/rooms/{roomID}/local_delete`

`local_delete` request forms:

```json
{ "event_ids": ["$event:server"] }
```

```json
{ "clear": true }
```

`event_ids` hides specific Matrix events from the requesting user's Matrix read paths. `clear=true` hides room events through the current sync stream position. Neither form sends a Matrix redaction or changes other users' history.

The local hide state is persisted in syncapi storage and filtered from:

- `/sync`
- `/rooms/{roomID}/messages`
- `/rooms/{roomID}/event/{eventID}`
- `/rooms/{roomID}/context/{eventID}`
- `/rooms/{roomID}/relations/...`
- `/search`

## Product Room Classification

Room classification remains a product metadata concern and is not rebuilt from message history:

- Direct/private chats: `contacts.list`, `sync.bootstrap.contacts`, pending friend requests, and Dirextalk direct room profile state.
- Groups: `groups.list`, `sync.bootstrap.groups`, pending group invites, and `io.dirextalk.room.profile` with group type.
- Channels: `channels.list`, `sync.bootstrap.channels`, pending channel notices, public channel actions, and `io.dirextalk.room.profile` with channel type.

`sync.bootstrap.rooms` was removed. `sync.bootstrap` now returns product metadata sections only; clients should combine those sections with Matrix room timelines from `/sync` instead of consuming a P2P-derived room list.

## Channel Posts And Comments

Channel post/comment product content still uses Matrix events, but carries product classification:

- `p2p_kind=channel_post` projects to `p2p_channel_posts`.
- `p2p_kind=channel_comment` projects to `p2p_channel_comments`.
- Matrix ProductPolicy enforces channel owner/comment rules before write. ProductCore group/channel roles are owner/member only.
- Channel post/comment recall uses Matrix redaction and removes the product projection.

Ordinary `m.room.message` events without channel post/comment product markers are not mirrored into P2P message tables and do not emit P2P ordinary-message SSE events.

## P2P Product Surface

The product route contract remains:

- `GET /_p2p/health`
- `POST /_p2p/query`
- `POST /_p2p/command`
- `GET /_p2p/events`
- `GET /.well-known/portal/owner.json`

At that point, protected product actions required bearer `access_token`, while `agent_token` was accepted only for fixed `mcp.*` actions and `GET /_p2p/events`. Current servers have removed `GET /_p2p/events` and accept `agent_token` only for `agent.matrix_session.create` and fixed `mcp.*` HTTP actions. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `rooms.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.

The live P2P action count is now 90.

## ProductCore Conversation Contract

`conversations.list` and `conversations.get` expose ProductCore conversation identity for clients. The response keeps the existing stable fields:

- `conversation_id`
- `matrix_room_id`
- `kind`
- `lifecycle`
- `peer_mxid`
- `title`
- `avatar_url`
- `last_event_id`
- `last_activity_at`
- `projection_state`
- `projection_reason`

This pass adds hydrated membership and relationship fields to the conversation view:

- `member_count`: direct conversations return `2`; group and channel conversations return the joined member count from ProductCore membership state when available.
- `membership`: the current owner membership in this conversation. Direct accepted contacts map to `join`; pending direct contacts map to `pending`.
- `relationship_status`: direct-contact relationship state such as `accepted`, `pending_inbound`, or `pending_outbound`.
- `role`: current owner role in the conversation, for example `member` or `owner`.
- `hydration_state`: `ready` when ProductCore has enough state to open the conversation, otherwise `pending`, `conflict`, or `failed`.
- `hydration_reason`: machine-readable reason when hydration is not ready, for example `owner_membership_missing`.
- `capabilities`: server-derived operation flags. Current keys are `open`, `send`, `send_media`, `call`, `invite`, `manage_members`, `rename`, `remove_members`, `leave`, `delete`, `post_create`, `comment_create`, `reaction_toggle`, `post_recall`, `comment_recall`, and `comments_enabled`. Group/channel management and post capabilities are true only when the current owner is joined with role `owner`.

Clients should use these ProductCore fields instead of inferring room type or owner membership from Matrix timeline shape, display names, or member-count text.

## ProductCore Create/Join Mutation Result

`groups.create`, `groups.join`, and `channels.join` now return the ProductCore conversation created or hydrated by the mutation path:

- `operation`: `{action, status, room_id, conversation_id}` for the completed mutation.
- `conversation`: the same `ConversationView` shape returned by `conversations.list/get` when a conversation record exists for the created or joined room.

Clients should open the returned `conversation.conversation_id` / `conversation.matrix_room_id` directly after a successful create or join. They should not reconstruct a chat route from group/channel names, member counts, or Matrix room aliases.

## Contact Re-Request Semantics

`contacts.request` is idempotent by `mxid`. When a non-deleted contact already exists for the same peer, the action returns the stored contact and does not create a second direct Matrix room. Existing pending contacts re-send a pending invite in the stored room. Existing accepted contacts normally return unchanged; when `remote_node_base_url` is supplied and the peer node reports that it no longer retains the relationship, the contact becomes `pending_outbound` in the stored room and waits for peer approval.

Inbound direct invite projection now treats the Matrix membership event sender as the authoritative requester identity. `io.dirextalk.room.profile` stripped-state fields such as `requester_mxid` and `domain` cannot override the projected `peer_mxid` or peer domain; if they conflict with the event sender, profile display fields from that direct profile are ignored. This prevents a third user from making a pending friend request appear to come from another Matrix user or domain.

`contacts.request` restores an existing `deleted` contact for the same peer only when the peer still retains the accepted relationship. The response preserves the original `room_id`, refreshes supplied display/domain metadata, returns `status: "accepted"`, and rejoins the original direct Matrix room through the P2P transport when transport is configured. If the requester has left the old invite-only direct room, the requester node calls the peer node `contacts.reactivate`; the peer node re-invites the requester only when it still has an accepted contact for the same `peer_mxid` and `room_id`. This lets the side that deleted a contact intentionally restore that old direct conversation without peer approval. If the peer node has an existing non-accepted contact for the same requester and old `room_id`, `contacts.reactivate` records `pending_inbound` on the peer node and returns `status: "pending_inbound"`; the requester node preserves the original `room_id`, returns `pending_outbound`, does not try to invite from a user that already left the direct room, and does not join or restore chat until the peer accepts. If the peer no longer has a matching contact record, `contacts.request` preserves the original `room_id`, returns `pending_outbound`, sends a direct invite for that old room, and waits for peer acceptance. Requests to add the local owner and self `contacts.reactivate` calls are rejected with `400`.

If a node still has an accepted contact for the real Matrix sender and receives a fresh direct invite for a different room, it does not create a new pending contact from the supplied invite metadata. Instead, it re-invites that real sender to the retained accepted `room_id`, allowing a peer whose local contact data was deleted or rebuilt to recover the old direct room. `contacts.reactivate` also ignores caller-supplied profile fields for non-accepted retained contacts; missing local display/domain values are derived from `requester_mxid`.

When `contacts.request` is called again for an existing `pending_outbound` peer, the requester node now re-sends a direct Matrix invite to the stored direct room instead of only returning the cached contact. A target node that previously stored the peer as `rejected` now accepts the new direct invite projection and changes the contact back to `pending_inbound`, so pending friend request notices can appear again.

When a direct invite projection creates or reopens a local `pending_inbound` contact, `/_p2p/events` now emits `contact.requested` with `room_id`, `peer_mxid`, `display_name`, `avatar_url`, `domain`, and `status: "pending_inbound"`. Existing pending/accepted contacts remain de-duplicated and do not emit another contact request event.

`contacts.request` accepts optional friend-request text as `remark` and also recognizes `request_message`, `message`, or `reason` for compatibility. Pending contact responses, `contacts.list`, `sync.bootstrap.contacts`, `sync.bootstrap.pending.friend_requests`, and `contact.requested` events expose the text as `remark` while the request is pending. The value is carried in native direct-room profile state for invite projection and is cleared when the contact becomes accepted so it is not reused as a contact display remark or conversation title.

`contacts.requests.accept` is idempotent for an already accepted contact and returns the stored accepted contact without issuing another Matrix join. This prevents a repeat accept from surfacing a Matrix "already joined" failure.

P2P contact persistence now enforces one row per `peer_mxid`. Existing duplicate contact rows are compacted during migration, preferring `accepted`, then `pending_inbound`, then `pending_outbound`, then rejected/deleted records.

Contact responses now expose peer avatar metadata through `avatar_url`. This applies to `contacts.list`, contact mutation responses, and the `contacts` array returned by `sync.bootstrap`. Direct-contact conversations derived from contact records also carry the same `avatar_url` so clients can render the peer avatar consistently after bootstrap or contact mutations.

Contact mutation responses now include a ProductCore `operation` object and attach the hydrated direct `conversation` when the contact has a `room_id`. This applies to `contacts.request`, `contacts.reactivate`, `contacts.requests.accept`, `contacts.requests.reject`, `contacts.requests.delete`, `contacts.delete`, and `contacts.update`. Clients should consume the returned `conversation_id` / `matrix_room_id` instead of reconstructing a direct chat route from peer display names or Matrix direct-room heuristics.

## Group Invite Reject And Stored Member Role Semantics

`groups.invite.reject` records the current local user's pending group invite as `membership: "reject"` and returns `{status: "rejected", member}`. Rejected group invites are hidden from `groups.members` and `groups.list`, matching the first-version ProductCore rule that hidden memberships (`leave`, `remove`, `reject`, `ban`) are not ordinary visible members.

Group and channel member mutations now load the existing ProductCore member record before applying leave/remove/mute/unmute/reject transitions. Owner protection is therefore based on persisted `role` and `membership`, including after a service reload backed by PostgreSQL, instead of relying on an in-memory default member record. ProductCore group/channel roles are owner/member only.

Group/channel invite and member mutation responses now include a ProductCore `operation` object and attach the hydrated `conversation` when the mutated room has a `p2p_conversations` record. This applies to `groups.invite`, `groups.invite.reject`, `groups.leave`, `groups.member.remove`, `groups.member.mute`, `groups.member.unmute`, `channels.invite`, `channels.leave`, `channels.member.remove`, `channels.member.mute`, `channels.member.unmute`, `channels.join_request.approve`, and `channels.join_request.reject`.

## Client Migration Notes

Clients should align as follows:

- Message list, offline history, search, unread, and recall use Matrix SDK calls.
- Local clear-history/delete-for-me uses the Dirextalk Matrix `local_delete` extension.
- Conversation placement still uses product metadata: contacts for private chats, groups for groups, channels for channels.
- `sync.bootstrap` is still useful for product metadata and pending notices, but no longer provides a `rooms` array.
- Agent API allow-lists must not include removed message/search/backup actions.

## Updated Artifacts

- P2P action registry and fixed Agent MCP allowlist.
- P2P storage migration dropping the legacy ordinary-message mirror table.
- Syncapi local hide storage and Matrix read-path filtering.
- Roomserver projector rules for ordinary messages, channel posts/comments, reactions, and redactions.
- Dual-node smoke script using Matrix send/history/search/redaction/local_delete.
- Postman collection with removed P2P actions deleted and `local_delete` examples added.
- Feature inventory and implementation notes.
