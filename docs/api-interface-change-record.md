# API Interface Change Record

Last updated: 2026-06-25

## 2026-06-25 Immutable Channel Type

`channels.update` now ignores `channel_type`. Channel type is creation-time metadata and cannot be changed after a channel exists. Requests that include `channel_type` continue to apply other mutable fields but leave the stored `channel_type` unchanged.

Clients must set `channel_type` only in `channels.create`. Post channels (`channel_type=post`) get shared Matrix history visibility at creation or when binding an existing room as a post channel.

## 2026-06-25 Agent Token And CLI Cleanup

Agent-token dynamic permission management is removed. `apis.list` and `apis.status` are no longer P2P actions and calls to those action names return `unknown action`.

Protected product actions require bearer `access_token`. `agent_token` is accepted only for fixed MCP actions: `mcp.rooms.search`, `mcp.messages.send`, `mcp.messages.list`, `mcp.channel_posts.list`, `mcp.channel_comments.list`, and `mcp.channel_comments.create`. `GET /_p2p/events` and non-MCP protected actions reject `agent_token`.

The first-party CLI module and its helper package are removed: `cmd/direxio-cli`, `internal/agentclient`, CLI build scripts, CLI agent-skill docs, and the project-local `direxio-cli` Codex skill.

## 2026-06-25 Matrix Push Gateway Metadata

Matrix event pushes sent to HTTP push gateways now include optional Direxio display/routing metadata when the room has Direxio product room state. Normal direct, group, and text-channel message pushes can include `notification.title`, `notification.push_type=message`, `notification.room_id`, `notification.event_id`, and short `notification.room_type` (`direct`, `group`, or `channel`). The gateway owns the visible body text and sets it to `Send you a new message`.

Post channels (`io.direxio.room.profile.channel_type=post`) are not sent to HTTP push gateways in this phase. Matrix `m.call.invite` events in Direxio rooms use `push_type=call` and add `call_id` plus `call_kind=voice`; product `calls.create` / `calls.incoming` actions remain P2P event/call-record flows unless represented as Matrix call invite events.

## 2026-06-24 Portal Single-Device Login

`portal.bootstrap`, `portal.auth`, and `portal.password` now create an exclusive Matrix device session for the portal owner. After the new session is created, the server deletes the owner's other Matrix devices while preserving the current `device_id`, so previous phones receive Matrix `M_UNKNOWN_TOKEN` on later authenticated requests and must ask the user to log in manually.

`agent.matrix_session.create` remains an owner-token internal Matrix session helper and does not evict the portal user's phone session.

## 2026-06-24 User Public Channel Lookup

`users.public_channels` now returns only public channels owned by the target user. Public channels where the target user is only a normal member are no longer included in the "user's channels" list.

`users.public_channels` also accepts optional `remote_node_base_url` and forwards the public query to that owner node, matching remote public channel discovery flows. The forwarded request strips `remote_node_base_url` before reaching the target node.

## 2026-06-24 Channel Room Projection Guard

Matrix room state is now treated as a channel projection source only when `io.direxio.room.profile.room_type` is explicitly `io.direxio.room.channel` and `channel_id` is an explicit product channel id. Empty profiles, group/direct room profiles, missing `channel_id`, and Matrix-room-id-shaped `channel_id` values are ignored by channel refresh logic.

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

Backend startup now creates a real private Matrix agents room when the stored `agent_room_id` is empty or still uses the legacy pseudo form `!agent:<server>`. The real room id is persisted in portal state and written to the bootstrap credentials file as `agent_room_id`. The room contains the portal owner and the local agent user `@agent:<server>`; existing real agents rooms are repaired on startup by joining the local agent user if needed.

`portal.bootstrap`, `portal.auth`, and `sync.bootstrap` expose the current real `agent_room_id` so app and gateway clients can restore the Agent conversation from either login/session metadata or first-screen metadata.

`GET /_p2p/events` can now emit `agent_room.message` for ordinary `m.room.message` events in the configured agents room only. Payload fields are `room_id`, `event_id`, `sender_mxid`, `body`, `msgtype`, and `origin_server_ts`. Ordinary messages in other non-product rooms still do not produce P2P events or P2P message records.

`mcp.messages.send` accepts internal optional gateway marker params, including `agent_gateway=true` and `gateway_source`. Marked replies are sent by the local agent user, written as Matrix messages with `io.direxio.agent_gateway` metadata, and are not re-emitted as inbound `agent_room.message` events, preventing gateway reply loops. `mcp.messages.list` returns the agents room name as `Agents` and displays messages from `@agent:<server>` using the configured agent `display_name`.

## 2026-06-23 Channel Join Request Approval Retry

`channels.join_request.approve` now treats an existing `join_failed` or `approved` channel join request as retryable approval state instead of returning `404 join request not found`. This lets a channel owner retry approval after the requester-node `channels.public.join_result` callback temporarily failed. Ordinary channel invites still are not accepted by the join-request approval action.

## 2026-06-22 Direxio Local MCP Backend Actions

Added six protected MCP-oriented P2P actions for the local Direxio MCP adapter:

- `mcp.rooms.search` on `POST /_p2p/query`
- `mcp.messages.list` on `POST /_p2p/query`
- `mcp.channel_posts.list` on `POST /_p2p/query`
- `mcp.channel_comments.list` on `POST /_p2p/query`
- `mcp.messages.send` on `POST /_p2p/command`
- `mcp.channel_comments.create` on `POST /_p2p/command`

All six require bearer auth. Owner `access_token` can call them, and `agent_token` is accepted only for these fixed MCP actions. The response contracts are intentionally concise for MCP tooling and do not expose full `conversationView`, `channelPostRecord`, `channelCommentRecord`, Matrix event payloads, projection state, capability maps, or internal Matrix tokens.

Ordinary MCP message send/list remains separate from channel post/comment product content. `mcp.messages.send` writes a plain `m.room.message` without `p2p_kind`; `mcp.messages.list` reads ordinary Matrix timeline messages through a server-side Matrix reader and filters out events carrying product `p2p_kind`. No P2P ordinary-message store was added.

The live P2P body-action count is now 91.

## 2026-06-22 Matrix-First Cleanup

This pass removes the remaining ambiguous compatibility surface from current code, examples, skills, and Postman.

Breaking removals and contract changes:

- `portal.setup` is no longer a P2P action. Portal initialization is automatic; clients use `portal.bootstrap`, `portal.auth`, `portal.status`, and `portal.password`.
- `P2P_BOOTSTRAP_CREDENTIALS_FILE` is no longer a compatibility alias. Use `P2P_PORTAL_CREDENTIALS_FILE`.
- Removed legacy Matrix product state is no longer generated, read, or projected. Current product state is `io.direxio.room.profile`, `io.direxio.member.policy`, and `io.direxio.join_request`.
- Public channel approval no longer exposes Matrix invite as the product workflow. Approval writes `io.direxio.join_request status=approved`; the requester homeserver performs Matrix join.
- New public internal action `channels.public.join_result` carries owner-node approval results to the requester node. Params: `room_id`, `channel_id`, `user_id`, `status`, `reason`, `server_names`, and `request_id`.
- Public channel join response status is one of `pending`, `rejected`, `approved`, `joining`, `joined`, or `join_failed`.
- Added protected action `agent.matrix_session.create` on `POST /_p2p/command`. It requires bearer `access_token` and returns a Matrix Client-Server session: `access_token`, `device_id`, `user_id`, and `homeserver`.
- `portal.bootstrap`, `portal.auth`, and `portal.password` return one setup state field: `initialized`. It is `false` while the generated initial password is still in use and becomes `true` after `portal.password` changes that password. Clients should store `access_token` and route by `initialized`; profile completion is independent.

The live P2P body-action count is 86. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.

## Current Pass

This pass completes the Matrix-only ordinary message migration for Direxio product rooms. There is now one ordinary message source of truth: Matrix Client-Server event storage and timelines. P2P product APIs keep product metadata, contact/group/channel state, channel post/comment projections, calls, favorites, follows, reports, Agent configuration, and bootstrap metadata.

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
- Per-user local hide/clear: `POST /_matrix/client/v1/io.direxio/rooms/{roomID}/local_delete`

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

- Direct/private chats: `contacts.list`, `sync.bootstrap.contacts`, pending friend requests, and Direxio direct room profile state.
- Groups: `groups.list`, `sync.bootstrap.groups`, pending group invites, and `io.direxio.room.profile` with group type.
- Channels: `channels.list`, `sync.bootstrap.channels`, pending channel notices, public channel actions, and `io.direxio.room.profile` with channel type.

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

Protected product actions require bearer `access_token`. `agent_token` is accepted only for fixed `mcp.*` actions. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`.

The live P2P action count is now 85.

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

`contacts.request` restores an existing `deleted` contact for the same peer only when the peer still retains the accepted relationship. The response preserves the original `room_id`, refreshes supplied display/domain metadata, returns `status: "accepted"`, and rejoins the original direct Matrix room through the P2P transport when transport is configured. If the requester has left the old invite-only direct room, the requester node calls the peer node `contacts.reactivate`; the peer node re-invites the requester only when it still has an accepted contact for the same `peer_mxid` and `room_id`. This lets the side that deleted a contact intentionally restore that old direct conversation without peer approval. If the peer no longer retains the relationship, `contacts.request` preserves the original `room_id`, returns `pending_outbound`, sends a direct invite for that old room, and does not join or restore chat until the peer accepts. Requests to add the local owner and self `contacts.reactivate` calls are rejected with `400`.

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
- Local clear-history/delete-for-me uses the Direxio Matrix `local_delete` extension.
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
