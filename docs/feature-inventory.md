# Feature Inventory

Last updated: 2026-06-30

`p2p.Service.Handle` is the source of truth for the product action surface. The current code exposes **93** P2P body actions plus the WS product request/event endpoint.

## Current Feature Areas

| Area | Current Status |
| --- | --- |
| Portal/Auth | Default portal initialization, password auth, password rotation, Matrix session issuing, credential file refresh. |
| Profile | Owner profile read/update and propagation to Matrix room member state. |
| Sync | Bootstrap metadata, read markers, and pending notices. Ordinary message sync/history/search uses Matrix APIs. |
| Contacts | Direct invite request/accept/reject/delete, remark update, inbound invite projection, contact reactivation. |
| Blocks | Owner-managed blacklist for contacts, with display names for the settings list and blocked friend requests rejected before Matrix writes. |
| Matrix Messages | Text/media send, history, search, unread data, local hiding, and redaction use Matrix Client-Server APIs. |
| Groups | Create/update/list/invite/join/members/mute/leave/remove/dissolve/invite policy/invite reject. |
| Channels | Create/update/list/invite/join/invite grants/members/moderation/mute/read-marker/dissolve. |
| Public Channels | Public detail/search, remote room-id discovery, public join request forwarding, approval result callback, public channels by user. |
| Channel Posts/Comments/Reactions | Post/comment create/list/recall, reply and mention metadata, like toggles, owner comment/reaction history. |
| Calls | Create/incoming/get/list/active, persisted lifecycle timestamps, and realtime call state events. |
| Favorites/Follows | Favorite lifecycle and followed domains. User-facing reports are handled by the signed imadmin public API, not the message-server P2P action surface. |
| Agent/API Permissions | Agent config/status/password and per-action permission enable/disable. |
| Conversations | ProductCore conversation list/get and operation summaries. |

## Current Caveats

- Product APIs use `POST /_p2p/query` and `POST /_p2p/command` with an action envelope.
- Public remote channel lookup requires request-provided `remote_node_base_url`.
- Public channel approval does not expose Matrix invite as the product workflow. Approval triggers Matrix join locally or through `channels.public.join_result` on the requester node.
- `joined` means Matrix membership has reached join state.
- Current product Matrix state is `io.dirextalk.room.profile`, `io.dirextalk.member.policy`, and `io.dirextalk.join_request`.
- P2P tables are projection/read models; Matrix membership and ordinary message timelines are canonical.

## Action Groups

Public actions:

- `portal.bootstrap`
- `portal.auth`
- `portal.status`
- `portal.account.delete`
- `contacts.reactivate`
- `rooms.reactivate`
- `channels.public.search`
- `channels.public.get`
- `channels.public.join_request`
- `channels.public.join_result`
- `users.public_channels`

Protected action groups:

- Agent/MCP: `agent.config.get`, `agent.config.update`, `agent.password`, `mcp.rooms.search`, `mcp.messages.send`, `mcp.messages.list`, `mcp.room_members.list`, `mcp.channel_posts.list`, `mcp.channel_comments.list`, `mcp.channel_comments.create`
- Portal/Profile/Sync: `portal.password`, `profile.get`, `profile.update`, `sync.bootstrap`, `sync.read_marker`
- Contacts: `contacts.request`, `contacts.list`, `contacts.update`, `contacts.delete`, `contacts.requests.accept`, `contacts.requests.reject`, `contacts.requests.delete`
- Blocks: `blocks.add`, `blocks.list`, `blocks.remove`
- Conversations: `conversations.list`, `conversations.get`
- Groups: `groups.create`, `groups.update`, `groups.invite`, `groups.invite.reject`, `groups.join`, `groups.list`, `groups.members`, `groups.leave`, `groups.dissolve`, `groups.mute`, `groups.unmute`, `groups.invite_policy.update`, `groups.member.remove`, `groups.member.mute`, `groups.member.unmute`
- Channels: `channels.create`, `channels.update`, `channels.invite`, `channels.invite_grant.create`, `channels.join`, `channels.list`, `channels.members`, `channels.leave`, `channels.dissolve`, `channels.mute`, `channels.unmute`, `channels.read_marker`, `channels.join_request.approve`, `channels.join_request.reject`, `channels.member.remove`, `channels.member.mute`, `channels.member.unmute`
- Channel posts/comments/reactions: `channels.posts.create`, `channels.posts.list`, `channels.posts.recall`, `channels.comments.create`, `channels.comments.list`, `channels.comments.recall`, `channels.post_reaction.toggle`, `channels.comment_reaction.toggle`, `channels.my_comments`, `channels.my_reactions`
- Calls: `calls.create`, `calls.incoming`, `calls.get`, `calls.event`, `calls.active`, `calls.list`
- Favorites/Follows: `favorites.add`, `favorites.list`, `favorites.delete`, `favorites.delete_batch`, `follows.add`, `follows.list`, `follows.remove`

The importable examples for every action are maintained in `docs/postman/dirextalk-message-server.postman_collection.json`.
