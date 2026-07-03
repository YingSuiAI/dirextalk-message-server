# Unified Channel Post And Chat Plan

Date: 2026-07-03

## Goal

Merge the current Dirextalk text-channel and post-channel behavior into one channel model. A single channel room must support:

- owner-created posts;
- member comments and reactions on posts;
- ordinary Matrix chat messages in the same room;
- client-side switching between the post surface and chat surface;
- independent post/comment/reaction state and ordinary chat state;
- no HTTP Push Gateway delivery for channel messages.

## Decisions

- One channel maps to one Matrix room. There is no secondary room for chat or posts.
- Post, comment, and reaction events stay in the same room but remain product-marked:
  - posts use `m.room.message` with `content.p2p_kind=channel_post`;
  - comments use `m.room.message` with `content.p2p_kind=channel_comment`;
  - reactions use `m.reaction` with channel/post/comment metadata.
- Ordinary chat uses normal `m.room.message` events without `content.p2p_kind`.
- The ordinary chat timeline must ignore product-marked post/comment/reaction events.
- The post/comment/reaction projection must ignore ordinary chat events.
- Channel conversation activity and last-message preview are driven only by ordinary chat events. Creating posts or comments must not replace the chat preview.
- New unified channels use room-level `m.room.history_visibility=shared` so members can read current posts and comments after joining.
- Because Matrix history visibility is room-level, shared visibility also applies to ordinary chat events in that room. This is an accepted product tradeoff for the unified room model.
- Channel mute remains a single channel-level mute. There is no separate post mute versus chat mute in this iteration.
- Push suppression means Push Gateway suppression only. Matrix sync, unread accounting, and local app state can still reflect channel messages.

## Server Contract

`channels.create` continues to accept `channel_type` for existing clients, but the server must treat current channel rooms as unified regardless of whether the stored value is `chat`, `text`, `post`, or empty.

Server behavior:

- Default new channel type to the unified/post-capable behavior.
- Publish shared history visibility for every newly created or newly bound channel room.
- Expose post/comment/reaction capabilities for joined channel conversations according to role and `comments_enabled`, not according to `channel_type`.
- Continue to reject member-created post events through Matrix Client-Server policy; channel owners create posts through product actions.
- Continue to enforce `comments_enabled` for comments and product reactions.
- Backfill channel post/comment/reaction projections for any joined channel during rebuild/reactivation, not only stored `channel_type=post`.
- Suppress Push Gateway delivery for all `room_type=channel` events.

## Flutter Contract

The Flutter client treats every channel as a unified channel:

- channel creation no longer asks the user to choose chat versus posts;
- channel creation sends the server's unified/post-capable channel type;
- joined channel cards and shared channel links open a channel surface that can reach both posts and chat;
- the post surface remains backed by `channels.posts.*` actions;
- the chat surface is backed by Matrix room messaging for the same `room_id`;
- post/comment/reaction events must not render as ordinary chat messages.

The client may keep parsing legacy `channel_type` values for cached data and old share links, but UI affordances must not hide chat or post features based only on that value.

## Implementation Tasks

1. Server tests first:
   - channel creation/history-visibility tests expect shared visibility for all channel types;
   - conversation capability tests expect post/comment/reaction capabilities for joined channels independent of channel type;
   - projector tests prove `channel_post`/`channel_comment` do not update ordinary conversation activity;
   - channel-content backfill tests cover non-`post` channel types;
   - push metadata tests expect `SuppressGateway=true` for all channels.

2. Server implementation:
   - centralize unified channel capability/type handling in `p2p`;
   - update channel creation/history visibility;
   - update conversation capability derivation;
   - update post-content backfill guard;
   - update message projection separation;
   - update Push Gateway suppression.

3. Flutter tests first:
   - channel creation request sends the unified/post-capable type;
   - create-channel UI has no type selector;
   - joined channel page exposes both post and chat entry points;
   - legacy chat/post channel types still resolve to the unified channel navigation;
   - channel chat rendering filters product-marked events if the chat view directly consumes Matrix events.

4. Flutter implementation:
   - remove the visible channel-type choice from creation;
   - route every joined channel to the unified channel page;
   - keep post creation/detail/list flows available for joined channels;
   - wire the chat tab/entry to the same Matrix room's chat behavior;
   - remove type-based hiding of post or chat affordances.

5. Documentation sync:
   - update `docs/current-project-documentation.md`;
   - update `docs/p2p-integrated-as-implementation.md`;
   - update `docs/api-interface-change-record.md`;
   - update `docs/dirextalk-push-gateway.md`;
   - update Postman examples if channel creation examples still imply separate channel modes;
   - update project-local agent rules when they mention split channel history behavior.

## Verification

Minimum automated verification:

- `gofmt -w` on touched Go files;
- `go test ./p2p ./internal/productpolicy -count=1`;
- `go test ./userapi/consumers -count=1`;
- `go build ./cmd/dirextalk-message-server`;
- `python -m json.tool docs/postman/dirextalk-message-server.postman_collection.json`;
- `git diff --check`;
- Flutter targeted tests for AS client, channel creation, channel page, channel conversation, and product navigation;
- Flutter analyzer if the targeted changes compile.

Multi-node and manual verification:

- bring up the dual-node compose stack;
- run the existing three-node regression;
- create a channel;
- publish a post;
- send a normal chat message in the same channel room;
- confirm the post list shows the post only through post APIs;
- confirm the chat surface shows ordinary chat and does not show the product post/comment event as a chat bubble;
- confirm server-side Push Gateway suppression is applied for channel messages.

## Resolved Client Risk

Flutter channel chat reuses the existing Matrix-backed `GroupChatPage` with channel context (`channelId`/`channelName`) instead of the removed static `ChannelConversationPage` shell. The chat timeline filters product-marked channel post/comment/reaction events, while ordinary `m.room.message` sends continue through the Matrix SDK in the same channel room.
