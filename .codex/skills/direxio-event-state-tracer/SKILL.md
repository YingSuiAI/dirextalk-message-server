---
name: direxio-event-state-tracer
description: Trace Direxio behavior through Matrix events, state, membership, profile data, redactions, sync output, federation, product policy, transport writes, consumers, projected read models, and multi-node flows.
---

# Direxio Event State Tracer

Use this skill when correctness depends on Matrix events or state becoming visible through another API, node, read model, or client sync.

## Trace Paths

For user/API-originated writes, trace the relevant path:

1. HTTP route or product action handler.
2. Authorization and `internal/productpolicy`.
3. Matrix write path: Client-Server route or `p2p.Transport`.
4. Roomserver input and output event.
5. Consumers in `syncapi`, `federationapi`, `userapi`, `appservice`, and Direxio projection.
6. Persistent stores and read models.
7. Client-visible read path: `/sync`, history, search, federation response, Direxio action read, `/_p2p/ws`, or `/_p2p/events`.

For inbound or federated behavior, start at roomserver/federation output and trace consumers forward to visible state.

## State Rules

- Product room type lives in `m.room.create.content.type` with Direxio direct, group, and channel room types.
- Product metadata uses `io.direxio.room.profile`.
- Member policy uses `io.direxio.member.policy`.
- Public channel approval uses `io.direxio.join_request`.
- Matrix `m.room.member membership=join` is the joined fact. Approval state is not joined state.
- New group rooms and chat/text channel rooms write `m.room.history_visibility=joined`. Post channels (`channel_type=post`) write `m.room.history_visibility=shared` on creation and existing-room binding. Channel type is immutable; `channels.update` ignores `channel_type` for old-client compatibility. Do not change existing rooms retroactively unless explicitly requested.
- Malformed optional product metadata must not block unrelated later projection events.
- Non-product Matrix rooms must not pollute Direxio product lists unless the bridge is intentional.
- Profile changes must keep Matrix-facing profile storage and Direxio profile/member views aligned when both are read.

## Direxio Checks

- Membership and invitation flows must cover owner, requester, local user, remote user, leave/kick/ban, deleted direct-contact recovery, and idempotent already-applied paths.
- Deleted direct contacts keep the old direct room for recovery. A peer re-request stays pending until the deleting side explicitly accepts.
- Local delete hides for one user; recall/redaction propagates as Matrix redaction.
- Ordinary timeline messages must not create a second product message source of truth.
- Channel posts, comments, and reactions are product projections backed by Matrix events and redactions.
- Agent online state is native Matrix room state in the real `agent_room_id`: event type `io.direxio.agent.status`, state key `@agent:<server>`, content field `online`. Do not mirror it through `sync.bootstrap.agent_online`, `agent.presence` SSE, Matrix `m.presence`, or agent-token `/_p2p/events` stream lifetime.
- Agent WS stream fragments are ephemeral owner-display frames only: agent-token `client.agent_stream` may fan out as owner `server.agent_stream`, but it must not be projected into product read models, P2P outbox, Matrix state, or Matrix timeline. The final recoverable agent answer remains a Matrix `m.room.message` from `@agent:<server>`.
- App foreground/background is not inferred from `/sync`, read receipts, or pusher registration. Current clients report lifecycle and focused room through `GET /_p2p/ws` using `client.lifecycle` and `client.focus`; session state is server-clocked and expires after 60 seconds. Fresh foreground WS state suppresses userapi notification insertion and HTTP push gateway delivery only for the same focused room, while background, disconnected, expired, no-focus, or different-room state keeps normal push behavior. Global Matrix account data `io.direxio.push.context` remains a fallback only when no fresh WS session exists.
- The real `agent_room_id` defaults to no system push for the portal owner through a room-level Matrix push rule with empty actions. Preserve an existing explicit room push rule for that room.
- Public channel remote approval must not report joined until the requester node performs the Matrix join successfully.
- Federation tests must use real compose users such as `@owner:dendrite-a:8448` and `@owner:dendrite-b:8448`, not fabricated remote Matrix users.

Use `direxio-targeted-verification` to choose package, Docker, and multi-node checks for the traced surface.
