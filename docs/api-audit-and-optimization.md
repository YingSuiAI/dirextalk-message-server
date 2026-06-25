# API Audit And Optimization Notes

Last updated: 2026-06-20

## Scope

This audit covers the current checkout of the Direxio Message Server backend. It verifies that exposed product features are backed by real code, records completed functionality, checks multi-node communication paths, and lists optimization opportunities. Runtime behavior changes from the hardening pass are recorded in `docs/api-interface-change-record.md`.

Primary sources:

- `p2p/routing.go`
- `p2p/service.go`
- `p2p/storage.go`
- `p2p/dendrite_transport.go`
- `p2p/projector.go`
- `p2p/remote_public.go`
- `p2p/consumer.go`
- `setup/monolith.go`
- `scripts/p2p-dual-smoke.ps1`
- route registration in `clientapi`, `syncapi`, `federationapi`, `mediaapi`, `relayapi`, and `setup/mscs/msc2836`

Generated/maintained outputs:

- `AGENTS.md`
- `docs/feature-inventory.md`
- `docs/postman/direxio-message-server.postman_collection.json`
- `docs/api-interface-change-record.md`

## Summary

- Current P2P product API exposes 81 actions from `p2p.Service.Handle`.
- Current Postman collection includes the live P2P product action requests plus Matrix/Direxio Message Server route-index requests.
- The P2P API is not a placeholder implementation. Requests pass through real handler validation, action dispatch, optional Bearer authorization, service logic, persistence, Matrix transport, and roomserver projection.
- The P2P store has concrete PostgreSQL/SQLite-compatible migrations and table-level operations for portal, markers, contacts, groups, channels, posts, comments, reactions, members, calls, favorites, reports, and follows. Ordinary messages use Matrix/syncapi storage only.
- Multi-node communication is implemented through Matrix federation for room/member/message/redaction/state events and a narrow unauthenticated public-action proxy for public channel discovery and join requests. Product projections cover group/channel lifecycle and channel post/comment state; ordinary message history remains Matrix-native.
- Runtime behavior changes were made for security and consistency; see `docs/api-interface-change-record.md`.

## Confirmed Implemented Feature Areas

See `docs/feature-inventory.md` for the full action checklist.

Implemented areas:

- Portal bootstrap/auth/status/setup compatibility/password rotation
- Matrix session issuing for the portal owner
- Owner profile read/update and member-profile propagation
- Bootstrap sync metadata and read markers
- Contact request/accept/reject/delete/update
- Matrix-native room send/media/history/search/unread/redaction, plus Direxio Matrix local history hiding
- Group create/update/list/invite/join/members/mute/unmute/invite policy/member moderation/leave/dissolve
- Channel create/update/list/invite/invite grant/join/members/mute/unmute/read marker/member moderation/leave/dissolve
- Public channel search/detail/join request and public channels by user
- Channel posts/comments/reactions and owner comment/reaction history
- Calls create/incoming/get/event/list/active
- Favorites add/list/delete/batch delete
- Follows add/list/remove
- Reports submit
- Agent password/config/status and API permission enable/disable

## Real Implementation Evidence

### HTTP Surface

`p2p/routing.go` registers real handlers for:

- `POST /_p2p/query`
- `POST /_p2p/command`
- `GET /_p2p/health`
- `GET /.well-known/portal/owner.json`

The handler decodes JSON with a 1 MB limit, requires `action`, defaults missing `params` to `{}`, enforces public-action vs Bearer authorization, calls `Service.Handle`, and serializes success/error JSON.

### Action Dispatch

`p2p.Service.Handle` contains concrete switch cases for 81 product actions. Cases dispatch to named service methods such as `bootstrap`, `auth`, `syncBootstrap`, `contactRequest`, `groupResult`, `channelResult`, `channelJoinRequest`, `channelInviteGrantCreate`, `channelPost`, `channelComment`, `channelReaction`, and `memberMutation`.

Unknown actions return `400 unknown action`; this is real validation, not a placeholder.

### Persistence

`p2p.Store` defines the persistence boundary. `p2p.DatabaseStore` creates and uses `p2p_%` tables through migrations, including indexes for public channel lookup, members, reactions, contacts, calls, favorites, and follows. Ordinary message timelines, search, and unread data are stored and queried by Matrix/syncapi.

The service writes through store methods for business state that must survive restart. If the store cannot open, `setup/monolith.go` intentionally falls back to in-memory state and logs a warning so the Matrix homeserver can still start.

### Matrix Transport

`p2p.DendriteTransport` is the real Matrix boundary:

- creates rooms through `roomserverAPI.PerformCreateRoom`
- product post/comment/reaction writes go through Matrix event building and `roomserverAPI.SendEvents`
- invites, joins, leaves, and kicks users through roomserver APIs
- writes owner profile changes as `m.room.member`
- sends product group/channel metadata and dissolve state events
- sends product post/comment redactions as `m.room.redaction`; ordinary chat recall uses Matrix Client-Server redaction directly
- reads native `io.direxio.room.profile` and room members from roomserver current state

This confirms product actions are integrated with Direxio Message Server rather than being a detached in-memory API.

### Projection

`p2p.ProjectOutputEvent` consumes roomserver output and projects:

- channel posts/comments from `p2p_kind`
- `m.reaction` to `p2p_reactions`
- `m.room.member` to `p2p_members`
- native `io.direxio.room.profile` to `p2p_channels` and `p2p_groups`
- native `io.direxio.member.policy` to `p2p_members` role/mute policy
- native `io.direxio.join_request` to pending/approved/rejected channel member state
- native `io.direxio.room.profile` to channel/group projections
- direct contact invites to pending inbound contacts
- redacted events to channel post/comment projection removal
- ordinary `m.room.message` events stay in Matrix storage and are not mirrored into P2P message tables

This is the critical read-side bridge that keeps local P2P state synchronized with federated Matrix events.

## Multi-Node Communication Audit

### Confirmed Flow

Two-node communication uses two mechanisms:

- Product public lookup proxy: `remotePublicAction` posts public actions only to the request-provided `remote_node_base_url` after URL validation.
- Matrix federation: room creation, invites, joins, member state, messages, reactions, and redactions flow through Direxio Message Server federation. Product state is projected back into P2P tables by the roomserver consumer; ordinary message history remains Matrix-native.

The dual-node smoke script validates:

- independent PostgreSQL-backed nodes
- portal auth on both nodes
- Matrix `/keys/upload` with returned Matrix access tokens
- A-to-B contact request projection and accept flow
- profile display/avatar propagation
- public channel creation, remote discovery by room ID, public join request forwarding, approval/rejection
- cross-node channel join with `server_names`
- channel member projection
- cross-node room/group/channel messages
- local delete vs distributed recall behavior
- posts/comments/reactions projection
- group/channel member moderation and owner constraints
- restart recovery from persisted state
- Agent token/API permission behavior
- all actions present in `Service.Handle` are exercised by the smoke script's coverage check

### Integration Fit With Direxio Message Server

The new P2P service fits the existing framework well:

- It mounts through `httputil.Routers` and uses the same external HTTP server as other components.
- It reuses Direxio Message Server database connection management and migration utilities.
- It uses roomserver APIs for Matrix-side writes instead of bypassing Direxio Message Server internals.
- It consumes JetStream roomserver output with its own durable consumer.
- It leaves Matrix client/federation/media routes unchanged.
- It keeps product API routing isolated under `/_p2p/*`.

The main integration caveat is operational policy: when the P2P store or projector falls back/fails, the Matrix service can still start. `portal.status` now exposes `store_mode` and `projector_started`; deployments that require strict durability should still enforce that in their process manager or health checks.

## Findings And Optimization Opportunities

### P1: Public Remote Lookup SSRF Risk - Fixed

Current behavior validates Matrix room IDs with Matrix parsing, rejects URL-shaped server names, and requires `remote_node_base_url` on remote public lookup requests. Missing or invalid remote node URLs return `400` without outbound probing.

Remaining operational guidance:

- only pass remote node URLs learned from trusted client-side discovery or explicit user intent;
- use the insecure TLS override only for trusted local self-signed test nodes.

### P1: Production Remote Public Lookup Trust Model - Fixed

Remote public lookup now verifies TLS by default. `P2P_REMOTE_NODE_INSECURE_SKIP_TLS_VERIFY=true` must be explicitly set for the local dual-node self-signed topology.

Remaining operational guidance:

- production deployments should use trusted certificates;
- dual-node local compose sets the insecure flag intentionally for generated self-signed certificates.

### P1: Remote Node Discovery Is Naive Outside The Compose Topology - Fixed

Implicit `https://<serverName>/_p2p` discovery is disabled. Operators must configure each remote P2P base URL explicitly.

Remaining operational guidance:

- use exact Matrix server names as keys, including ports;
- consider a future signed discovery mechanism if manual mapping becomes too operationally heavy.

### P1: Public Channel Join State Can Diverge From Matrix Membership - Fixed

Open public join requests and approved join requests now return product approval/join states instead of exposing Matrix invite. Local users are joined through local `Transport.JoinRoom`; remote users are joined through requester-node `channels.public.join_result`. Product state reports `joined` only after Matrix join succeeds.

Remaining operational guidance:

- clients should refresh after `joined` or `join_failed` and should not infer joined from approval alone;
- `user_id` remains accepted for compatibility, so callers should still validate identity at their own trust boundary.

### P1: Federated Channel Metadata Can Be Overwritten With Defaults - Fixed

Channel creation/update now publishes full native `io.direxio.room.profile` metadata, including visibility, join policy, type, comments setting, and dissolved state. Removed legacy Matrix product state is ignored by current read/project paths. Sparse remote state preserves known values or defaults conservatively to private/invite.

### P1: `calls.active` Terminal-State Filter Looks Incomplete - Fixed

`calls.active` now filters `ended`, `rejected`, `missed`, and `failed` in both memory and database store paths.

### P2: Group/Channel Update And Dissolve Are Local Product Mutations - Fixed

`groups.update`, `groups.dissolve`, `channels.update`, and `channels.dissolve` now publish product state events. Remote projectors upsert metadata or remove dissolved records.

Remaining operational guidance:

- verify lifecycle projection in the dual-node smoke for every release.

### P2: Malformed Product Metadata Can Stall Projection - Fixed

Malformed optional channel comment mention metadata now falls back to an empty mentions array instead of returning a projector error.

Remaining operational guidance:

- continue to reserve NAKs for infrastructure/storage failures.

### P2: Projector Ingests All Matrix Room Messages - Fixed

Generic Matrix messages are now projected only for known P2P contact/group/channel rooms or product-marked events.

Remaining operational guidance:

- contact invite projection remains intentionally supported.

### P2: Store/Projector Health Is Not Exposed - Partially Fixed

`portal.status` now returns `store_mode` and `projector_started`. `/_p2p/health` remains intentionally simple for load balancers.

Remaining operational guidance:

- production readiness checks should inspect `portal.status` or logs, not only `/_p2p/health`.

### P2: Action Catalog Is Split Across Code And Docs

`p2p/action_registry.go`, `serviceapi` public/MCP allowlists, docs, smoke coverage, and Postman examples can drift. Keep generated artifacts synchronized with that source of truth.

Recommended improvement:

- generate `docs/feature-inventory.md` and Postman collection from the action switch in CI;
- add a test that verifies docs metadata count or generated artifacts are current.

### P2: Duplicate P2P Message Sync Surface - Fixed

The duplicate P2P ordinary-message sync/search/send/delete/recall surface was removed. Clients use Matrix Client-Server APIs for ordinary message send, incremental sync, history, search, unread data, and redaction, with Direxio Matrix `local_delete` for per-user local hiding.

### P2: Public Remote Action Error Detail Is Collapsed - Fixed

`remotePublicAction` now extracts safe `error` or `message` fields from upstream non-200 responses and preserves upstream status where practical.

### P1: Post-Migration Client/Server Room-State Regressions - Fixed Again

The Matrix-native migration exposed repeated regressions at the boundary between Matrix membership, P2P projections, and client conversation state.

Observed recurring symptoms:

- closing and reopening the app could return to login when the client cleared persisted session state;
- message refresh events were applied as global unread totals instead of room-scoped deltas;
- friend search fired on every keystroke and displayed stale bootstrap `owner` names instead of current Matrix profile names;
- direct contacts could appear as duplicate `owner` conversations when contact rows and Matrix rooms were not normalized by peer;
- invited groups/channels appeared on the home page before true Matrix `join`, then failed to send messages;
- shared public channel invite cards still went through public approval instead of direct invite/join;
- private channel invite cards attempted public lookup and failed with `channel not found`.

Server-side guardrails now in place:

- main `sync.bootstrap`, `groups.list`, and `channels.list` payloads expose only owner `membership=join`; invite/pending rooms stay in pending sections;
- contact, group/channel create, join, invite, invite reject, leave, member remove, member mute/unmute, and channel join-request approval/rejection paths return a ProductCore `operation` plus hydrated `conversation` when the mutated room has a conversation record;
- `channels.invite_grant.create` creates a room-scoped channel grant and Matrix-invites current joined members of the share room;
- `channels.join` accepts `grant_id` and `share_room_id`/`via_room_id` for invite-card joins while public search users still use `channels.public.join_request`;
- `contacts.list` and bootstrap contacts de-duplicate by `peer_mxid`, preferring accepted contacts over pending rows;
- ordinary message deltas come from Matrix `/sync`; P2P SSE remains for product projection refreshes.

Client-side guardrails required for every release:

- session storage is cleared only by explicit logout, not by process restart;
- search requests fire only on submit/search, not on every text change;
- direct conversations are keyed by `room_id` with `peer_mxid` merge fallback;
- unread counters update from Matrix room-scoped sync data;
- invite/pending rooms are shown only in invitation surfaces until `membership=join`;
- private/shared channel cards call `channels.join`, not public lookup or public join-request.

### P3: Fallback To In-Memory State Can Hide Persistence Misconfiguration - Partially Fixed

The fallback remains by design, but `portal.status.store_mode` now exposes whether P2P state is backed by `database` or `memory`.

Remaining operational guidance:

- make strict-fail startup configurable for production.

## Placeholder/Stub Assessment

No P2P product action was found to be a pure placeholder. The code contains real validation, state mutation, persistence calls, or Matrix transport/projection behavior for each current action. The main gaps are not empty handlers; they are cross-node consistency, trust-boundary hardening, and operational visibility issues.

Repo-wide TODO/FIXME comments still exist in inherited Direxio Message Server areas, including user-interactive auth, sync filtering/redaction comments, and federation housekeeping. These are not newly added P2P placeholders, but they are inherited maintenance items.

## Interface Change Impact

This pass changed runtime behavior for remote public lookup, public channel join/approval status, product projection, and status diagnostics. The complete contract and compatibility record is in `docs/api-interface-change-record.md`.

Any future change to input/output contracts must also be recorded there.
