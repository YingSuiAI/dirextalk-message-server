# AGENTS.md

This repository is a Direxio fork of Element Dendrite. It is one Go monolith that serves Matrix homeserver protocols, Direxio product APIs, agent tooling, event projection, policy enforcement, storage, and deployment/runtime wiring. Maintain it from that whole-system perspective.

## Project Scope

- Matrix-compatible APIs stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Direxio product APIs use a small body-action surface:
  - `GET /_p2p/health`
  - `POST /_p2p/query`
  - `POST /_p2p/command`
  - `GET /_p2p/ws`
  - `GET /.well-known/portal/owner.json`
- Product action requests use this envelope:

```json
{
  "action": "channels.public.get",
  "params": {
    "room_id": "!room:dendrite-a:8448",
    "remote_node_base_url": "https://dendrite-a:8448/_p2p"
  }
}
```

Protected product actions require `Authorization: Bearer <access_token>` when issued through retained HTTP routes. Logged-in client product actions use owner `GET /_p2p/ws` `client.request` frames after creating a `realtime.ws_ticket.create` ticket with the owner `access_token`; ordinary timeline/media/history/search/redaction still use Matrix Client-Server APIs. HTTP `/query` and `/command` remain for health/bootstrap/auth/status/password, owner WS ticket creation, fixed MCP actions, and node-to-node public/callback actions. `agent_token` is accepted only for fixed `mcp.*` HTTP actions. MCP actions must not be migrated into WS `client.request`. `GET /_p2p/ws` authenticates only a short-lived single-use owner WS ticket, not a bearer token. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`. `channels.public.join_result` is an internal node-to-node approval callback, not a normal client workflow entry.

## Runtime Model

- `cmd/direxio-message-server` is the production service entry point.
- `setup/monolith.go` wires client, federation, media, sync, relay, and Direxio product routes.
- `setup/config` owns runtime configuration.
- `internal/productpolicy` enforces Direxio product rules on Matrix Client-Server writes.
- `p2p/action_registry.go` maps product actions to service handlers; `p2p/service_*.go` files own business orchestration.
- `p2p/transport.go`, `p2p/transportapi`, and `p2p/dendrite` adapt product-originated writes into Matrix room/member/state/message/redaction behavior.
- `p2p/consumer.go` and `p2p/projector.go` project roomserver output into Direxio read models and product events.
- Package storage implementations own durable state and migrations for their package.
- Docker development uses PostgreSQL 18 and writes bootstrap credentials to `/var/direxio-message-server/p2p/bootstrap.json`.

Do not reason about changes as isolated P2P, Matrix, or Direxio Message Server layers. Trace the complete path from entry point to authorization, policy, storage, roomserver output, consumers, federation/sync visibility, docs examples, and verification.

## Matrix-Native Product State

Current Direxio product rooms use native Matrix state:

- `m.room.create.content.type`
  - `io.direxio.room.direct`
  - `io.direxio.room.group`
  - `io.direxio.room.channel`
- `io.direxio.room.profile`
- `io.direxio.member.policy`
- `io.direxio.join_request`

Rules:

- Matrix `m.room.member membership=join` is the final joined fact.
- New group rooms and chat/text channel rooms must set `m.room.history_visibility` to `joined` at creation so later members only receive ordinary timeline events from their own join point. Post channels are the explicit exception: set `m.room.history_visibility` to `shared` for `channel_type=post` when creating a post channel or binding an existing room as a post channel, because members must be able to see all current channel posts and comments. Channel type is immutable after creation; `channels.update` must ignore any `channel_type` value for old-client compatibility. Do not apply history visibility changes retroactively to existing rooms unless explicitly requested.
- Product read models are projections unless a domain rule explicitly makes a table source-of-truth state.
- Product group/channel roles are `owner` or `member` only. Do not add or document additional product roles.
- Ordinary Matrix timeline messages are not copied into a second P2P ordinary-message store. Ordinary send, history, search, unread, and redaction use Matrix Client-Server APIs.
- Deleted direct contacts keep the old direct room identity for recovery. The side that deleted the contact may intentionally restore the old room without peer approval when the peer still retains the accepted relationship. A peer re-request after the relationship is deleted must remain `pending_*` in the old room until the deleting side explicitly accepts; it must not silently rejoin or restore chat.
- The configured agents room is a real private Matrix room id persisted as `agent_room_id`. Backend startup must join both owner and local `@agent:<server>` to that room and grant the agent enough state power to publish `io.direxio.agent.status`. Agent bridge message intake, streaming previews, edits, and final replies use Matrix Client-Server sync/send/edit as `@agent:<server>`; they must not be mirrored through `agent_room.message`, `client.agent_stream`, or `server.agent_stream`. Do not use legacy pseudo ids such as `!agent:<domain>`.
- Channel posts/comments/reactions are product projections backed by Matrix events and redactions.
- Removed legacy product state must not be generated, read, or projected as current behavior.

## Business Scenarios

- Portal/auth: bootstrap, password login, password rotation, Matrix device session creation, credentials file refresh. Portal login/password/bootstrap Matrix sessions are single-device for the portal owner: creating a new portal session deletes the owner's other Matrix devices so old phones receive `M_UNKNOWN_TOKEN`; `agent.matrix_session.create` is the exception and must not evict the user's phone session.
  - Login/session responses expose only `access_token` and one setup flag, `initialized`.
  - `initialized` means the generated initial password has been changed through `portal.password`; profile completion must not affect it.
- Profile: owner profile read/update, Matrix-facing profile storage, member profile propagation.
- Contacts: direct room invite, inbound/outbound request projection, accept/reject/delete/reactivate, remark update.
- Rooms/messages: ordinary text/media send, history, search, local hiding, and redaction through Matrix APIs.
- Groups: create, update, invite, join, leave, dissolve, mute/unmute, invite policy, member moderation.
- Channels: create, update, list, public search/detail, public join request, approval/rejection callbacks, automatic Matrix join after approval, invite/join/leave/dissolve, members, moderation, read markers.
- Posts/comments/reactions: create/list/recall posts, create/list/recall comments, reply/mention metadata, like toggles, owner comment/reaction history.
- Calls: create, incoming, get, list, active, and state events `connected`, `ended`, `missed`, `failed`.
- Favorites/follows: favorite add/list/delete/batch delete, follow add/list/remove. User-facing report submission is owned by the signed imadmin public API, not this message-server P2P action surface.
- Push: System pushes use Matrix Push Gateway after userapi push-rule evaluation. The server must not infer app foreground/background from `/sync`, read receipts, or pusher registration. Current Direxio clients report lifecycle and focused room over `GET /_p2p/ws` frames after creating a `realtime.ws_ticket.create` ticket. A connected foreground WS session suppresses unread notification insertion and HTTP push gateway delivery only for the same focused room; background, disconnected, expired, or different-room state keeps normal push behavior. During migration, global Matrix account data `io.direxio.push.context` remains a server-clock 60-second fallback for clients without a fresh WS session.
- Agent/API: Agent config/password are owner-token operations. Agent tokens may call only fixed MCP HTTP actions; they cannot create realtime WS tickets. Owner clients must read bridge online state from native Matrix room state in the real `agent_room_id`: `io.direxio.agent.status` with state key `@agent:<server>` and content field `online`. The running local Agent bridge publishes `online=true/false` through its Matrix `@agent:<server>` session; the server must not infer online from `agent.config.enabled`, `/sync`, or WS sessions. Server startup/repair and `agent.config.update enabled=false` may publish `online=false` as a safe fallback. `sync.bootstrap` only returns `agent_room_id`; do not add `agent_online` back to bootstrap and do not emit `agent.presence` events. `agent.status`/`agents.status` are removed and must not be used. The real `agent_room_id` defaults to no system push for the portal owner through a room-level Matrix push rule with empty actions; preserve an existing explicit rule for that room.
- Multi-node communication: Matrix federation plus remote public channel lookup and approval flows through explicit `remote_node_base_url`.

## Development Workflow

Run commands from the repository root in the shell that matches the current environment. PowerShell is acceptable on Windows; Bash is acceptable on Linux, macOS, or WSL. Prefer the platform-native command form instead of forcing WSL-only instructions.

Recommended discovery and diagnostics:

- `gopls`: recommended Go semantic diagnostics. If installed, run `gopls check <touched-go-files>` for Go changes.
- `rg`: exact strings, configs, docs, JSON, shell, and fallback search.

Common validation commands:

```bash
gofmt -w <touched go files>
go test ./p2p ./internal/productpolicy -count=1
go test ./internal/httputil ./setup -count=1
go build ./cmd/direxio-message-server
python3 -m json.tool docs/postman/direxio-message-server.postman_collection.json >/dev/null
git diff --check
docker compose -f docker-compose.p2p.yml config
docker compose -f docker-compose.p2p-dual.yml config
```

Run the local single-node stack:

```bash
docker compose -f docker-compose.p2p.yml up --build
docker compose -f docker-compose.p2p.yml exec message-server cat /var/direxio-message-server/p2p/bootstrap.json
```

Run the multi-node regression.

PowerShell:

```powershell
$env:P2P_DUAL_PUBLIC_HOST = if ($env:P2P_DUAL_PUBLIC_HOST) { $env:P2P_DUAL_PUBLIC_HOST } else { "host.docker.internal" }
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python scripts/p2p-three-node-regression.py
```

Bash:

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

Run local PostgreSQL-backed tests by setting `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_HOST`, `POSTGRES_PORT`, and `POSTGRES_DB`. The default local password used by this workspace is `123789`. Tests create isolated `dendrite_test_*` databases and must drop those test databases when each test finishes.

Use `docs/postman/direxio-message-server.postman_collection.json` for manual API checks. Import it into Postman, set `baseUrl`, then call `portal.auth` to obtain `access_token` and `agent_token`.

## Project-Local Codex Skills

Project-specific skills live under `.codex/skills/`. They must be maintained as global Direxio server skills, not as P2P/Matrix/Direxio Message Server layer silos:

- `direxio-change-orchestrator`: whole-server impact maps and project-skill routing.
- `direxio-contract-sync`: route/action/schema/auth/Postman/current-doc synchronization.
- `direxio-event-state-tracer`: Matrix event/state/policy/consumer/projection/sync/federation rules.
- `direxio-storage-migration-guard`: durable storage, migrations, indexes, DB selection, and restart recovery rules.
- `direxio-targeted-verification`: repo-specific formatting, tests, build, JSON, compose, skill, and lint check selection.

Keep project skills as Direxio-specific guidance. Do not duplicate generic system skills; update `AGENTS.md`, `docs/current-project-documentation.md` when applicable, and the relevant `.codex/skills/*/SKILL.md` files together when project rules, contracts, event/state behavior, validation expectations, or workflow conventions change.

## Code Standards

- Keep Go code formatted with `gofmt` or existing `goimports`.
- Keep behavior close to the owning package, but review the complete cross-package path before editing.
- Keep large Direxio product modules grouped by business responsibility. Prefer business directories only when package dependencies stay acyclic; otherwise split focused files in the owning package before introducing a new module seam.
- Shared product record/value types that must be used by multiple business packages should live in a small domain package with aliases or adapters at existing entry points when that preserves compatibility.
- Do not add URL-shaped product endpoints unless there is a strong compatibility reason. Prefer stable product actions and documented `params` schemas.
- Do not silently change API request or response fields. If an input/output contract changes, update `docs/api-interface-change-record.md`.
- Do not add memory-only state for behavior that must survive restart. Add or extend durable storage and migrations.
- Do not bypass `p2p.Transport` for product-originated Matrix room/member/state/message/redaction behavior.
- Do not bypass `internal/productpolicy` expectations for Matrix Client-Server writes into Direxio product rooms.
- Do not derive outbound remote-node URLs from Matrix room IDs. Remote public lookup must validate Matrix IDs and require request-provided `remote_node_base_url`.
- Keep public channel lookup read-only. Missing/private channels must not create placeholder records.
- Do not rely on fabricated remote Matrix users in multi-node tests. Use real portal owners from the compose topology, such as `@owner:dendrite-a:8448` and `@owner:dendrite-b:8448`.
- Do not mark public channel membership as `joined` until Matrix membership has actually reached join state.
- Do not overwrite rich channel metadata with sparse federated defaults. Missing visibility, join policy, type, or comments settings should fail closed or preserve known state.
- Keep local delete and recall distinct: local delete hides locally; recall sends Matrix redaction and should project across nodes.
- Keep Postman examples importable JSON, not snippets copied into Markdown.

## Multi-Node Review Checklist

- Verify remote public lookup rejects malformed room IDs, URL-shaped server names, and untrusted private/internal hosts.
- Verify remote public lookup requests include the expected `remote_node_base_url`.
- Verify behavior works when caller and owner are on different homeservers.
- Verify `server_names` is passed when joining a remote room.
- Verify remote room IDs preserve domains and ports.
- Verify target nodes reject private or invite-only public requests correctly.
- Verify public join approval calls the requester node for remote users and does not report `joined` until the requester node's Matrix join succeeds.
- Verify channel visibility, join policy, type, and comments settings survive remote discovery and join.
- Verify group/channel update and dissolve behavior on a second node when cross-node behavior changes.
- Verify roomserver output consumers/projectors write product read models and do not pollute them from non-product Matrix rooms.
- Verify malformed optional product metadata cannot block later projection events.
- Verify restart recovery from PostgreSQL when durable state changes.
- Verify owner profile changes propagate through `m.room.member` projection when profile behavior changes.
- Verify multi-node regression coverage exists for changed cross-node flows on the current platform.

## Documentation Rules

- Keep README-level docs focused on operating the service.
- Treat `docs/current-project-documentation.md` as the current project fact source.
- Put detailed implementation notes in `docs/p2p-integrated-as-implementation.md`.
- Put audit findings and optimization notes in `docs/api-audit-and-optimization.md`.
- Put request/response contract changes in `docs/api-interface-change-record.md`.
- Keep Postman examples importable JSON.
