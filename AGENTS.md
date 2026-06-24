# AGENTS.md

This repository is a Direxio fork of Element Dendrite. It is one Go monolith that serves Matrix homeserver protocols, Direxio product APIs, agent tooling, event projection, policy enforcement, storage, and deployment/runtime wiring. Maintain it from that whole-system perspective.

## Project Scope

- Matrix-compatible APIs stay under `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*`.
- Direxio product APIs use a small body-action surface:
  - `GET /_p2p/health`
  - `POST /_p2p/query`
  - `POST /_p2p/command`
  - `GET /_p2p/events`
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

Protected product actions require `Authorization: Bearer <access_token>` or an enabled `agent_token`. Public actions are `portal.bootstrap`, `portal.auth`, `portal.status`, `contacts.reactivate`, `channels.public.search`, `channels.public.get`, `channels.public.join_request`, `channels.public.join_result`, and `users.public_channels`. `channels.public.join_result` is an internal node-to-node approval callback, not a normal client workflow entry.

## Runtime Model

- `cmd/direxio-message-server` is the production service entry point.
- `setup/monolith.go` wires client, federation, media, sync, relay, and Direxio product routes.
- `setup/config` owns runtime configuration.
- `internal/productpolicy` enforces Direxio product rules on Matrix Client-Server writes.
- `p2p/service.go` owns product action dispatch and business orchestration.
- `p2p/transport.go` and `p2p/dendrite_transport.go` adapt product-originated writes into Matrix room/member/state/message/redaction behavior.
- `p2p/consumer.go` and `p2p/projector.go` project roomserver output into Direxio read models and product events.
- Package storage implementations own durable state and migrations for their package.
- `cmd/direxio-cli` and `internal/agentclient` are first-party agent/operator integration surfaces.
- Docker development uses PostgreSQL 18 and writes bootstrap credentials to `/var/direxio-message-server/p2p/bootstrap.json`.

Do not reason about changes as isolated P2P, Matrix, or Direxio Message Server layers. Trace the complete path from entry point to authorization, policy, storage, roomserver output, consumers, federation/sync visibility, CLI/docs examples, and verification.

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
- New group rooms and chat/text channel rooms must set `m.room.history_visibility` to `joined` at creation so later members only receive ordinary timeline events from their own join point. Do not apply this retroactively to existing rooms unless explicitly requested.
- Product read models are projections unless a domain rule explicitly makes a table source-of-truth state.
- Ordinary Matrix timeline messages are not copied into a second P2P ordinary-message store. Ordinary send, history, search, unread, and redaction use Matrix Client-Server APIs.
- The configured agents room is the narrow gateway exception: backend startup must create and persist a real private Matrix room id for `agent_room_id`, join both owner and local `@agent:<server>` to that room, and ordinary messages in that room may emit `agent_room.message` SSE events for local agent gateways. Gateway replies must be sent by `@agent:<server>`, not owner. Do not use legacy pseudo ids such as `!agent:<domain>`.
- Channel posts/comments/reactions are product projections backed by Matrix events and redactions.
- Removed legacy product state must not be generated, read, or projected as current behavior.

## Business Scenarios

- Portal/auth: bootstrap, password login, password rotation, Matrix device session creation, credentials file refresh.
- Profile: owner profile read/update, Matrix-facing profile storage, member profile propagation.
- Contacts: direct room invite, inbound/outbound request projection, accept/reject/delete/reactivate, remark update.
- Rooms/messages: ordinary text/media send, history, search, local hiding, and redaction through Matrix APIs.
- Groups: create, update, invite, join, leave, dissolve, mute/unmute, invite policy, member moderation.
- Channels: create, update, list, public search/detail, public join request, approval/rejection callbacks, automatic Matrix join after approval, invite/join/leave/dissolve, members, moderation, read markers.
- Posts/comments/reactions: create/list/recall posts, create/list/recall comments, reply/mention metadata, like toggles, owner comment/reaction history.
- Calls: create, incoming, get, list, active, and state events `connected`, `ended`, `missed`, `failed`.
- Favorites/follows/reports: favorite add/list/delete/batch delete, follow add/list/remove, report submission.
- Agent/API permissions: Agent config/status/password and per-action enable/disable gating for Agent tokens.
- Multi-node communication: Matrix federation plus remote public channel lookup and approval flows through explicit `remote_node_base_url`.

## Development Workflow

Use Bash from the repository root in WSL2/Linux.

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

Run the WSL-compatible multi-node regression:

```bash
export P2P_DUAL_PUBLIC_HOST="${P2P_DUAL_PUBLIC_HOST:-host.docker.internal}"
docker compose -f docker-compose.p2p-dual.yml up -d --force-recreate dendrite-a dendrite-b dendrite-c
python3 scripts/p2p-three-node-regression.py
```

Use `docs/postman/direxio-message-server.postman_collection.json` for manual API checks. Import it into Postman, set `baseUrl`, then call `portal.auth` to obtain `access_token` and `agent_token`.

## Project-Local Codex Skills

Project-specific skills live under `.codex/skills/`. They must be maintained as global Direxio server skills, not as P2P/Matrix/Direxio Message Server layer silos:

- `direxio-change-orchestrator`: first-pass whole-system impact map before behavior changes.
- `direxio-contract-sync`: public route/action/schema/auth/CLI/Postman/docs synchronization.
- `direxio-event-state-tracer`: Matrix event/state/policy/consumer/projection/sync/federation tracing.
- `direxio-storage-migration-guard`: durable storage, migrations, indexes, DB selection, and restart recovery.
- `direxio-targeted-verification`: focused formatting, tests, build, JSON, compose, skill, and lint checks.
- `direxio-cli`: operate an existing Direxio service through the first-party CLI.

When project rules, contracts, event/state behavior, validation expectations, or workflow conventions change, update `AGENTS.md`, `docs/current-project-documentation.md` when applicable, and the relevant `.codex/skills/*/SKILL.md` files in the same change.

## Code Standards

- Keep Go code formatted with `gofmt` or existing `goimports`.
- Keep behavior close to the owning package, but review the complete cross-package path before editing.
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
- Verify WSL-compatible multi-node regression coverage exists for changed cross-node flows.

## Documentation Rules

- Keep README-level docs focused on operating the service.
- Treat `docs/current-project-documentation.md` as the current project fact source.
- Put detailed implementation notes in `docs/p2p-integrated-as-implementation.md`.
- Put audit findings and optimization notes in `docs/api-audit-and-optimization.md`.
- Put request/response contract changes in `docs/api-interface-change-record.md`.
- Keep Postman examples importable JSON.
