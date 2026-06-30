# WebSocket Realtime Sync Migration

## Goal Anchor

- Server repository: `C:\Users\84960\Desktop\direxio\direxio-message-server`
- Flutter repository: `C:\Users\84960\Desktop\direxio\direxio-flutter`
- Migration document: `C:\Users\84960\Desktop\direxio\direxio-message-server\docs\ws-realtime-sync-migration.md`
- Server branch: `feature/ws-realtime-sync`
- Flutter branch: `feature/ws-realtime-sync`
- Server WebSocket library: existing `github.com/coder/websocket`
- Flutter WebSocket library: `web_socket_channel`
- Target result: migrate the logged-in Direxio client/product action surface to WebSocket `client.request`/`server.response`, remove `GET /_p2p/events` SSE, keep HTTP for startup/session, fixed MCP, and node-to-node public/callback flows, and keep Matrix Client-Server as the ordinary timeline/media/history/search/redaction source of truth.

## Contract Summary

- Retain protected body action `realtime.ws_ticket.create` on HTTP.
- Retain WebSocket route `GET /_p2p/ws?ticket=<ticket>`.
- A WS ticket is short-lived, server-local, single-use, and issued only from an owner `access_token`.
- Remove `GET /_p2p/events`; there is no SSE fallback.
- HTTP `/query` and `/command` stay registered for portal bootstrap/auth/status/password, WS ticket creation, fixed MCP actions, and node-to-node public/callback actions.
- Logged-in owner clients call product actions through WS:

```json
{
  "type": "client.request",
  "id": "req-1",
  "action": "contacts.list",
  "params": {}
}
```

```json
{
  "type": "server.response",
  "id": "req-1",
  "action": "contacts.list",
  "ok": true,
  "result": {}
}
```

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

- WS still supports `client.hello`, `client.lifecycle`, `client.focus`, `client.ack`, `client.ping`, `client.request`, `server.response`, `server.ready`, `server.event`, `server.cursor_reset`, `server.pong`, and `server.error`. `client.lifecycle` may include `state`, `hidden`, and `flags`; `client.focus` may include `focused` and `flags`.
- `client.command` remains a one-release compatibility alias and maps internally to `client.request`; new Flutter code sends only `client.request`.
- Fixed `mcp.*` actions stay HTTP-only. WS `client.request` for `mcp.*` returns `server.response ok=false status=400 error="action requires http"`.

## Behavioral Rules

- Matrix `/sync` remains the source for ordinary room timeline, Matrix state, membership, account data, and agent room state.
- Matrix Client-Server APIs remain responsible for ordinary text/media send, history, search, redaction, and local delete.
- WS `client.request` is the primary product request/response API after login, excluding MCP.
- `/_p2p/query` and `/_p2p/command` reject non-retained client product actions with an explicit "action requires websocket" error.
- Flutter keeps the existing chat timeline and `AgentMessageBody`/`gpt_markdown` rendering stack for agent output. Whole-chat packages such as `flutter_chat_ui` are not introduced in this phase because they would replace current Matrix timeline, local outbox, scroll, read-marker, call-record, and selection behavior.
- WS session state is server-internal only. It must not expose user-visible presence or focused room information to other users.
- WS session state is memory-only because it is a connection/session fact. Persistent product facts continue to use existing Matrix state and product stores.
- Agent room message intake, previews, edits, and final replies use Matrix Client-Server APIs as `@agent:<server>` and are not transported through product WS frames.
- Push suppression uses server time. A connected foreground, non-hidden WS session with the same focused room suppresses system push for that room; hidden, background, disconnected, expired, no-focus, or different-room state allows normal push. Lifecycle/focus `flags` are stored as server-side session context for future push decisions, not exposed as user presence. The agents room keeps its default no-system-push rule.
- Non-idempotent mutations are not automatically retried if the WS connection drops before a matching `server.response` arrives; the UI must show the error/retry state.

## Acceptance Criteria

- Server route `GET /_p2p/ws` supports ticket authentication, replay from `since`, live P2P event streaming, cursor reset, client lifecycle/focus/ack, heartbeat, `client.request`, and clean disconnect handling.
- Owner WS sessions can call representative query and command actions through `client.request`, including `contacts.list`, `groups.create`, and `sync.read_marker`/`channels.read_marker`.
- Agent-token callers cannot create WS tickets. MCP calls remain HTTP actions authorized by `agent_token` or owner `access_token`; local agent bridge message transport uses Matrix sync/send/edit.
- Unknown action, malformed request frame, missing `id`, and handler errors return `server.response` with `ok=false`.
- `GET /_p2p/events` is not registered.
- HTTP `/query` and `/command` reject non-retained logged-in product actions while retained login/ticket and node public/callback actions still work.
- Flutter uses `WsAsClient` for logged-in product methods and does not construct SSE fallback transports.
- Flutter login flow remains HTTP token -> HTTP WS ticket -> WS hello -> WS `sync.bootstrap` on cold start or cursor reset -> `server.event` reducer plus `client.ack`.
- Flutter resolves/rejects requests from `server.response` and does not auto-retry non-idempotent mutations after a dropped pending response.
- Flutter renders Agent output from the Matrix timeline and Matrix edit aggregation; it does not consume `server.agent_stream`.
- Disconnect, weak network, stale ticket, backend restart, browser refresh, and cursor retention gaps recover without losing product events.
- Single-node, Matrix agent room bridge, weak-network, and multi-node public lookup/join_result acceptance paths are covered by automated tests or documented manual validation.
- Server and Flutter repositories are committed separately after verification.

## Migration Checklist

- [x] Phase 0.1: Create `feature/ws-realtime-sync` in the server repository.
- [x] Phase 0.2: Create `feature/ws-realtime-sync` in the Flutter repository.
- [x] Phase 0.3: Create this migration document with goal anchor and acceptance criteria.
- [x] Phase 1: Add server WS ticket contract and tests.
- [x] Phase 2: Add server `/_p2p/ws` protocol and tests.
- [x] Phase 3: Reuse server P2P event replay/live fanout over WS.
- [x] Phase 4: Add server WS session state and push decision integration.
- [x] Phase 5: Add Flutter realtime transport abstraction.
- [x] Phase 6: Add Flutter WS transport and ticket request during the initial realtime migration.
- [x] Phase 7: Add Flutter lifecycle, room focus, ack, reconnect, and weak-network recovery.
- [x] Phase 8: Sync docs, API change record, Postman, and project rules.
- [x] Phase 9: Run server automated verification.
- [x] Phase 10: Run Flutter automated verification.
- [x] Phase 11: Start Docker backend image and validate WS integration.
- [x] Phase 12: Build Flutter Web and complete browser/manual WS verification.
- [x] Phase 13: Review diffs and commit server repository.
- [x] Phase 14: Review diffs and commit Flutter repository.
- [x] Phase 15: Add server WS `client.command` read-marker migration and tests.
- [x] Phase 16: Add agent WS stream fanout plus Flutter final-message aggregation tests.
- [x] Phase 17: Run Phase 2 server and Flutter verification.
- [x] Phase 18: Review diffs and commit server and Flutter Phase 2 repositories.
- [x] Phase 19: Implement server WS `client.request`/`server.response` action dispatch.
- [x] Phase 20: Remove server SSE route/authorization/tests and make HTTP product routes reject non-retained client actions.
- [x] Phase 21: Implement Flutter WS-backed `AsClient` and remove SSE fallback transports.
- [x] Phase 22: Update Flutter event-stream provider and auth/bootstrap paths to use WS product requests.
- [x] Phase 23: Sync docs, API change record, Postman, project rules, and this acceptance checklist for WS Product API full migration.
- [x] Phase 24: Run server verification for p2p, policy, route/build, Postman JSON, gopls, and diff checks.
- [x] Phase 25: Run Flutter verification for WS transport/client/provider tests, analyzer, formatting, and diff checks.
- [x] Phase 26: Validate device or browser WS smoke; record whether a physical Android/iOS device is available.
- [x] Phase 27: Review diffs and commit server repository.
- [x] Phase 28: Review diffs and commit Flutter repository.
- [x] Phase 29: Correct MCP boundary back to fixed HTTP body actions and reject `mcp.*` over WS.
- [x] Phase 30: Extend WS lifecycle/focus state with `state`, `hidden`, and `flags`; make hidden sessions keep normal push behavior.
- [x] Phase 31: Extend Flutter lifecycle reporting and reconnect replay of client state.
- [x] Phase 32: Re-run focused server/Flutter verification, update documentation, and commit the correction.

## Verification Log

Record command evidence here as phases complete.

- `go test ./p2p -run "TestRealtimeWS" -count=1` passed.
- `go test ./userapi/consumers -run "TestNotifyLocal(OnlySuppressesFreshForegroundNotifications|UsesRealtimeFocusWhenWSSessionExists)" -count=1` passed.
- `go test ./p2p ./userapi/consumers -count=1` passed.
- `go build ./cmd/direxio-message-server` passed.
- `python -m json.tool docs/postman/direxio-message-server.postman_collection.json > $null` passed.
- `git diff --check` passed in the server repository.
- `gopls check internal/realtime/session_store.go p2p/realtime_ws.go p2p/routing.go p2p/routing_ws_test.go p2p/service.go userapi/consumers/roomserver.go userapi/consumers/roomserver_test.go` passed with no diagnostics.
- `flutter pub get` passed in `C:\Users\84960\Desktop\direxio\direxio-flutter`.
- `dart format --set-exit-if-changed lib/data/as_realtime_transport.dart lib/data/http_as_client.dart lib/presentation/providers/as_event_stream_provider.dart lib/presentation/providers/push_context_provider.dart lib/presentation/widgets/realtime_room_focus.dart lib/presentation/pages/chat_page.dart lib/presentation/pages/group_chat_page.dart test/as_realtime_transport_test.dart test/as_event_stream_refresh_controller_test.dart` passed.
- `flutter test --no-pub test/as_event_stream_refresh_controller_test.dart test/as_realtime_transport_test.dart test/matrix_push_context_test.dart` passed.
- `flutter analyze --no-pub` passed.
- `flutter build web --no-pub` passed; it reported existing WebAssembly dry-run incompatibility warnings from `flutter_secure_storage_web`, Matrix IndexedDB, and `olm`/`dart:ffi`, but produced `build\web`.
- `git diff --check` passed in the Flutter repository.
- `docker compose -f docker-compose.p2p.yml config` passed.
- `docker compose -f docker-compose.p2p.yml up -d --build` built and started the single-node stack.
- `Invoke-RestMethod http://127.0.0.1:8008/_p2p/health` returned `status=ok`.
- Docker WS smoke used real owner `realtime.ws_ticket.create` tickets to connect to `GET /_p2p/ws`, returned `server.ready`, and accepted `client.lifecycle`, `client.focus`, and `client.ack` frames. Agent-token WS smoke is no longer a valid acceptance item; Agent bridge traffic is verified through Matrix Client-Server sync/send/edit.
- `flutter devices` found Windows, Chrome, and Edge targets; no Android/iOS device was connected in this workspace.
- Chrome Web smoke served `build\web` at `http://127.0.0.1:3001`, logged in against `http://127.0.0.1:8008`, opened the Agent room, and verified browser WS frames: `client.hello`, `client.lifecycle foreground=true`, and `client.focus` for the real `agent_room_id`.
- Flutter repository commit: `037567e feat: add realtime websocket transport`.
- `go test ./p2p -run "TestRealtimeWS(CommandUpdatesReadMarker|CommandRejectsAgentRole|AgentStreamFanoutToOwner)$" -count=1` passed.
- `go test ./p2p -run "TestRealtimeWS|TestEventStream|TestP2PEvent" -count=1` passed.
- `go test ./p2p -count=1` passed.
- `go test ./p2p ./internal/productpolicy -count=1` passed.
- `go test ./internal/httputil ./setup -count=1` passed.
- `go build ./cmd/direxio-message-server` passed.
- `python -m json.tool docs/postman/direxio-message-server.postman_collection.json > $null` passed.
- `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" .codex\skills\direxio-contract-sync` passed.
- `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" .codex\skills\direxio-event-state-tracer` passed.
- `gopls check p2p/realtime_ws.go p2p/routing_ws_test.go p2p/service.go` passed.
- `git diff --check` passed in the server repository.
- `flutter test --no-pub test/as_realtime_transport_test.dart` passed.
- `flutter test --no-pub test/agent_message_content_test.dart` passed.
- `flutter test --no-pub test/as_realtime_transport_test.dart test/as_event_stream_refresh_controller_test.dart test/agent_message_content_test.dart` passed.
- `flutter analyze --no-pub` passed.
- `flutter build web --no-pub` passed; it reported the existing Wasm dry-run incompatibility warnings from `flutter_secure_storage_web`, Matrix IndexedDB, and `olm`/`dart:ffi`, but produced `build\web`.
- `git diff --check` passed in the Flutter repository.
- `flutter devices` found Windows, Chrome, and Edge targets; no Android/iOS physical device was connected in this workspace.
- Flutter Phase 2 repository commit: `21b2b97 feat: add websocket read markers and agent stream aggregation`.
- `go test ./p2p ./internal/productpolicy -count=1` passed.
- `go test ./internal/httputil ./setup -count=1` passed.
- `go build ./cmd/direxio-message-server` passed.
- `gopls check p2p\routing.go p2p\service.go p2p\realtime_ws.go p2p\routing_test.go p2p\routing_ws_test.go p2p\native_migration_test.go` passed.
- `python -m json.tool docs\postman\direxio-message-server.postman_collection.json > $null` passed.
- `git diff --check` passed in the server repository.
- `docker compose -f docker-compose.p2p.yml config` and `docker compose -f docker-compose.p2p-dual.yml config` passed with `P2P_DUAL_PUBLIC_HOST=host.docker.internal`.
- `docker compose -f docker-compose.p2p.yml up -d --build` rebuilt and started the single-node stack from the current server code.
- Docker owner WS smoke passed: health `ok`, real owner ticket connected to `GET /_p2p/ws`, `client.hello` returned `server.ready`, `client.request contacts.list` returned `server.response ok=true`, `GET /_p2p/events` returned `404`, and HTTP `contacts.list` returned `action requires websocket`.
- Docker agent WS smoke passed in the full-migration commit, then MCP was corrected back to HTTP-only: real agent tickets connect to `GET /_p2p/ws`; `mcp.rooms.search` over WS returns `server.response status=400 error=action requires http`, while HTTP `mcp.rooms.search` remains available to `agent_token`; `contacts.list` over agent WS returns `server.response status=403`.
- `dart format --set-exit-if-changed ...` passed for touched Flutter files.
- `flutter test --no-pub test\as_realtime_transport_test.dart test\http_as_client_test.dart test\as_event_stream_refresh_controller_test.dart test\as_bootstrap_store_test.dart test\as_event_cursor_store_test.dart` passed.
- `flutter analyze --no-pub` passed.
- `flutter build web --no-pub` passed; it still reports existing WebAssembly dry-run incompatibility warnings from `flutter_secure_storage_web`, Matrix IndexedDB, and `olm`/`dart:ffi`, but produced `build\web`.
- `git diff --check` passed in the Flutter repository.
- `flutter devices` found Windows, Chrome, and Edge targets; no Android/iOS physical device was connected in this workspace. `flutter emulators` found `Pixel_10_Pro`, but it is an emulator, not a physical device.
- Browser Web smoke served `build\web` at `http://127.0.0.1:3017`, logged in against `http://127.0.0.1:8008`, reached `#/home`, and after the Web SharedPreferences store fix no longer logged `P2P event stream refresh failed: MissingPluginException(... getApplicationSupportDirectory ...)`. Existing unrelated Web file-cache logs remain for chat-clear/conversation-summary/profile providers.
- Flutter WS Product API migration commit: `3e0d0ba feat: migrate product actions to websocket`.
- `go test ./p2p ./internal/realtime ./userapi/consumers -count=1` passed.
- `go build ./cmd/direxio-message-server` passed.
- `python -m json.tool docs\postman\direxio-message-server.postman_collection.json > $null` passed.
- `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" .codex\skills\direxio-contract-sync` passed.
- `python "$env:USERPROFILE\.codex\skills\.system\skill-creator\scripts\quick_validate.py" .codex\skills\direxio-event-state-tracer` passed.
- `git diff --check` passed in the server repository.
- `flutter test test/as_realtime_transport_test.dart test/as_event_stream_refresh_controller_test.dart` passed.
- `flutter analyze` passed.
- `git diff --check` passed in the Flutter repository.
- `flutter devices` found Windows, Chrome, and Edge targets; no Android/iOS physical device was connected, so physical-device acceptance still requires a connected phone build.

## Manual Device Acceptance

Run after automated verification:

- Start the server from the WS branch and log in with a real Flutter client build.
- Confirm `realtime.ws_ticket.create` succeeds and the client opens `GET /_p2p/ws`.
- In room A, receive a message for room A while the app is foreground and focused on room A: no system push should be delivered.
- Stay foreground in room A, receive a message for room B: system push should be delivered.
- Switch the app to background, receive a message for room A or B: system push should be delivered.
- Enter the real `agent_room_id`, receive an agent room message: default room push rule should suppress system push unless the user has explicitly overridden the room push rule.
- Trigger an agent streaming response: one visible agent message should update while streaming, hide intermediate fragment cards after completion, and keep the final output body visible.
- Disable WS during read-marker submission or another non-idempotent mutation: the client should surface the pending request failure and not retry automatically.
- Disable network and re-enable it: the client should reconnect with the last persisted `seq`, replay missed `server.event` frames, and keep local product state correct.
- Force a stale cursor beyond retention if available: the client should handle `server.cursor_reset`, run one `sync.bootstrap`, and resume from the recovered cursor.
