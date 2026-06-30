# WebSocket Realtime Sync Migration

## Goal Anchor

- Server repository: `C:\Users\84960\Desktop\direxio\direxio-message-server`
- Flutter repository: `C:\Users\84960\Desktop\direxio\direxio-flutter`
- Migration document: `C:\Users\84960\Desktop\direxio\direxio-message-server\docs\ws-realtime-sync-migration.md`
- Server branch: `feature/ws-realtime-sync`
- Flutter branch: `feature/ws-realtime-sync`
- Server WebSocket library: existing `github.com/coder/websocket`
- Flutter WebSocket library: `web_socket_channel`
- Target result: migrate Direxio client/server realtime product-event delivery and client session state reporting to a WebSocket realtime layer while keeping Matrix `/sync`, federation, node sync, and HTTP product actions intact.

## Contract Summary

- Add protected body action `realtime.ws_ticket.create`.
- Add WebSocket route `GET /_p2p/ws?ticket=<ticket>`.
- A WS ticket is short-lived, server-local, single-use, and issued from an owner `access_token` or an `agent_token`.
- WS client frames are JSON text frames:
  - `client.hello`: replay cursor and client metadata.
  - `client.lifecycle`: app foreground/background state.
  - `client.focus`: currently focused Matrix room id, or empty when no room is focused.
  - `client.ack`: latest processed P2P event sequence.
  - `client.command`: owner-session lightweight command envelope. Initial allowed actions are `sync.read_marker` and `channels.read_marker`.
  - `client.agent_stream`: agent-session ephemeral stream fragment for the configured `agent_room_id`.
  - `client.ping`: client heartbeat.
- WS server frames are JSON text frames:
  - `server.ready`: authenticated connection accepted.
  - `server.event`: existing P2P event payload.
  - `server.cursor_reset`: retained cursor gap requiring bootstrap recovery.
  - `server.sync_hint`: reserved lightweight refresh hint.
  - `server.command_result`: result for a `client.command` frame.
  - `server.command_error`: validation/auth/action error for a command-like frame.
  - `server.agent_stream`: owner-visible ephemeral agent stream fragment.
  - `server.pong`: heartbeat response.
  - `server.error`: protocol or validation error.

## Behavioral Rules

- Matrix `/sync` remains the source for ordinary room timeline, Matrix state, membership, account data, and agent room state.
- `/_p2p/events` SSE remains available as fallback during this migration.
- `/_p2p/query` and `/_p2p/command` remain the primary product request/response APIs.
- Flutter keeps the existing chat timeline and `AgentMessageBody`/`gpt_markdown` rendering stack for agent output. Whole-chat packages such as `flutter_chat_ui` are not introduced in this phase because they would replace current Matrix timeline, local outbox, scroll, read-marker, call-record, and selection behavior.
- WS session state is server-internal only. It must not expose user-visible presence or focused room information to other users.
- WS session state is memory-only because it is a connection/session fact. Persistent product facts continue to use existing Matrix state and product stores.
- Agent stream fragments are memory-only delivery hints. They are not stored in the P2P outbox or Matrix timeline, and they do not replace the final Matrix `m.room.message` reply from `@agent:<server>`.
- Push suppression uses server time. A connected foreground WS session with the same focused room suppresses system push for that room; background, disconnected, expired, or different-room state allows normal push. The agents room keeps its default no-system-push rule.

## Acceptance Criteria

- Server route `GET /_p2p/ws` supports ticket authentication, replay from `since`, live P2P event streaming, cursor reset, client lifecycle/focus/ack, heartbeat, and clean disconnect handling.
- Owner WS sessions can update `sync.read_marker` and `channels.read_marker` through `client.command`, with HTTP body actions remaining the fallback request path.
- Agent-token WS sessions can send `client.agent_stream` for the configured `agent_room_id`; owner WS sessions receive `server.agent_stream`; the final durable reply still arrives through Matrix.
- Flutter uses WS by default, replays from persisted `lastSeq`, sends lifecycle and focused room state, sends event acknowledgements, sends read marker commands, and falls back to SSE/HTTP after WS failure.
- Flutter renders same-stream agent fragments as one visible agent message and replaces intermediate cards with the final body when the stream completes.
- Owner and agent connections keep their existing authorization boundaries.
- Disconnect, weak network, stale ticket, backend restart, browser refresh, and cursor retention gaps recover without losing product events.
- Docker backend startup, Flutter Web build, and browser verification demonstrate normal WS state transitions.
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
- [x] Phase 6: Add Flutter WS transport, ticket request, and SSE fallback.
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
- Docker WS smoke used real `realtime.ws_ticket.create` tickets for owner and agent tokens; both connected to `GET /_p2p/ws`, returned `server.ready`, and accepted `client.lifecycle`, `client.focus`, and `client.ack` frames.
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

## Manual Device Acceptance

Run after automated verification:

- Start the server from the WS branch and log in with a real Flutter client build.
- Confirm `realtime.ws_ticket.create` succeeds and the client opens `GET /_p2p/ws`.
- In room A, receive a message for room A while the app is foreground and focused on room A: no system push should be delivered.
- Stay foreground in room A, receive a message for room B: system push should be delivered.
- Switch the app to background, receive a message for room A or B: system push should be delivered.
- Enter the real `agent_room_id`, receive an agent room message: default room push rule should suppress system push unless the user has explicitly overridden the room push rule.
- Trigger an agent streaming response: one visible agent message should update while streaming, hide intermediate fragment cards after completion, and keep the final output body visible.
- Disable WS during read-marker submission: the client should fall back to the HTTP read-marker action instead of treating SSE no-op as success.
- Disable network and re-enable it: the client should reconnect with the last persisted `seq`, replay missed `server.event` frames, and keep local product state correct.
- Force a stale cursor beyond retention if available: the client should handle `server.cursor_reset`, run one `sync.bootstrap`, and resume from the recovered cursor.
