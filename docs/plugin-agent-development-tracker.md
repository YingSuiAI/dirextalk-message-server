# Plugin Agent Development Tracker

Last updated: 2026-07-04

This tracker covers the staged implementation for the official plugin center, Agent plugin configuration, and the new server-backed Agent chat flow across:

- `dirextalk-message-server`
- `dirextalk-plugins`
- `direxio-flutter`

Do not mark a task complete until the code is implemented and the listed verification has run or the exact blocker is recorded.

## Stage 0 - Workspace And Baseline

- [x] Create message-server branch `feat/plugin-agent-invoke` from `feat/plugin-system`.
- [x] Create Flutter branch `feat/plugin-management-agent` from latest `origin/main`.
- [x] Create plugins branch `feat/agent-streaming-invoke` from latest `origin/main`.
- [x] Install and verify local Flutter/Dart tooling:
  - Flutter `3.44.4` stable at `/opt/flutter`
  - Dart `3.12.2`
- [x] Write initial failing message-server plugin tests for:
  - owner-only plugin invoke
  - WS plugin stream response
  - write-only direct API key handling
  - runtime secret env injection
- [x] Record initial plugin Python test dependency status: plugin `.venv` is usable and `.venv/bin/python -m pytest` runs.

## Stage 1 - Message Server Plugin Contract

- [x] Add durable plugin secret storage with migration and restart-safe storage tests.
- [x] Ensure direct API key input is stored only as write-only plugin secret, not in persisted plugin config.
- [x] Return `secret_status` from `plugins.config.get` without leaking secret values.
- [x] Add `plugins.invoke` owner-only HTTP action for enabled official plugins.
- [x] Add `client.plugin_stream` owner-only WS frame for streaming Agent replies; HTTP `plugins.invoke.stream` returns `action requires websocket`.
- [x] Extend plugin runner with HTTP invoke and WebSocket streaming invoke against the official plugin container.
- [x] Keep all `plugins.*` actions unavailable to `agent_token`; plugin stream uses owner WS ticket only.
- [x] Update message-server docs and Postman examples for new actions, WS stream boundary, and secret handling.
- [x] Verification:
  - [x] `gofmt` on touched Go files
  - [x] `go test ./p2p -run 'TestPlugin|TestRealtimeWSRequestCoverageMatchesActionRegistry' -count=1`
  - [x] Docker PostgreSQL-backed `go test ./p2p -run 'TestDatabaseStorePersistsPluginsAndJobs' -count=1`
  - [x] Docker PostgreSQL-backed `go test ./p2p -count=1`
  - [x] `go build ./cmd/dirextalk-message-server`
  - [x] Postman JSON validation

## Stage 2 - Official Agent Plugin Runtime

- [x] Add `agent.chat.stream` invoke handling.
- [x] Support streamed model output when the model runtime provides chunks.
- [x] Support per-request `model_profile_id`.
- [x] Support `AGENT_MODEL_PROFILES_JSON` for multiple provider/model profiles.
- [x] Keep provider/model/key user-selectable; do not hardcode DeepSeek as default.
- [x] Preserve non-streaming `agent.chat` compatibility.
- [x] Verification:
  - [x] Python unit tests for profile selection
  - [x] Python unit tests for stream event shape
  - [x] Python unit tests for missing key/model errors
  - [x] `.venv/bin/python -m pytest`

## Stage 3 - Flutter Product API Client

- [x] Add plugin catalog, installed plugin, config, invoke, and stream models.
- [ ] Add plugin job, health, and logs models when backend exposes first-class client-facing endpoints.
- [x] Add `AsClient` methods for plugin management and Agent streaming invoke.
- [x] Implement `HttpAsClient` body-action mappings for `plugins.*`.
- [x] Force `plugins.*` management actions to HTTP-only, never generic realtime `client.request`.
- [x] Redact `secrets`, `api_key`, and attachment payload-like fields from API logs.
- [x] Update `MockAsClient` with in-memory plugin state for widget/provider tests.
- [x] Add realtime WS transport support and tests for `client.plugin_stream`, stream events, and cancel frames.
- [x] Update `docs/P2P_API_BOUNDARY.md` and `docs/FEATURES.md`.
- [ ] Verification:
  - [x] `flutter pub get`
  - [x] `flutter test --no-pub test/http_as_client_test.dart`
  - [x] `flutter test --no-pub test/as_realtime_transport_test.dart`
  - [x] `flutter analyze --no-pub`

## Stage 4 - Flutter Plugin Center UI

- [x] Add Settings -> Extensions & Plugins -> Plugin Management entry.
- [x] Add plugin list page backed by service catalog and installed state.
- [x] Add install/configure, enable, and disable flows with visible loading/error states.
- [ ] Add uninstall confirmation flow in the UI; provider/API method exists.
- [x] Add Agent plugin detail/config page:
  - provider selection from first-version provider list
  - free-form model input
  - write-only API key field
  - system prompt editor
  - enabled tools toggles
  - skills configuration
  - MCP servers configuration
  - model profile JSON management
- [ ] Add schema-driven provider options and explicit default model profile selector.
- [ ] Verification:
  - [x] `flutter test --no-pub test/widget_test.dart --plain-name 'settings page matches Dirextalk settings sections'`
  - [ ] focused widget/provider tests for plugin center
  - [ ] focused widget/provider tests for Agent config page

## Stage 5 - Flutter Agent Chat UI

- [x] Add third-party AI chat UI dependency after public API compatibility check.
- [x] Prefer `flutter_gen_ai_chat_ui` unless dependency resolution proves incompatible.
- [x] Add new server-backed Agent chat page independent of old connect/Codex Agent room flow.
- [x] Support:
  - new conversation
  - model profile switching
  - streaming output
  - stop generation
  - Markdown rendering
  - file/image attachment selection metadata
- [ ] Add local conversation list if product requires multiple retained local threads.
- [ ] Add real attachment upload/content transport after a server attachment contract exists.
- [x] Keep first-version Agent chat history local-only; no server persistence contract was added.
- [ ] Verification:
  - [ ] focused widget/provider tests for streaming chunk merge
  - [ ] focused widget/provider tests for model profile switch
  - [ ] focused widget/provider tests for attachment validation

## Stage 6 - Final Integration

- [x] Run all available focused checks in each repository.
- [x] Record unavailable/full-suite checks:
  - Full `test/widget_test.dart` was not used as the completion gate because
    it currently has broad non-plugin failures on Flutter `3.44.4`; focused
    plugin/realtime/settings checks are recorded above.
- [x] Review diffs for secret leaks:
  - repository scan only found the fake `sk-plugin-secret` unit-test fixture.
- [x] Confirm no unrelated user changes were reverted.
- [ ] Prepare commit/PR summary across the three repositories.

Verification commands run:

- `gopls check` on touched Go files.
- `go test ./p2p -run 'TestPlugin|TestRealtimeWSRequestCoverageMatchesActionRegistry' -count=1`.
- Docker PostgreSQL-backed
  `go test ./p2p -run 'TestDatabaseStorePersistsPluginsAndJobs' -count=1`.
- Docker PostgreSQL-backed `go test ./p2p -count=1`.
- `go build ./cmd/dirextalk-message-server`.
- Postman JSON validation for message-server and plugin collections.
- `docker compose -f docker-compose.p2p-dual.yml -f docker-compose.p2p-dual.plugins.yml config`.
- Plugin runtime `.venv/bin/python -m pytest`.
- Plugin catalog JSON validation.
- Plugin `docker compose -f deploy/docker-compose.agent.yml config` with
  placeholder required env vars.
- `flutter pub get`.
- `flutter analyze --no-pub`.
- `flutter test --no-pub test/http_as_client_test.dart`.
- `flutter test --no-pub test/as_realtime_transport_test.dart`.
- `flutter test --no-pub test/widget_test.dart --plain-name 'settings page matches Dirextalk settings sections'`.
- `git diff --check` in message-server, plugins, and Flutter repositories.

## Stage 7 - Local Docker x1 Installation Verification

Target: the local Docker `dirextalk-p2p-dual-dendrite-a-1` service published by
Caddy as `https://x1.dirextalk.ai`.

- [x] Build and push official Agent plugin image
  `dirextalk/agent-plugin:0.1.0-ws-20260704`.
- [x] Pin catalog digest to
  `sha256:d7f5d0fdc8878bf173c79968ea9db8ec6a4fb23d872cdcfde664055d2e0baddd`.
- [x] Build local `dirextalk/message-server:latest` with plugin management and
  plugin stream support.
- [x] Recreate only local Docker `dendrite-a` with the Docker plugin runner
  enabled and the host Docker socket mounted.
- [x] Install official Agent plugin through `plugins.install`.
- [x] Configure Agent plugin with user-selected DeepSeek provider/model profile
  and write-only API key secret.
- [x] Confirm `plugins.config.get` returns `secret_status.api_key.configured`
  without returning the key value.
- [x] Enable the Agent plugin and confirm the runtime container
  `dirextalk-plugin-agent` is reachable from the message-server container.
- [x] Verify non-streaming `plugins.invoke` `agent.chat` against DeepSeek:
  response `插件连通性测试成功。`.
- [x] Verify WS `client.plugin_stream` `agent.chat.stream` against DeepSeek:
  streamed response `WS 流式插件测试成功。`.
- [x] Verify HTTP `plugins.invoke.stream` is rejected with
  `action requires websocket`.
