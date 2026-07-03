# cc-connect Matrix Agent Room Bridge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run Dirextalk's `dirextalk-connect` bridge against the current Dirextalk agents Matrix room so users can talk to the local agent directly through Matrix-native room messages.

**Architecture:** Dirextalk issues a dedicated Matrix Client-Server session for the local `@agent:<server>` account. `dirextalk-connect` reuses cc-connect's existing `platform/matrix` implementation for sync, replies, media, typing, and session context, but is restricted to the backend-created real `agent_room_id`. The deployer creates the agent Matrix session, writes a service-scoped Matrix-only config, and installs `@dirextalk/connent` when `DIREXTALK_AGENT_INSTALL=auto`.

**Packaging:** Dirextalk distribution uses npm package `@dirextalk/connent`, binary command `dirextalk-connect`, GitHub repository `https://github.com/YingSuiAI/connect`, and Homebrew docs `brew install dirextalk-connect`. Non-Dirextalk chat platform docs and platform code are intentionally removed.

---

### Task 1: Backend Agent Matrix Session

**Files:**
- `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server\p2p\service_auth_api.go`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server\p2p\password_test.go`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server\docs\api-interface-change-record.md`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server\docs\current-project-documentation.md`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-message-server\docs\postman\dirextalk-message-server.postman_collection.json`

- [x] Update `agent.matrix_session.create` so `createAgentMatrixSession` calls `EnsureMatrixSession` for `s.agentMXIDLocked()` using the configured agent display name, not the portal owner profile.
- [x] Keep `revokeExistingDevices=false` so the portal owner phone/session is not evicted.
- [x] Return `access_token`, `device_id`, `user_id`, and `homeserver`; do not expose portal password or `agent_token`.
- [x] Update tests so the agent Matrix session test expects `@agent:<server>`.
- [x] Run `go test ./p2p -run 'TestAgentMatrixSession|TestDendriteMatrixSessionIssuer' -count=1`.

### Task 2: Dirextalk cc-connect Matrix Bridge

**Files:**
- `C:\Users\84960\Desktop\dirextalk\cc-connect\platform\matrix\matrix.go`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\platform\matrix\matrix_test.go`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\npm\package.json`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\npm\install.js`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\npm\run.js`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\README.md`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\INSTALL.md`
- `C:\Users\84960\Desktop\dirextalk\cc-connect\Makefile`

- [x] Add optional `room_id` / `allowed_room_id` Matrix platform config. When set, ignore all rooms except that room and make `ReconstructReplyCtx` reject other room IDs.
- [x] Keep existing Matrix behavior for sync, media downloads/uploads, typing, markdown rendering, edits, E2EE hooks, auto-join, and reply context.
- [x] Delete non-Dirextalk platform packages, plugin registration files, web UI, and platform docs.
- [x] Rename distribution assets to `dirextalk-connect` and publish docs to `@dirextalk/connent` / `brew install dirextalk-connect`.
- [x] Run targeted Matrix/config/core tests, `npm pack --dry-run`, and `make build`.

### Task 3: Deployer cc-connect Wiring

**Files:**
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\scripts\phases\s5_init_tokens.sh`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\scripts\phases\s6_wire_local.sh`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\scripts\orchestrate.sh`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\tests\s6_wire_local_test.sh`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\tests\skill_structure_test.sh`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\README.md`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\SKILL.md`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\references\runtime-wiring.md`
- `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer\references\agent-targets.md`

- [x] Remove every `!agent:<domain>` fallback. Missing or legacy `agent_room_id` fails closed.
- [x] Add `_create_cc_connect_matrix_session` that calls `POST /_p2p/command` with action `agent.matrix_session.create` using owner `access_token`.
- [x] Add runtime mapping from deployer runtime names to cc-connect agent names, especially `claude-code -> claudecode`.
- [x] Write `~/.dirextalk/nodes/<service_id>/cc-connect/config.toml` with one project, detected agent, `work_dir`, Matrix `homeserver`, Matrix `access_token`, `user_id`, `room_id`, `share_session_in_channel=true`, `group_reply_all=true`, `auto_join=false`, and `auto_verify=false`.
- [x] Add source-build install helpers using `DIREXTALK_CC_CONNECT_REPO`, `DIREXTALK_CC_CONNECT_REF`, and `DIREXTALK_CC_CONNECT_DIR`. Build with `AGENTS=<mapped> PLATFORMS_INCLUDE=matrix NO_WEB=1 make build-noweb`.
- [x] Replace old agent plugin/gateway summary with cc-connect daemon/config/binary status.
- [x] Run `bash tests/s6_wire_local_test.sh`, `bash tests/skill_structure_test.sh`, and shell syntax checks.

### Task 4: Real Deployment and Functional Check

**Files:**
- Existing deployer stack and runtime state under `C:\Users\84960\Desktop\dirextalk\dirextalk-deployer`.

- [ ] Run or reuse a single-node deployer flow with `DIREXTALK_AGENT_INSTALL=auto`.
- [ ] Verify the deployed server returns real `agent_room_id`, and `agent.matrix_session.create` returns `user_id="@agent:<server>"`.
- [ ] Start the local `dirextalk-connect` daemon with the generated config.
- [ ] Send a message into the agents room from a normal Matrix/IM user and confirm `dirextalk-connect` receives it and posts a reply as `@agent:<server>`.
- [ ] Confirm media/text behavior is not regressed for plain Matrix platform tests.

### Task 5: Final Review and Commits

**Files:**
- All touched files across `dirextalk-message-server`, `cc-connect`, and `dirextalk-deployer`.

- [ ] Run `git diff --check` in each changed repository.
- [ ] Run targeted Go/deployer tests from Tasks 1-3.
- [ ] Commit each repository on the `cc-connect` branch with focused commit messages.
- [ ] Report any real-deployment limitation explicitly if local cloud credentials, Docker, GitHub release assets, npm publish state, or the local agent runtime blocks the end-to-end check.
