# Direxio Agent CLI Design

## Purpose

Direxio needs first-party tooling that lets Developer Operators use a Direxio node from agent-oriented tools such as Codex, Claude Code, OpenClaw, and Hermes-style plugins. The first version is CLI-first: it provides a cross-platform Go binary with domain commands for P2P product workflows and Matrix Client-Server workflows, while keeping MCP as a thin optional adapter.

## Target User

The first version targets Developer Operators. They manually provide:

- `DIREXIO_DOMAIN`, such as `https://example.com`
- `DIREXIO_AGENT_TOKEN`, a portal-owned Agent token with permission for the requested actions

The CLI derives `/_p2p` and `/_matrix` route bases internally. Users do not configure `/_p2p` base URLs, Matrix base URLs, portal passwords, or Matrix access tokens.

## Goals

- Ship the tooling from this repository as an independent Agent Tools Package.
- Build the CLI in Go and release cross-platform binaries.
- Cover the relevant P2P product surface with first-class domain commands.
- Cover Matrix Client-Server workflows needed by agents, including session initialization, message sending, message listing, one-shot sync, and continuous listen.
- Keep `direxio p2p action` as a fallback for body-action calls not yet represented by domain commands.
- Provide high-quality `help` output for root commands and subdomain commands.
- Ship agent skill recipes that teach intelligent agent tools how to use the CLI safely.
- Keep successful command output machine-readable.

## Non-Goals

- Do not embed a full MCP service into the Dendrite monolith.
- Do not mirror all P2P actions as MCP tools.
- Do not require users to provide Matrix access tokens.
- Do not print Matrix access tokens from normal CLI commands.
- Do not add new URL-shaped P2P product endpoints.
- Do not build the App-to-user-agent Gateway in the first CLI phase.

## Architecture

The Agent Tools Package should be organized as separate tool code inside the current repository:

```text
cmd/direxio-cli
cmd/direxio-mcp
internal/agentclient
docs/agent-skills
scripts/build-agent-tools.ps1
scripts/build-agent-tools.sh
```

`internal/agentclient` owns shared HTTP behavior:

- Normalize `DIREXIO_DOMAIN`
- Build P2P and Matrix route URLs
- Attach `Authorization: Bearer <agent_token>` for P2P calls
- Request an internal Agent Matrix Session when Matrix APIs are needed
- Send JSON requests and decode JSON responses
- Return structured errors for CLI rendering

`cmd/direxio-cli` is the primary user interface. `cmd/direxio-mcp` can reuse the same client, but should expose only curated, low-risk tools. Skills and recipes should prefer CLI workflows over raw MCP coverage.

## Server Contract Needs

Matrix workflows require the CLI to obtain a Matrix access token internally without asking the user to log in. Add a protected P2P action for this purpose, recommended as:

```text
agent.matrix_session.create
```

The action should:

- Require a valid Agent token.
- Be permission-gated through `defaultAPIPermissions()`.
- Return the Matrix session needed by the CLI to call `/_matrix/client/*`.
- Represent the portal owner/current Agent-token owner, not an arbitrary app user.
- Be documented as sensitive because the response contains a Matrix access token.

The CLI must consume the token internally and must not print it as a normal command result.

When this contract is implemented, update:

- `p2p.Service.Handle`
- `defaultAPIPermissions()`
- Focused tests for Agent token authorization and response behavior
- `docs/postman/direxio-message-server.postman_collection.json`
- `docs/api-interface-change-record.md`

## CLI Command Shape

The accepted command tree is:

```text
direxio auth status
direxio init

direxio p2p action <action> --params '{}'
direxio p2p apis
direxio p2p sync-bootstrap

direxio contacts list
direxio channels list
direxio channels public-search
direxio groups list

direxio matrix session init
direxio matrix messages send --room-id ... --text ...
direxio matrix messages list --room-id ... --limit 50
direxio matrix sync --timeout 30s
direxio matrix listen
```

The domain commands are the primary intelligent-agent workflow. `direxio p2p action` is a compatibility and advanced-operator fallback.

## Help and Examples

Every command group must support `help` and include:

- What the command does
- Required environment variables
- Important flags
- At least one copyable example
- Common error hints, especially missing credentials, disabled Agent permission, unauthorized action, invalid room ID, and Matrix sync timeout

Examples:

```powershell
direxio help
direxio channels help
direxio matrix messages help
direxio p2p action help
```

Help text should be useful to humans and to agent tools that inspect commands before running them.

## Output Contract

Successful non-streaming commands print pretty JSON to stdout by default:

```json
{
  "channels": []
}
```

`--raw` prints compact JSON for scripts. Errors are written to stderr and return non-zero exit codes. Stdout must remain reserved for successful result data.

`direxio matrix listen` should emit newline-delimited JSON, one event per line, so long-running agent bridges and shell pipelines can process events incrementally.

## Matrix Message Workflows

Message send, message listing, one-shot sync, and continuous listening use Matrix Client-Server APIs under `/_matrix/*`.

The CLI uses the internal Agent Matrix Session for these calls:

- `matrix messages send` sends ordinary Matrix messages.
- `matrix messages list` retrieves room history for one room.
- `matrix sync --timeout` performs a bounded sync call for new events.
- `matrix listen` continuously syncs and emits NDJSON events.

Do not add a second P2P ordinary-message source. P2P remains responsible for product projections, while ordinary messages use Matrix APIs.

## MCP Adapter

The MCP adapter is optional and thin. It should expose only curated workflows that are safe and common for AI use, such as listing contacts, listing channels, searching public channels, and reading recent messages.

It should not expose all P2P actions. Destructive or high-risk operations such as delete, dissolve, remove, mute, redaction, or approval should remain CLI workflows with explicit user intent unless a later design adds confirmation semantics.

## Agent Skill Recipes

Ship task-oriented recipes alongside the CLI:

```text
docs/agent-skills/codex-direxio-cli.md
docs/agent-skills/claude-code-direxio-cli.md
docs/agent-skills/openclaw-direxio-cli.md
docs/agent-skills/hermes-direxio-cli.md
.codex/skills/direxio-cli/SKILL.md
```

Recipes should explain how to:

- Verify credentials and node reachability
- Initialize Matrix connectivity
- List contacts, groups, and channels
- Send a Matrix message
- Fetch or listen for Matrix messages
- Use `direxio p2p action` for unsupported domain commands
- Ask for user confirmation before risky actions

Recipes should not become a hidden API contract. The CLI help and repository docs remain the source of truth.

## Cross-Platform Build

Provide PowerShell and POSIX shell build scripts that produce OS and architecture-specific binaries. The first build matrix should cover:

- Windows amd64
- Windows arm64
- Linux amd64
- Linux arm64
- macOS amd64
- macOS arm64

The scripts should compile `cmd/direxio-cli` first. `cmd/direxio-mcp` can be added to the same build scripts once the thin adapter is implemented.

## Verification

The implementation should include:

- Unit tests for domain normalization and route derivation.
- Unit tests for CLI output formatting and stderr behavior.
- Unit tests for P2P action fallback request construction.
- Focused P2P tests for `agent.matrix_session.create` once server support is added.
- JSON validation for generated docs or recipe examples where practical.
- `go test` for touched Go packages.
- Cross-platform build script dry runs on the local platform.

Full lint can be run when `golangci-lint` is installed. Otherwise use `gofmt` and record that full lint was skipped.

## Phasing

Phase 1 builds the Go CLI, shared client, help text, agent recipes, and server-side Agent Matrix Session contract.

Phase 2 adds the thin MCP adapter over curated workflows.

Phase 3 designs the App Gateway for connecting the app directly to a user's own agent. The Gateway is a separate service concern and should not be mixed into the first CLI implementation.
