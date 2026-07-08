# Native Agent Requirements

> Current contract: Native Agent is exposed through first-class owner `agent.*`
> product actions and realtime `client.native_agent_stream` /
> `client.native_agent_stream.cancel` frames. `io.dirextalk.agent` is not
> listed, installed, enabled, configured, invoked, checked for health, or tailed
> through the plugin catalog/list/lifecycle/invoke/log surfaces. Ops and future
> non-Agent plugins continue to use the plugin manager and Docker runner.
> Native Agent config storage uses the portal Agent config JSON; old hidden
> `io.dirextalk.agent` plugin config is only a sanitized, idempotent startup
> migration source.

## Scope

Dirextalk message server embeds Native Agent as a native server feature. The old Agent Docker/plugin-runtime reuse path is deprecated for Agent only. `dirextalk-plugins` is not changed in this version, and non-Agent plugins such as `io.dirextalk.ops` may continue to use the Docker plugin runner.

Clients use the current call surface:

- First-class owner `agent.*` body actions for Native Agent chat, model listing, runtime, skills, MCP, context compression, config patch proposal, and built-in Dirextalk tools.
- `client.native_agent_stream` over realtime WebSocket for Native Agent streaming, with `client.native_agent_stream.cancel` for cancellation.
- Standard external MCP clients call `POST /_p2p/mcp` with MCP Streamable HTTP JSON-RPC and `Authorization: Bearer <agent_token>`.
- Plugin manager actions remain for Ops and future non-Agent plugins only.

## Runtime Requirements

- Native Agent owner actions always route to the native runtime, never to a Docker Agent container.
- Native Agent runtime config is stored in native portal Agent config storage. On startup, old hidden Agent plugin config/runtime state is imported once in a sanitized, idempotent way; current clients must not use plugin management as the Native Agent contract.
- Native Agent uses CloudWeGo Eino as the only model orchestration path. The runtime must track the latest stable Eino release, use Eino ReAct for model/tool loops, use maintained Eino model components for OpenAI and DeepSeek, use direct-only Anthropic Messages API as an Eino `ToolCallingChatModel` adapter, and use Eino official MCP tooling backed by `modelcontextprotocol/go-sdk`.
- Native Agent supports `openai`, `anthropic`, `deepseek`, and `openai_compatible`.
- `anthropic` first-version support is direct Anthropic API only. Bedrock and Vertex are intentionally not supported, and AWS/Google SDK dependencies must not be introduced for this provider.
- Requests may pass `model_profile` with `provider`, `model`, `base_url`, `api_key`, `temperature`, `top_p`, `max_output_tokens`, and `context_window`.
- DeepSeek defaults to the OpenAI-compatible endpoint `https://api.deepseek.com`.
- API keys are request-local or temporary environment values only. They must not be persisted, logged, committed, or returned by config APIs.
- System prompts come from native Agent config, request overrides, and enabled static skills.
- `agent.chat` returns a complete response.
- Native stream emits `delta`, `error`, `trace`, and `done` events through `server.native_agent_stream.*` frames and respects client cancellation.
- Chat responses and stream completion payloads expose observable `steps` and `trace` data for UI display of context use, tool calls, tool results, and final output. Streamed chats also emit a `trace` event before `done`.
- Trace data must not expose hidden model chain-of-thought. It is limited to observable runtime progress, tool inputs/outputs, context metadata, and final answer previews.

## Native Tools

The runtime exposes Dirextalk tools generated from the shared `internal/dirextalkmcp` registry and invoked through the same capability service as fixed `mcp.*` body actions:

- `agent.contacts.list`
- `agent.contacts.search`
- `agent.rooms.search`
- `agent.messages.list`
- `agent.messages.send`
- `agent.room_members.list`
- `agent.channel_posts.list`
- `agent.channel_comments.list`
- `agent.channel_comments.create`
- `agent.summarize`

Matrix writes must continue through roomserver/`p2p.Transport`. Direct DB access is read-only and only for context/history/state material.

The external `POST /_p2p/mcp` transport must call the same `internal/dirextalkmcp` service as these built-in tools. Do not duplicate Dirextalk MCP business logic in Native Agent, fixed `mcp.*` body-action wrappers, or the MCP HTTP transport.

## Skills

- Skills can be installed, listed, enabled, disabled, and uninstalled.
- Installed skill content is cached below the native Agent data directory.
- Only static `SKILL.md` text is read into the prompt. Remote scripts or arbitrary skill code are not executed.
- Skill install supports explicit `content` and URL/GitHub raw retrieval.
- Agent conversations expose native skill management tools, so the model can install, list, enable, disable, and uninstall skills when the user explicitly asks for that operation. These management tools are base Agent capabilities and remain available even when older `enabled_tools` config/request values list only Dirextalk content tools.
- A newly installed or re-enabled skill affects the next Agent turn after the system prompt is rebuilt.

## MCP

- Third-party MCP servers can be installed, listed, enabled, disabled, and uninstalled.
- Supported transports are `stdio`, remote HTTP/SSE, and streamable HTTP.
- Dirextalk's own standard MCP server endpoint is `POST /_p2p/mcp`. It supports JSON-RPC `initialize`, `tools/list`, and `tools/call` over POST, requires `Authorization: Bearer <agent_token>`, rejects query-string tokens, validates `Origin`, returns 405 for GET/SSE while server-to-client streaming is unused, and must not pass the inbound bearer token to downstream services.
- MCP tools discovered from enabled servers become dynamic Agent tools.
- MCP discovery and tool invocation must go through `github.com/cloudwego/eino-ext/components/tool/mcp/officialmcp` and `github.com/modelcontextprotocol/go-sdk/mcp`, not a custom JSON-RPC client.
- MCP server command/env configuration may be stored, but secrets must be passed through request-local values or temporary env references.
- Agent conversations expose native MCP server management tools, so the model can install, list, enable, disable, and uninstall MCP servers when the user explicitly asks for that operation. These management tools are base Agent capabilities and remain available even when older `enabled_tools` config/request values list only Dirextalk content tools.
- MCP tools discovered during a dialogue install become callable on the next Agent turn after the Eino tool list is rebuilt.

## Runtime CLI Tools

- Runtime CLI tools can be installed, recorded, found, and executed under the native Agent data directory.
- Supported actions include install, inspect, which, and run.
- Execution is bounded by timeout and returns stdout/stderr/exit status.
- Install and run commands must work in minimal Alpine runtime images that provide `sh` but not `bash`; the official runtime image also installs `bash` for bash-based deployment scripts.
- Timed-out install and run commands must clean up their child process groups so dynamic dependency installs cannot continue indefinitely after the request is cancelled.
- Enabled installed runtime CLI tools are exposed to the Agent as Eino tools, so the model can call them inside the same orchestration loop and summarize their results.
- Agent conversations expose a built-in `runtime__shell` Eino tool for explicit command execution requests. Multi-step runtime workflows use configurable `max_tool_calls` or `max_steps` budgets with a server-side cap.

## Storage And Data Directory

- `P2P_NATIVE_AGENT_DATA_DIR` configures the Agent data directory.
- Default data dir is `/var/dirextalk-message-server/agent`.
- Docker compose must mount a durable Agent data volume for skills, MCP metadata, and runtime CLI tools.
- Homeserver/sync DB access is read-only. Native Agent must not write Matrix tables directly.

## Acceptance

- Automated checks pass:
  - `go test ./p2p ./internal/productpolicy -count=1`
  - `go test ./internal/httputil ./setup -count=1`
  - `go test ./syncapi/storage ./syncapi/routing -count=1` when sync reader code is touched
  - `go build ./cmd/dirextalk-message-server`
  - `docker compose -f docker-compose.p2p.yml config`
  - `python3 -m json.tool docs/postman/dirextalk-message-server.postman_collection.json >/dev/null`
  - `git diff --check`
- Real local interface testing passes with a temporary DeepSeek key:
  - Native Agent is absent from plugin catalog/list/lifecycle/invoke surfaces.
  - Direct `agent.chat` returns a Chinese reply.
  - Realtime `client.native_agent_stream` emits `delta`, `trace`, and `done`.
  - Skill install/list works and enabled skill text affects the system prompt.
  - MCP install/list works and a discovered MCP tool can be invoked by Agent.
  - Runtime CLI tool install/which/run works.
  - Built-in tools for contacts, rooms, messages, summaries, and sending messages work.
  - The temporary key does not appear in logs, docs, git diff, persisted config, or test output.

## Test Secret Handling

The DeepSeek API key supplied by the operator is a live secret. Use it only as an ephemeral environment variable or request-local `model_profile.api_key` during final interface testing. Do not write it to repository files, shell history snippets, docs, or logs. Recommend rotating the key after acceptance testing.
