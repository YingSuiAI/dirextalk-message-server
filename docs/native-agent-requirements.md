# Native Agent Requirements

## Scope

Dirextalk message server embeds `io.dirextalk.agent` as a native server feature. The old Agent Docker/plugin-runtime reuse path is deprecated for Agent only. `dirextalk-plugins` is not changed in this version, and non-Agent plugins such as `io.dirextalk.ops` may continue to use the Docker plugin runner.

Clients keep the current call surface:

- `plugins.install`, `plugins.enable`, `plugins.disable`, `plugins.uninstall`, `plugins.config.get`, `plugins.config.update`, `plugins.health`
- `plugins.invoke` with `plugin_id=io.dirextalk.agent`
- `client.plugin_stream` over realtime WebSocket

## Runtime Requirements

- `io.dirextalk.agent` always routes to native runtime, never to the Docker Agent container.
- No migration from old Agent config or runtime state is required.
- Native Agent uses CloudWeGo Eino as the only model orchestration path. The runtime must track the latest stable Eino release, use Eino ReAct for model/tool loops, use maintained Eino model components for OpenAI and DeepSeek, use direct-only Anthropic Messages API as an Eino `ToolCallingChatModel` adapter, and use Eino official MCP tooling backed by `modelcontextprotocol/go-sdk`.
- Native Agent supports `openai`, `anthropic`, `deepseek`, and `openai_compatible`.
- `anthropic` first-version support is direct Anthropic API only. Bedrock and Vertex are intentionally not supported, and AWS/Google SDK dependencies must not be introduced for this provider.
- Requests may pass `model_profile` with `provider`, `model`, `base_url`, `api_key`, `temperature`, `top_p`, `max_output_tokens`, and `context_window`.
- DeepSeek defaults to the OpenAI-compatible endpoint `https://api.deepseek.com`.
- API keys are request-local or temporary environment values only. They must not be persisted, logged, committed, or returned by config APIs.
- System prompts come from plugin config, request overrides, and enabled static skills.
- `agent.chat` returns a complete response.
- `agent.chat.stream` emits `delta`, `error`, and `done` frames through the existing WebSocket protocol and respects client cancellation.

## Native Tools

The runtime exposes Dirextalk tools implemented against server internals:

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

## Skills

- Skills can be installed, listed, enabled, disabled, and uninstalled.
- Installed skill content is cached below the native Agent data directory.
- Only static `SKILL.md` text is read into the prompt. Remote scripts or arbitrary skill code are not executed.
- Skill install supports explicit `content` and URL/GitHub raw retrieval.

## MCP

- Third-party MCP servers can be installed, listed, enabled, disabled, and uninstalled.
- Supported transports are `stdio`, remote HTTP/SSE, and streamable HTTP.
- MCP tools discovered from enabled servers become dynamic Agent tools.
- MCP discovery and tool invocation must go through `github.com/cloudwego/eino-ext/components/tool/mcp/officialmcp` and `github.com/modelcontextprotocol/go-sdk/mcp`, not a custom JSON-RPC client.
- MCP server command/env configuration may be stored, but secrets must be passed through request-local values or temporary env references.

## Runtime CLI Tools

- Runtime CLI tools can be installed, recorded, found, and executed under the native Agent data directory.
- Supported actions include install, inspect, which, and run.
- Execution is bounded by timeout and returns stdout/stderr/exit status.
- Enabled installed runtime CLI tools are exposed to the Agent as Eino tools, so the model can call them inside the same orchestration loop and summarize their results.

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
  - Install and enable native Agent through plugin APIs.
  - `plugins.invoke -> agent.chat` returns a Chinese reply.
  - `client.plugin_stream -> agent.chat.stream` emits `delta` and `done`.
  - Skill install/list works and enabled skill text affects the system prompt.
  - MCP install/list works and a discovered MCP tool can be invoked by Agent.
  - Runtime CLI tool install/which/run works.
  - Built-in tools for contacts, rooms, messages, summaries, and sending messages work.
  - The temporary key does not appear in logs, docs, git diff, persisted config, or test output.

## Test Secret Handling

The DeepSeek API key supplied by the operator is a live secret. Use it only as an ephemeral environment variable or request-local `model_profile.api_key` during final interface testing. Do not write it to repository files, shell history snippets, docs, or logs. Recommend rotating the key after acceptance testing.
