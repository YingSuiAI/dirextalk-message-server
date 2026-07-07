# Native Agent Progress

Only mark an item after implementation and verification are complete. Do not pre-check planned or partially finished work.

## Requirements And Tests

- [x] Requirements document created in `docs/native-agent-requirements.md`.
- [x] Native Agent progress document created in `docs/native-agent-progress.md`.
- [x] Native Agent contract tests added before production implementation.
- [x] Final diff reviewed before commit.

## Runtime Integration

- [x] `io.dirextalk.agent` routes to native runtime and never to Docker runner.
- [x] Ops and future non-Agent plugins still route to Docker runner when enabled.
- [x] Agent plugin lifecycle APIs implement native install/enable/disable/uninstall/config/health semantics.
- [x] Existing `plugins.invoke` action supports native Agent calls.
- [x] Existing `client.plugin_stream` WebSocket path supports native Agent streaming and cancellation.
- [x] `P2P_NATIVE_AGENT_DATA_DIR` is supported with default `/var/dirextalk-message-server/agent`.
- [x] Docker compose includes a durable native Agent data volume/path.

## Agent Orchestration And Memory

- [x] Model orchestration loop supports model request, tool call, tool result feedback, and final answer.
- [x] Tool results can be summarized by a subsequent model call in the same loop.
- [x] Conversation memory stores recent user/assistant turns.
- [x] Context compression summarizes older turns and keeps a bounded recent window.
- [x] Streaming chat stores completed output in memory.

## Model And Prompt

- [x] Provider factory supports `openai`.
- [x] Provider factory supports `anthropic`.
- [x] Provider factory supports `deepseek`.
- [x] Provider factory supports `openai_compatible`.
- [x] Request-level `model_profile` supports provider/model/base_url/api_key/temperature/top_p/max_output_tokens/context_window.
- [x] System prompt supports config prompt, request override, and enabled skills.
- [x] API keys are request-local only and are not persisted/logged/returned.

## Tools

- [x] Built-in contacts list/search tool works.
- [x] Built-in rooms search tool works.
- [x] Built-in messages list tool works through read-only homeserver/sync DB context.
- [x] Built-in messages send tool writes through `p2p.Transport`.
- [x] Built-in room members tool works.
- [x] Built-in channel content tools work.
- [x] Built-in summarize tool works.

## Skills

- [x] Skill install works from explicit `SKILL.md` content.
- [x] Skill install works from URL/GitHub raw source.
- [x] Skills list/enable/disable/uninstall work.
- [x] Enabled skills are read statically into system prompt.
- [x] Remote skill scripts are not executed.

## MCP

- [x] MCP server install/list works.
- [x] `stdio` MCP transport works.
- [x] Remote HTTP/SSE MCP transport works.
- [x] Streamable HTTP MCP transport works.
- [x] Discovered MCP tools are callable by Agent.
- [x] MCP server enable/disable/uninstall work.

## Runtime CLI

- [x] Runtime CLI install records tool metadata under Agent data dir.
- [x] Runtime CLI install can run a bounded install command.
- [x] Runtime CLI which finds installed tools.
- [x] Runtime CLI run executes with bounded timeout and returns stdout/stderr/exit status.

## Verification

- [x] `go test ./p2p ./internal/productpolicy -count=1`.
- [x] `go test ./internal/httputil ./setup -count=1`.
- [x] `go test ./syncapi/storage ./syncapi/routing -count=1` if sync reader code is touched.
- [x] `go build ./cmd/dirextalk-message-server`.
- [x] `docker compose -f docker-compose.p2p.yml config`.
- [x] `python3 -m json.tool docs/postman/dirextalk-message-server.postman_collection.json >/dev/null`.
- [x] `git diff --check`.

## Real Interface Acceptance

- [x] Local service starts for native Agent testing.
- [x] Owner token installs/enables native Agent.
- [x] DeepSeek `plugins.invoke -> agent.chat` returns a Chinese reply.
- [x] DeepSeek `client.plugin_stream -> agent.chat.stream` emits `delta` and `done`.
- [x] Test skill install/list works and changes system prompt behavior.
- [x] Test MCP server install/list works and tool is callable.
- [x] Runtime CLI install/which/run works.
- [x] Built-in contacts, rooms, message list, summarize, and send tools are exercised.
- [x] Temporary DeepSeek key is absent from logs, docs, git diff, persisted config, and test output.
