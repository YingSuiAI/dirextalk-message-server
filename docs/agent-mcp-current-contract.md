# Current Agent and MCP Contract

This document is the backend-owned current contract for Dirextalk Agent state, Native Agent, and external MCP access. It describes existing behavior; it does not add a compatibility surface.

## External MCP

- External MCP clients use the standard Streamable HTTP endpoint `POST /mcp`. `/_p2p/mcp` is unavailable.
- Requests are MCP JSON-RPC and currently support `initialize`, `tools/list`, and `tools/call`.
- Authentication is `Authorization: Bearer <agent_token>`. The owner access token is not accepted, bearer tokens in query parameters are rejected, and the inbound bearer token must not be forwarded in tool arguments or to downstream services.
- The endpoint validates `Origin`. `GET /mcp` returns method not allowed while server-to-client streaming is unused.
- Fixed `mcp.*` body actions are removed from `/_p2p/query` and `/_p2p/command`. Any `mcp.*` action identifiers that remain in backend packages are internal capability identifiers, not callable product actions.
- `POST /mcp` and Native Agent built-in Dirextalk tools share the backend `internal/dirextalkmcp` registry, schemas, pagination, room authorization, DTOs, and invocation service. Room IDs in `mcp_blocked_room_ids` are filtered from discovery and rejected on direct access.

## Agent Room Status

- The configured `agent_room_id` is a real private Matrix room containing the owner and local `@agent:<server>` user.
- Bridge availability is Matrix room state type `io.dirextalk.agent.status`, state key `@agent:<server>`, with content field `online`.
- The running bridge publishes `online=true` or `online=false` through its Matrix session. Server startup or repair and `agent.config.update enabled=false` may publish `online=false` as a safe fallback.
- The server does not infer bridge availability from Agent configuration, `/sync`, realtime WebSocket lifetime, or Matrix presence. `sync.bootstrap` returns `agent_room_id`, not `agent_online`, and does not emit `agent.presence`.

## Native Agent Ownership

- Native Agent is owned by `dirextalk-message-server`. The backend owns native `agent.*` actions, `client.native_agent_stream` / `server.native_agent_stream.*` frames, model-provider request handling, skills, external MCP client wiring, runtime CLI tools, orchestration, built-in Dirextalk tools, native config storage, and sanitized migration from the former hidden Agent plugin config.
- Native Agent is not installed, enabled, configured, or invoked through `plugins.*`. Backend `plugins.*` actions remain for non-Agent plugins.
- Model API keys are request-scoped inputs. The message server must not persist, return, or inject them into plugin or runtime environment state.

## Consumer Boundaries

- `dirextalk-connect` owns the local conversation bridge. It consumes the Matrix session and real `agent_room_id` for message sync/send and consumes the deployed `https://<server>/mcp` endpoint only through a runtime capability that supports connect-managed MCP. Host-managed runtimes keep MCP enrollment in their host runtime.
- `dirextalk-deployer` creates the Agent Matrix session, writes service-scoped `dirextalk-connect` configuration, records the canonical `/mcp` endpoint and Agent bearer credential, and generates only the runtime-specific MCP artifacts allowed by the capability registry.
- Neither consumer owns MCP business logic. They must not recreate a local MCP CLI, daemon, proxy, stdio bridge, listening port, fixed `mcp.*` product action path, or alternate endpoint.
- Flutter reads Online Agent availability from Matrix state in `agent_room_id` and uses backend-owned `agent.*` actions and native stream frames for Native Agent. It does not call fixed `mcp.*` product actions.
