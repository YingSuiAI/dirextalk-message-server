# Current Agent and MCP Contract

This document is the backend-owned current contract for Dirextalk Agent state, Native Agent, and external MCP access. It describes existing behavior; it does not add a compatibility surface.

## External MCP

- External MCP clients use the standard Streamable HTTP endpoint `POST /mcp`. `/_p2p/mcp` is unavailable.
- Requests are MCP JSON-RPC and currently support `initialize`, `tools/list`, and `tools/call`.
- Authentication is `Authorization: Bearer <agent_token>`. The owner access token is not accepted, bearer tokens in query parameters are rejected, and the inbound bearer token must not be forwarded in tool arguments or to downstream services.
- The endpoint validates `Origin`. `GET /mcp` returns method not allowed while server-to-client streaming is unused.
- Fixed `mcp.*` body actions are removed from `/_p2p/query` and `/_p2p/command`. Any `mcp.*` action identifiers that remain in backend packages are internal capability identifiers, not callable product actions.
- `POST /mcp` and Native Agent built-in Dirextalk tools share the backend `internal/dirextalkmcp` registry, schemas, pagination, room authorization, DTOs, and invocation service. Room IDs in `mcp_blocked_room_ids` are filtered from discovery and rejected on direct access.
- The only Cloud MCP capabilities are read-only `dirextalk_cloud_workloads_list`, `dirextalk_cloud_workloads_get`, and `dirextalk_cloud_status`. They return whitelisted Plan/Deployment/Service summaries, de-secretsed Job aggregate counts, and alert metadata; they never return Goal prompts, Plan narratives, outbox data, account metadata, credential/secret references, pairing data, or service secrets. No Cloud create, approval, networking, operation, or destroy tool is registered.

## Agent Room Status

- The configured `agent_room_id` is a real private Matrix room containing the owner and local `@agent:<server>` user.
- Bridge availability is Matrix room state type `io.dirextalk.agent.status`, state key `@agent:<server>`, with content field `online`.
- The running bridge publishes `online=true` or `online=false` through its Matrix session. Server startup or repair and `agent.config.update enabled=false` may publish `online=false` as a safe fallback.
- The server does not infer bridge availability from Agent configuration, `/sync`, realtime WebSocket lifetime, or Matrix presence. `sync.bootstrap` returns `agent_room_id`, not `agent_online`, and does not emit `agent.presence`.

## Native Agent Ownership

- Native Agent is owned by `dirextalk-message-server`. The backend owns native `agent.*` actions, `client.native_agent_stream` / `server.native_agent_stream.*` frames, model-provider request handling, skills, external MCP client wiring, runtime CLI tools, orchestration, built-in Dirextalk tools, native config storage, and sanitized migration from the former hidden Agent plugin config.
- Native Agent is not installed, enabled, configured, or invoked through `plugins.*`. Backend `plugins.*` actions remain for non-Agent plugins.
- Model API keys are request-scoped inputs. The message server must not persist, return, or inject them into plugin or runtime environment state.
- The server-side Eino Native Agent includes a built-in Cloud Deployment Planner skill with `native_agent_cloud_deployment_plan` and the de-secretsed read-only `native_agent_cloud_status` tool for an explicit cloud-workload request. It requires an existing Cloud Connection before it writes a durable `researching` Goal and queued research Job. An existing Connection is established only by the owner-only CloudFormation Role Plan and signed Broker-registration flow; neither Agent nor MCP can invoke that flow. The model can produce only an experimental Recipe and non-price candidate draft; a separate Orchestrator may later obtain a typed read-only quote through the Connection Stack. The Agent reads Cloud projections through narrow ports, rejects credential-shaped text, and cannot approve spend, call AWS, open ingress, operate a deployment, or destroy resources. Its runtime gets an isolated home and rejects direct or common wrapped AWS CLI invocation. It is not a Codex workspace Skill and it is not an external MCP tool.
- A request may opt into `cloud_dialogue_mode=true` for a Cloud planning conversation. This is a capability reduction, not an authorization grant: the Eino tool list contains only `native_agent_cloud_deployment_plan` and `native_agent_cloud_status`; runtime shell/CLI tools, configured external MCP tools, dynamic Skill/MCP management tools, ordinary Dirextalk tools, request/config system prompts, installed Skill prompts, and conversation memory are excluded. This mode may create only a connection-bound research Goal/queued Job or read a de-secretsed Cloud snapshot and never bypasses device-signed Plan approval or the typed Cloud control plane.

## Consumer Boundaries

- `dirextalk-connect` owns the local conversation bridge. It consumes the Matrix session and real `agent_room_id` for message sync/send and consumes the deployed `https://<server>/mcp` endpoint only through a runtime capability that supports connect-managed MCP. Host-managed runtimes keep MCP enrollment in their host runtime.
- `dirextalk-deployer` creates the Agent Matrix session, writes service-scoped `dirextalk-connect` configuration, records the canonical `/mcp` endpoint and Agent bearer credential, and generates only the runtime-specific MCP artifacts allowed by the capability registry.
- Neither consumer owns MCP business logic. They must not recreate a local MCP CLI, daemon, proxy, stdio bridge, listening port, fixed `mcp.*` product action path, or alternate endpoint.
- Flutter reads Online Agent availability from Matrix state in `agent_room_id` and uses backend-owned `agent.*` actions and native stream frames for Native Agent. It does not call fixed `mcp.*` product actions.

## vNext Legacy Matrix Gateway Foundation (Release Gate M)

- The internal Gateway adapter accepts only owner-authored `io.dirextalk.agent.invoke.v1` timeline events from the configured real `agent_room_id`. Its consumer uses an independent JetStream durable, so an Agent Control outage cannot block normal ProductCore projections.
- Invoke content is capped at 16 KiB and strictly contains `request_id`, `installation_id`, optional `preferred_connector_id`, `dispatch_mode`, `grant_version`, `input_event_id`, `required_capabilities`, and `idempotency_key`. UUIDs are canonical UUIDv7; capabilities are bounded, lowercase, unique, and sorted. Unknown/duplicate fields, trailing JSON, unsafe grant versions, the wrong room, and non-owner senders are ignored without creating a Run.
- PostgreSQL migration v38 stores one reservation per `(matrix_room_id, request_id)`, with unique source event and tenant/room/idempotency digest constraints. It stores the local Matrix input reference and normalized routing facts, but never the prompt body or raw idempotency key. Crash replay returns the first generated opaque request event and request digest; accepted/rejected terminal facts are source-digest fenced and immutable.
- Agent Control is called through the frozen `dirextalk.agent_gateway.v1.AgentRunIngress/CreateAgentRun` contract. The client requires TLS 1.3, HTTP/2, explicit server roots, a clientAuth-only certificate with exactly `spiffe://dirextalk.internal/v1/tenants/<tenant>/services/legacy-matrix-gateway`, 64 KiB message limits, and a deadline no longer than 20 seconds. Request and idempotency digests use the frozen LP/COMMIT transcript.
- The production monolith does not expose a startup switch for this adapter yet. Activation remains deliberately unavailable until deployment can prove the old Connect room consumer is stopped and fenced; otherwise one Matrix input could execute through both paths.
- This foundation durably creates or replays an Agent Run. Exclusive-consumer cutover, Run completion/evidence ingress, `io.dirextalk.agent.result.v1` / `io.dirextalk.agent.error.v1` projection, and restricted plain-text fallback remain later Release Gate M work; the server must not fabricate completion or evidence from an admission receipt.
