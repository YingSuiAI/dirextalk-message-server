# Online MCP Service Design

## Goal

Embed the existing Dirextalk MCP tool surface into the Dirextalk Message Server monolith as an online MCP service.

The user-visible result is a standard MCP Streamable HTTP endpoint at `POST /_p2p/mcp`. MCP-capable agents can connect directly to the message server with `Authorization: Bearer <agent_token>` and call curated Dirextalk chat tools without running the sibling `dirextalk-mcp` Node adapter.

## Context

The current Go service already owns the backend MCP actions:

- `mcp.rooms.search`
- `mcp.messages.send`
- `mcp.messages.list`
- `mcp.room_members.list`
- `mcp.channel_posts.list`
- `mcp.channel_comments.list`
- `mcp.channel_comments.create`

Those actions are protected HTTP body actions. The owner `access_token` can call them through `/_p2p/query` or `/_p2p/command`; `agent_token` can call only `agent.matrix_session.create` and the fixed MCP action set. MCP actions must not move into `/_p2p/ws`, and `agent_token` must not gain access to owner product actions.

The sibling `../dirextalk-mcp` package is a TypeScript adapter that registers MCP tools and forwards them to the same backend actions. The monolith should reuse that tool contract but implement the online protocol in Go.

## Approach

Use the official Go MCP SDK package `github.com/modelcontextprotocol/go-sdk/mcp` to expose a Streamable HTTP MCP endpoint.

Add a focused Go adapter in the `p2p` package, because the adapter needs direct access to `Service.Handle`, current MCP authorization rules, and P2P route registration. The adapter should not add new durable state and should not bypass the existing MCP handlers. It should validate tool inputs, apply the same local defaults as `dirextalk-mcp`, call the existing `mcp.*` action, and return concise JSON text content.

## HTTP Contract

Add:

```text
POST /_p2p/mcp
Authorization: Bearer <agent_token>
Content-Type: application/json
```

The endpoint serves MCP Streamable HTTP requests. It is not a replacement for:

- `POST /_p2p/query`
- `POST /_p2p/command`
- `GET /_p2p/ws`

Only `agent_token` is accepted for the new MCP endpoint. The owner `access_token` is intentionally rejected on `/_p2p/mcp` so the online MCP service remains a narrow agent-facing surface rather than a second owner product API. Existing owner-token access to `mcp.*` body actions remains unchanged on `/_p2p/query` and `/_p2p/command`.

`OPTIONS /_p2p/mcp` should return the same CORS preflight behavior as the other P2P routes.

## Tool Contract

Expose these MCP tool names:

| Tool | Backend action | Route semantics |
| --- | --- | --- |
| `list_contacts` | `mcp.rooms.search` | query, default `type=contact` |
| `search_rooms` | `mcp.rooms.search` | query |
| `send_message` | `mcp.messages.send` | command |
| `list_messages` | `mcp.messages.list` | query |
| `list_room_members` | `mcp.room_members.list` | query |
| `list_channel_posts` | `mcp.channel_posts.list` | query |
| `list_post_comments` | `mcp.channel_comments.list` | query |
| `comment_channel_post` | `mcp.channel_comments.create` | command |

The Go adapter should keep tool descriptions aligned with the sibling MCP package. Inputs should stay simple:

- Search/list tools accept optional `query`, `type`, and `limit` where applicable.
- Message and channel content tools accept `room_id` or `post_id`, optional millisecond time ranges, optional `limit`, and plain text `msg` for writes.
- `send_message` requires an explicit non-agent `room_id`.
- `list_messages` may use `agent_room_id` as the default read room only if the service has one configured.

The backend remains responsible for room blacklist enforcement, product-room validation, Matrix timeline reads/writes, and concise MCP result shapes.

## Error Handling

Reject missing, malformed, or non-agent bearer tokens before constructing tool calls.

Do not expose access tokens, agent tokens, Matrix tokens, panic details, or verbose internal state in MCP errors. Tool-call failures should map existing `apiError` status and message into concise MCP tool errors.

The endpoint should reject unsupported methods with MCP-compatible JSON-RPC method errors where the SDK requires that shape, while preserving normal P2P CORS headers.

## Documentation

Update the current project docs and API change record to state:

- `POST /_p2p/mcp` is the standard online MCP endpoint.
- It accepts only `agent_token`.
- It exposes the same fixed MCP tools as the local adapter.
- Existing `mcp.*` body actions remain HTTP-only and are still not valid over `/_p2p/ws`.

Split Postman examples into two importable collections:

- `docs/postman/dirextalk-p2p.postman_collection.json` for `/_p2p/*` and `/.well-known/portal/*` product/API examples, including the new `POST /_p2p/mcp` MCP protocol endpoint.
- `docs/postman/dirextalk-matrix.postman_collection.json` for Matrix-native `/_matrix/*`, `/_synapse/*`, `/_dendrite/*`, and `/.well-known/matrix/*` route examples.

The old mixed `docs/postman/dirextalk-message-server.postman_collection.json` should not remain the current import target after the split. Update project docs, workflow instructions, and verification guidance to validate both new collection files.

## Tests

Add focused tests in `p2p`:

- `OPTIONS /_p2p/mcp` returns CORS preflight.
- Missing token returns unauthorized.
- Owner `access_token` is rejected.
- `agent_token` can initialize/list tools through the MCP endpoint.
- `agent_token` can call a representative tool such as `search_rooms`.
- The MCP endpoint cannot be used to call non-MCP product actions.

Keep existing routing tests for `/_p2p/query`, `/_p2p/command`, and `/_p2p/ws` intact.

## Non-Goals

- Do not embed the Node `dirextalk-mcp` runtime into the Go service.
- Do not add MCP over WebSocket.
- Do not allow `agent_token` to call owner product actions.
- Do not add new MCP tools beyond the existing fixed MCP action set.
- Do not add durable storage or migrations.
