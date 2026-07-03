# Online MCP Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standard online MCP Streamable HTTP endpoint at `POST /_p2p/mcp` protected by `agent_token`.

**Architecture:** Keep the MCP service inside the existing `p2p` package. A new adapter builds a `github.com/modelcontextprotocol/go-sdk/mcp` server, registers the same curated tools as the sibling `dirextalk-mcp` package, and forwards tool calls to existing `Service.Handle` `mcp.*` actions. Routing remains in `p2p.Register`, with token checks before SDK handling.

**Tech Stack:** Go 1.26, gorilla/mux, existing P2P service/action registry, official MCP Go SDK `github.com/modelcontextprotocol/go-sdk/mcp` v1.6.1, existing docs/Postman JSON.

---

## File Structure

- Create `p2p/mcp_online.go`: MCP server construction, tool specs, input defaults, JSON text tool result helpers, and authenticated HTTP handler.
- Create `p2p/mcp_online_test.go`: focused HTTP MCP route tests and adapter behavior checks.
- Modify `p2p/routing.go`: register `POST, OPTIONS /mcp`.
- Modify `go.mod` and `go.sum`: add `github.com/modelcontextprotocol/go-sdk v1.6.1`.
- Modify `p2p/service.go`: add a small `AgentRoomID()` accessor for the MCP adapter.
- Modify `docs/current-project-documentation.md`: document the online MCP endpoint and auth boundary.
- Modify `docs/api-interface-change-record.md`: record new `POST /_p2p/mcp` contract.
- Create `docs/postman/dirextalk-p2p.postman_collection.json`: P2P, portal well-known, realtime, and online MCP examples.
- Create `docs/postman/dirextalk-matrix.postman_collection.json`: Matrix-native route examples.
- Delete `docs/postman/dirextalk-message-server.postman_collection.json`: remove the old mixed import target after the two new collections are valid.
- Modify `AGENTS.md`, `docs/feature-inventory.md`, `docs/api-audit-and-optimization.md`, and `.codex/skills/dirextalk-targeted-verification/SKILL.md`: update current Postman paths and validation commands.

## Task 1: Add MCP Route Tests First

**Files:**
- Create: `p2p/mcp_online_test.go`
- Modify later: `p2p/routing.go`
- Modify later: `p2p/mcp_online.go`

- [ ] **Step 1: Write failing route and MCP behavior tests**

Create `p2p/mcp_online_test.go` with:

```go
package p2p

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOnlineMCPPreflight(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	req := httptest.NewRequest(http.MethodOptions, "/_p2p/mcp", nil)
	req.Header.Set("Origin", "https://app.example")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Fatalf("expected CORS origin echo, got %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestOnlineMCPRejectsMissingAndOwnerTokens(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	for name, token := range map[string]string{
		"missing": "",
		"owner":   service.AccessToken(),
	} {
		t.Run(name, func(t *testing.T) {
			req := mcpJSONRequest(t, map[string]any{
				"jsonrpc": "2.0",
				"id":      1,
				"method":  "initialize",
				"params":  map[string]any{},
			})
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestOnlineMCPAgentTokenCanInitializeAndListTools(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	initResp := mustOnlineMCPRequest(t, router, service.AgentToken(), map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params":  map[string]any{},
	})
	if initResp["error"] != nil {
		t.Fatalf("initialize returned error: %#v", initResp["error"])
	}

	listResp := mustOnlineMCPRequest(t, router, service.AgentToken(), map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
		"params":  map[string]any{},
	})
	result := listResp["result"].(map[string]any)
	tools := result["tools"].([]any)
	names := map[string]bool{}
	for _, rawTool := range tools {
		tool := rawTool.(map[string]any)
		names[tool["name"].(string)] = true
	}
	for _, name := range []string{
		"list_contacts",
		"search_rooms",
		"send_message",
		"list_messages",
		"list_room_members",
		"list_channel_posts",
		"list_post_comments",
		"comment_channel_post",
	} {
		if !names[name] {
			t.Fatalf("expected tool %q in tools/list response %#v", name, tools)
		}
	}
}

func TestOnlineMCPAgentTokenCanCallSearchRooms(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	resp := mustOnlineMCPRequest(t, router, service.AgentToken(), map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "search_rooms",
			"arguments": map[string]any{
				"query": "nobody",
				"type":  "all",
				"limit": 5,
			},
		},
	})
	if resp["error"] != nil {
		t.Fatalf("tools/call returned protocol error: %#v", resp["error"])
	}
	result := resp["result"].(map[string]any)
	if isError, _ := result["isError"].(bool); isError {
		t.Fatalf("search_rooms returned tool error: %#v", result)
	}
	content := result["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("expected one text content item, got %#v", content)
	}
	text := content[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, `"rooms"`) {
		t.Fatalf("expected JSON room search result, got %s", text)
	}
}

func TestOnlineMCPDoesNotExposeNonMCPActions(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	resp := mustOnlineMCPRequest(t, router, service.AgentToken(), map[string]any{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "portal.auth",
			"arguments": map[string]any{"password": service.password},
		},
	})
	if resp["error"] == nil {
		t.Fatalf("expected unknown MCP tool protocol error, got %#v", resp)
	}
}

func TestOnlineMCPListContactsAppliesContactDefault(t *testing.T) {
	client := &recordingMCPActionCaller{}
	server := newOnlineMCPServer(client, "")

	handler := newOnlineMCPStreamableHandler(server)
	req := mcpJSONRequest(t, map[string]any{
		"jsonrpc": "2.0",
		"id":      5,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "list_contacts",
			"arguments": map[string]any{"query": "alice"},
		},
	})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if client.action != "mcp.rooms.search" {
		t.Fatalf("expected mcp.rooms.search, got %q", client.action)
	}
	if client.params["type"] != "contact" {
		t.Fatalf("expected type=contact default, got %#v", client.params)
	}
}

type recordingMCPActionCaller struct {
	action string
	params map[string]any
}

func (c *recordingMCPActionCaller) Handle(ctx context.Context, action string, params map[string]any) (any, *apiError) {
	c.action = action
	c.params = params
	return map[string]any{"ok": true}, nil
}

func mustOnlineMCPRequest(t *testing.T, router http.Handler, token string, body map[string]any) map[string]any {
	t.Helper()
	req := mcpJSONRequest(t, body)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func mcpJSONRequest(t *testing.T, body map[string]any) *http.Request {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/_p2p/mcp", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	return req
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./p2p -run 'TestOnlineMCP' -count=1
```

Expected: FAIL with undefined symbols such as `newOnlineMCPServer` and route `/_p2p/mcp` returning 404 before implementation.

- [ ] **Step 3: Commit failing tests only if the team wants red commits**

Default for this repo: do not commit failing tests separately. Keep them staged only after implementation passes.

## Task 2: Add MCP SDK Dependency

**Files:**
- Modify: `go.mod`
- Modify: `go.sum`

- [ ] **Step 1: Add the official MCP Go SDK module**

Run:

```bash
go get github.com/modelcontextprotocol/go-sdk@v1.6.1
```

Expected: `go.mod` gains `github.com/modelcontextprotocol/go-sdk v1.6.1`; `go.sum` gains its checksums and any minimal transitive sums.

- [ ] **Step 2: Verify module graph still resolves**

Run:

```bash
go list ./p2p
```

Expected: prints `github.com/YingSuiAI/dirextalk-message-server/p2p`.

## Task 3: Implement the Online MCP Adapter

**Files:**
- Create: `p2p/mcp_online.go`
- Modify: `p2p/routing.go`
- Modify: `p2p/service.go`
- Test: `p2p/mcp_online_test.go`

- [ ] **Step 1: Add the Agent room accessor**

Modify `p2p/service.go` near the existing `AccessToken()` and `AgentToken()` accessors:

```go
func (s *Service) AgentRoomID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.agentRoomID
}
```

- [ ] **Step 2: Create the adapter implementation**

Create `p2p/mcp_online.go`:

```go
package p2p

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const onlineMCPVersion = "0.1.0"

type mcpActionCaller interface {
	Handle(context.Context, string, map[string]any) (any, *apiError)
}

type onlineMCPInput struct {
	Query  string `json:"query,omitempty"`
	Type   string `json:"type,omitempty"`
	Limit  int64  `json:"limit,omitempty"`
	RoomID string `json:"room_id,omitempty"`
	PostID string `json:"post_id,omitempty"`
	FromTS int64  `json:"from_ts,omitempty"`
	ToTS   int64  `json:"to_ts,omitempty"`
	Msg    string `json:"msg,omitempty"`
}

func onlineMCPHandler(service *Service) http.Handler {
	server := newOnlineMCPServer(service, service.AgentRoomID())
	streamable := newOnlineMCPStreamableHandler(server)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"jsonrpc": "2.0",
				"error": map[string]any{
					"code":    -32000,
					"message": "Method not allowed.",
				},
				"id": nil,
			})
			return
		}
		token := bearerToken(r.Header.Get("Authorization"))
		if token == "" || token != service.AgentToken() {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "M_UNKNOWN_TOKEN"})
			return
		}
		streamable.ServeHTTP(w, r)
	})
}

func newOnlineMCPStreamableHandler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, &mcp.StreamableHTTPOptions{
		JSONResponse:    true,
		SessionTimeout:  2 * time.Minute,
		Stateless:       true,
	})
}

func newOnlineMCPServer(caller mcpActionCaller, defaultAgentRoomID string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "dirextalk-message-server-mcp",
		Version: onlineMCPVersion,
	}, nil)

	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:        "list_contacts",
		Description: "List or search Dirextalk contacts/friends. Returns room_id values for messaging tools.",
		Action:      "mcp.rooms.search",
		DefaultType: "contact",
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:        "search_rooms",
		Description: "Search or list Dirextalk rooms. type=contact lists contacts, type=group lists groups, type=channel lists channels, type=all searches all room types.",
		Action:      "mcp.rooms.search",
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:           "send_message",
		Description:    "Send a plain text message as the portal owner to a non-agent Dirextalk private chat, group, or channel room.",
		Action:         "mcp.messages.send",
		RequireRoomID:  true,
		RejectAgentRoom: true,
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:               "list_messages",
		Description:        "List ordinary room messages by room_id and optional Unix millisecond time range.",
		Action:             "mcp.messages.list",
		DefaultAgentRoomID: true,
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:          "list_room_members",
		Description:   "List Dirextalk room members by room_id.",
		Action:        "mcp.room_members.list",
		RequireRoomID: true,
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:          "list_channel_posts",
		Description:   "List channel posts by channel room_id and optional Unix millisecond time range.",
		Action:        "mcp.channel_posts.list",
		RequireRoomID: true,
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:          "list_post_comments",
		Description:   "List comments for a channel post by post_id and optional Unix millisecond time range.",
		Action:        "mcp.channel_comments.list",
		RequirePostID: true,
	})
	addOnlineMCPTool(server, caller, defaultAgentRoomID, onlineMCPTool{
		Name:          "comment_channel_post",
		Description:   "Publish a plain text comment to an existing Dirextalk channel post.",
		Action:        "mcp.channel_comments.create",
		RequirePostID: true,
		RequireMsg:    true,
	})

	return server
}

type onlineMCPTool struct {
	Name               string
	Description        string
	Action             string
	DefaultType         string
	RequireRoomID      bool
	RejectAgentRoom    bool
	DefaultAgentRoomID bool
	RequirePostID      bool
	RequireMsg         bool
}

func addOnlineMCPTool(server *mcp.Server, caller mcpActionCaller, defaultAgentRoomID string, spec onlineMCPTool) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        spec.Name,
		Description: spec.Description,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, input onlineMCPInput) (*mcp.CallToolResult, map[string]any, error) {
		params, err := onlineMCPParams(spec, input, defaultAgentRoomID)
		if err != nil {
			return nil, nil, err
		}
		result, apiErr := caller.Handle(ctx, spec.Action, params)
		if apiErr != nil {
			return nil, nil, errors.New(apiErr.Error)
		}
		output, ok := result.(map[string]any)
		if !ok {
			raw, err := json.Marshal(result)
			if err != nil {
				return nil, nil, err
			}
			output = map[string]any{"result": json.RawMessage(raw)}
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: mustOnlineMCPJSONString(output)}},
		}, output, nil
	})
}

func onlineMCPParams(spec onlineMCPTool, input onlineMCPInput, defaultAgentRoomID string) (map[string]any, error) {
	params := make(map[string]any)
	if value := strings.TrimSpace(input.Query); value != "" {
		params["query"] = value
	}
	if value := strings.TrimSpace(input.Type); value != "" {
		params["type"] = value
	} else if spec.DefaultType != "" {
		params["type"] = spec.DefaultType
	}
	if input.Limit > 0 {
		params["limit"] = input.Limit
	}
	if input.FromTS > 0 {
		params["from_ts"] = input.FromTS
	}
	if input.ToTS > 0 {
		params["to_ts"] = input.ToTS
	}
	roomID := strings.TrimSpace(input.RoomID)
	if roomID == "" && spec.DefaultAgentRoomID {
		roomID = strings.TrimSpace(defaultAgentRoomID)
	}
	if spec.RequireRoomID && roomID == "" {
		return nil, errors.New("room_id is required")
	}
	if roomID != "" {
		if spec.RejectAgentRoom && roomID == strings.TrimSpace(defaultAgentRoomID) {
			return nil, errors.New("send_message cannot target the agent room")
		}
		params["room_id"] = roomID
	}
	postID := strings.TrimSpace(input.PostID)
	if spec.RequirePostID && postID == "" {
		return nil, errors.New("post_id is required")
	}
	if postID != "" {
		params["post_id"] = postID
	}
	msg := strings.TrimSpace(input.Msg)
	if spec.RequireMsg && msg == "" {
		return nil, errors.New("msg is required")
	}
	if msg != "" {
		params["msg"] = msg
	}
	return params, nil
}

func mustOnlineMCPJSONString(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return `{"error":"json marshal failed"}`
	}
	return string(raw)
}
```

- [ ] **Step 3: Add the route**

Modify `p2p/routing.go` inside `Register`:

```go
func Register(router *mux.Router, service *Service) {
	router.HandleFunc("/query", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/command", handle(service)).Methods(http.MethodPost, http.MethodOptions)
	router.Handle("/mcp", onlineMCPHandler(service)).Methods(http.MethodPost, http.MethodOptions)
	router.HandleFunc("/ws", realtimeWSHandler(service)).Methods(http.MethodGet, http.MethodOptions)
	router.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}).Methods(http.MethodGet, http.MethodOptions)
}
```

- [ ] **Step 4: Run the focused tests**

Run:

```bash
gofmt -w p2p/mcp_online.go p2p/mcp_online_test.go p2p/routing.go p2p/service.go
go test ./p2p -run 'TestOnlineMCP' -count=1
```

Expected: PASS.

- [ ] **Step 5: Run existing P2P routing tests**

Run:

```bash
go test ./p2p -run 'Test.*(HTTP|Agent|Realtime|OnlineMCP)' -count=1
```

Expected: PASS. Existing WS tests must still reject MCP actions over `/_p2p/ws`.

- [ ] **Step 6: Commit adapter and tests**

Run:

```bash
git add go.mod go.sum p2p/mcp_online.go p2p/mcp_online_test.go p2p/routing.go p2p/service.go
git commit -m "feat: add online MCP endpoint"
```

Expected: commit includes only dependency, adapter, route, and tests.

## Task 4: Synchronize Contracts and Docs

**Files:**
- Modify: `docs/current-project-documentation.md`
- Modify: `docs/api-interface-change-record.md`
- Create: `docs/postman/dirextalk-p2p.postman_collection.json`
- Create: `docs/postman/dirextalk-matrix.postman_collection.json`
- Delete: `docs/postman/dirextalk-message-server.postman_collection.json`
- Modify: `AGENTS.md`
- Modify: `docs/feature-inventory.md`
- Modify: `docs/api-audit-and-optimization.md`
- Modify: `.codex/skills/dirextalk-targeted-verification/SKILL.md`

- [ ] **Step 1: Update current project documentation**

In `docs/current-project-documentation.md`, update the Agent/MCP section around the existing MCP paragraph to include:

```markdown
- `POST /_p2p/mcp` is the online MCP Streamable HTTP endpoint. It accepts only `Authorization: Bearer <agent_token>` and exposes the fixed MCP tool names `list_contacts`, `search_rooms`, `send_message`, `list_messages`, `list_room_members`, `list_channel_posts`, `list_post_comments`, and `comment_channel_post`. The endpoint is a protocol wrapper over existing `mcp.*` body actions; it does not accept owner product actions, does not create realtime WS tickets, and does not move MCP actions into `/_p2p/ws`.
```

- [ ] **Step 2: Add API change record entry**

Add a dated entry near the top of `docs/api-interface-change-record.md`:

```markdown
## 2026-07-04 Online MCP Streamable HTTP Endpoint

Added `POST /_p2p/mcp` as the standard online MCP Streamable HTTP endpoint. The endpoint accepts only `Authorization: Bearer <agent_token>` and exposes the fixed MCP tool set: `list_contacts`, `search_rooms`, `send_message`, `list_messages`, `list_room_members`, `list_channel_posts`, `list_post_comments`, and `comment_channel_post`.

This endpoint is a protocol wrapper over existing `mcp.*` HTTP body actions. Existing `POST /_p2p/query` and `POST /_p2p/command` MCP actions remain available, MCP actions remain invalid over `/_p2p/ws`, and `agent_token` still cannot call owner product actions.
```

- [ ] **Step 3: Split the Postman collection**

Use a JSON-aware script to split the old mixed collection into two importable files and add the online MCP request to the P2P collection:

```bash
python3 - <<'PY'
import copy
import json
from pathlib import Path

src = Path("docs/postman/dirextalk-message-server.postman_collection.json")
p2p_dst = Path("docs/postman/dirextalk-p2p.postman_collection.json")
matrix_dst = Path("docs/postman/dirextalk-matrix.postman_collection.json")

collection = json.loads(src.read_text())
p2p_items = []
matrix_items = []

for item in collection["item"]:
    text = json.dumps(item, ensure_ascii=False)
    if "/_p2p/" in text or "/.well-known/portal/" in text:
        p2p_items.append(item)
    else:
        matrix_items.append(item)

online_mcp = {
    "name": "online MCP Streamable HTTP（在线MCP协议入口）",
    "request": {
        "method": "POST",
        "header": [
            {"key": "Authorization", "value": "Bearer {{agentToken}}"},
            {"key": "Content-Type", "value": "application/json"},
            {"key": "Accept", "value": "application/json, text/event-stream"}
        ],
        "body": {
            "mode": "raw",
            "raw": "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 1,\n  \"method\": \"initialize\",\n  \"params\": {}\n}",
            "options": {"raw": {"language": "json"}}
        },
        "url": "{{baseUrl}}/_p2p/mcp",
        "description": "Standard MCP Streamable HTTP endpoint. Auth uses agent_token only. This endpoint wraps the fixed MCP tool set and does not accept owner product actions or realtime WS requests."
    }
}

p2p = copy.deepcopy(collection)
p2p["info"]["name"] = "Dirextalk P2P Product API"
p2p["info"]["_postman_id"] = "dirextalk-p2p-generated-2026-07-04"
p2p["info"]["description"] = "Dirextalk product, portal well-known, realtime, and online MCP examples. Matrix-native API examples are in dirextalk-matrix.postman_collection.json."
p2p["item"] = [online_mcp] + p2p_items

matrix = copy.deepcopy(collection)
matrix["info"]["name"] = "Dirextalk Matrix Native API"
matrix["info"]["_postman_id"] = "dirextalk-matrix-generated-2026-07-04"
matrix["info"]["description"] = "Matrix-native Client-Server, Federation, Media, Synapse, Dendrite, and Matrix well-known examples. Dirextalk product P2P examples are in dirextalk-p2p.postman_collection.json."
matrix["item"] = matrix_items

p2p_dst.write_text(json.dumps(p2p, ensure_ascii=False, indent=2) + "\n")
matrix_dst.write_text(json.dumps(matrix, ensure_ascii=False, indent=2) + "\n")
src.unlink()
PY
```

After the script, verify that `dirextalk-p2p.postman_collection.json` contains `/_p2p/mcp`, `/_p2p/query`, `/_p2p/command`, `/_p2p/ws`, and `/.well-known/portal/owner.json`, and verify that `dirextalk-matrix.postman_collection.json` contains Matrix-native `/_matrix/*` examples.

- [ ] **Step 4: Update current references to the split Postman files**

Replace current references to `docs/postman/dirextalk-message-server.postman_collection.json` in current workflow docs with both new paths. Required current files:

```text
AGENTS.md
.codex/skills/dirextalk-targeted-verification/SKILL.md
docs/current-project-documentation.md
docs/feature-inventory.md
docs/api-audit-and-optimization.md
```

Use this wording where a concise validation command is needed:

```bash
python3 -m json.tool docs/postman/dirextalk-p2p.postman_collection.json >/dev/null
python3 -m json.tool docs/postman/dirextalk-matrix.postman_collection.json >/dev/null
```

Use this wording where manual import guidance is needed:

```markdown
Use `docs/postman/dirextalk-p2p.postman_collection.json` for Dirextalk product, portal, realtime, and MCP checks. Use `docs/postman/dirextalk-matrix.postman_collection.json` for Matrix-native route checks.
```

- [ ] **Step 5: Validate both Postman JSON files**

Run:

```bash
python3 -m json.tool docs/postman/dirextalk-p2p.postman_collection.json >/dev/null
python3 -m json.tool docs/postman/dirextalk-matrix.postman_collection.json >/dev/null
```

Expected: both commands exit 0 with no output.

- [ ] **Step 6: Commit docs**

Run:

```bash
git add AGENTS.md .codex/skills/dirextalk-targeted-verification/SKILL.md docs/current-project-documentation.md docs/api-interface-change-record.md docs/feature-inventory.md docs/api-audit-and-optimization.md docs/postman/dirextalk-p2p.postman_collection.json docs/postman/dirextalk-matrix.postman_collection.json
git rm docs/postman/dirextalk-message-server.postman_collection.json
git commit -m "docs: document online MCP endpoint"
```

Expected: commit includes only docs, project-local verification guidance, and Postman split changes.

## Task 5: Final Verification

**Files:**
- Verify all touched files from Tasks 1-4.

- [ ] **Step 1: Run targeted Go checks**

Run:

```bash
gofmt -w p2p/mcp_online.go p2p/mcp_online_test.go p2p/routing.go p2p/service.go
go test ./p2p -run 'TestOnlineMCP|TestAgentTokenCanOnlyCallAgentBootstrapAndMCPActions|TestRealtimeWSRejectsMCPRequests' -count=1
go test ./p2p ./internal/productpolicy -count=1
```

Expected: all tests pass.

- [ ] **Step 2: Run build and contract checks**

Run:

```bash
go build ./cmd/dirextalk-message-server
python3 -m json.tool docs/postman/dirextalk-p2p.postman_collection.json >/dev/null
python3 -m json.tool docs/postman/dirextalk-matrix.postman_collection.json >/dev/null
git diff --check
```

Expected: build succeeds, Postman JSON is valid, and diff check prints no whitespace errors.

- [ ] **Step 3: Optional semantic diagnostics**

If `gopls` is installed, run:

```bash
gopls check p2p/mcp_online.go p2p/mcp_online_test.go p2p/routing.go
```

Expected: no diagnostics.

- [ ] **Step 4: Inspect final diff**

Run:

```bash
git status --short
git diff --stat HEAD
```

Expected: only intentional files are changed. The pre-existing `docker-compose.p2p-dual.yml` modification may still appear and must not be reverted or included unless explicitly requested.
