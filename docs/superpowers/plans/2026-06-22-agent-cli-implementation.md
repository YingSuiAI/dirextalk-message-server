# Direxio Agent CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a cross-platform Go CLI that lets Developer Operators use Direxio P2P and Matrix workflows with `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN`, plus the server action needed to bootstrap an internal Matrix session.

**Architecture:** Add a protected P2P action, `agent.matrix_session.create`, that mints the Matrix session for the portal owner through existing Matrix session machinery. Build a shared `internal/agentclient` package for domain normalization, P2P action calls, Matrix API calls, output rendering, and sync helpers. Build `cmd/direxio-cli` as the primary user interface with domain commands and a fallback `p2p action`; defer `cmd/direxio-mcp` to the final optional task after CLI behavior is stable.

**Tech Stack:** Go standard library (`flag`, `net/http`, `encoding/json`, `httptest`, `os/exec`), existing P2P service code, Matrix Client-Server HTTP APIs, PowerShell and POSIX shell build scripts.

---

## Scope And File Map

The design spec covers several surfaces. Implement them in this order so each task leaves working software:

1. Server contract: `agent.matrix_session.create`
2. Shared client package
3. CLI command framework and output contract
4. P2P domain commands
5. Matrix session and message commands
6. Help, agent recipes, and build scripts
7. Optional thin MCP adapter skeleton

Files to create:

- `internal/agentclient/config.go` — reads `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN`, normalizes the domain, derives P2P and Matrix routes.
- `internal/agentclient/config_test.go` — unit tests for config and URL normalization.
- `internal/agentclient/client.go` — shared HTTP client, P2P action call, Agent Matrix Session creation, Matrix request helper.
- `internal/agentclient/client_test.go` — `httptest` coverage for headers, paths, request bodies, and error handling.
- `internal/agentclient/output.go` — pretty JSON, raw JSON, NDJSON event output, and stderr error rendering.
- `internal/agentclient/output_test.go` — output contract tests.
- `internal/agentclient/matrix.go` — Matrix message send, room messages, sync, and listen helper functions.
- `internal/agentclient/matrix_test.go` — Matrix endpoint request/response tests.
- `cmd/direxio-cli/main.go` — executable entry point.
- `cmd/direxio-cli/cli.go` — command router, help text, flags, exit codes.
- `cmd/direxio-cli/cli_test.go` — command parsing and help/output tests.
- `scripts/build-agent-tools.ps1` — Windows-friendly cross-platform binary build.
- `scripts/build-agent-tools.sh` — POSIX cross-platform binary build.
- `docs/agent-skills/codex-direxio-cli.md` — Codex recipe.
- `docs/agent-skills/claude-code-direxio-cli.md` — Claude Code recipe.
- `docs/agent-skills/openclaw-direxio-cli.md` — OpenClaw recipe.
- `docs/agent-skills/hermes-direxio-cli.md` — Hermes-style plugin recipe.
- `.codex/skills/direxio-cli/SKILL.md` — installable project-local Codex skill.

Files to modify:

- `p2p/service.go` — add `agent.matrix_session.create` dispatch, handler, and default Agent permission entry.
- `p2p/password_test.go` — add service-level tests for Matrix session creation through Agent action.
- `p2p/routing_test.go` — add route-level test showing Agent token can call the action.
- `docs/api-interface-change-record.md` — document the new protected action and sensitive response.
- `docs/postman/direxio-message-server.postman_collection.json` — add importable example for `agent.matrix_session.create`.

Do not modify or stage existing unrelated changes such as `Dockerfile` or `docs/client-ai-interface-migration-2026-06-22.md`.

---

### Task 1: Add the Agent Matrix Session P2P Contract

**Files:**
- Modify: `p2p/service.go`
- Modify: `p2p/password_test.go`
- Modify: `p2p/routing_test.go`
- Modify: `docs/api-interface-change-record.md`
- Modify: `docs/postman/direxio-message-server.postman_collection.json`

- [ ] **Step 1: Write the failing service test**

Add this test to `p2p/password_test.go` near the existing Matrix session tests:

```go
func TestAgentMatrixSessionCreateUsesAgentDeviceAndOwnerProfile(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	issuer := &recordingMatrixSessionIssuer{}
	service.SetMatrixSessionIssuer(issuer)

	session := mustHandle[map[string]any](t, service, "agent.matrix_session.create", map[string]any{
		"device_id": "DIREXIO_CLI",
	})

	if issuer.deviceID != "DIREXIO_CLI" {
		t.Fatalf("expected Matrix issuer to receive CLI device id, got %q", issuer.deviceID)
	}
	if session["device_id"] != "DIREXIO_CLI" {
		t.Fatalf("expected session device id to be DIREXIO_CLI, got %#v", session)
	}
	if session["access_token"] != "matrix-token-for-DIREXIO_CLI" {
		t.Fatalf("expected Matrix access token in internal session response, got %#v", session)
	}
	if session["user_id"] != "@owner:example.com" {
		t.Fatalf("expected portal owner user id, got %#v", session)
	}
	if _, ok := session["password"]; ok {
		t.Fatalf("agent Matrix session must not expose portal password: %#v", session)
	}
	if _, ok := session["agent_token"]; ok {
		t.Fatalf("agent Matrix session must not echo agent token: %#v", session)
	}
}
```

- [ ] **Step 2: Run the failing service test**

Run:

```powershell
go test ./p2p -run TestAgentMatrixSessionCreateUsesAgentDeviceAndOwnerProfile -count=1
```

Expected: FAIL with an unknown action error for `agent.matrix_session.create`.

- [ ] **Step 3: Implement the minimal service handler**

In `p2p/service.go`, add the dispatch case near the other `agent.*` cases:

```go
	case "agent.matrix_session.create":
		return s.agentMatrixSession(ctx, params)
```

Add this handler near `agentPassword`:

```go
func (s *Service) agentMatrixSession(ctx context.Context, params map[string]any) (any, *apiError) {
	session, apiErr := s.refreshMatrixSession(ctx, map[string]any{}, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{
		"access_token": session["access_token"],
		"device_id":    session["device_id"],
		"user_id":      session["user_id"],
		"homeserver":   session["homeserver"],
	}, nil
}
```

In `defaultAPIPermissions()`, add:

```go
		{Method: "POST", Path: "/agent/matrix-session", Action: "agent.matrix_session.create", Description: "Create an internal Matrix session for Agent tooling", Enabled: true},
```

- [ ] **Step 4: Run the service test**

Run:

```powershell
go test ./p2p -run TestAgentMatrixSessionCreateUsesAgentDeviceAndOwnerProfile -count=1
```

Expected: PASS.

- [ ] **Step 5: Write the route-level Agent token test**

Add this test to `p2p/routing_test.go` near the body-action auth tests:

```go
func TestAgentMatrixSessionCreateAllowsAgentToken(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	issuer := &recordingMatrixSessionIssuer{}
	service.SetMatrixSessionIssuer(issuer)
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "agent.matrix_session.create",
		"params": map[string]any{"device_id": "DIREXIO_CLI"},
	})
	req.Header.Set("Authorization", "Bearer "+service.AgentToken())
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["access_token"] != "matrix-token-for-DIREXIO_CLI" {
		t.Fatalf("expected Matrix access token for CLI device, got %#v", got)
	}
	if _, ok := got["password"]; ok {
		t.Fatalf("agent Matrix session response must not include password: %#v", got)
	}
	if _, ok := got["agent_token"]; ok {
		t.Fatalf("agent Matrix session response must not include agent token: %#v", got)
	}
}
```

- [ ] **Step 6: Run route tests**

Run:

```powershell
go test ./p2p -run "TestAgentMatrixSessionCreateAllowsAgentToken|TestAgentMatrixSessionCreateUsesAgentDeviceAndOwnerProfile" -count=1
```

Expected: PASS.

- [ ] **Step 7: Update API docs**

Append this section to `docs/api-interface-change-record.md`:

```md
## Agent Matrix session for CLI tooling

Added protected action `agent.matrix_session.create` on `POST /_p2p/command`. It requires a bearer access token or an enabled Agent token and returns the Matrix Client-Server session needed by trusted CLI tooling: `access_token`, `device_id`, `user_id`, and `homeserver`. The response is for internal CLI use and must not be displayed by normal CLI workflows.
```

- [ ] **Step 8: Update Postman collection**

Add an importable item under the Agent/API section in `docs/postman/direxio-message-server.postman_collection.json`:

```json
{
  "name": "agent.matrix_session.create",
  "request": {
    "method": "POST",
    "header": [
      {
        "key": "Content-Type",
        "value": "application/json"
      },
      {
        "key": "Authorization",
        "value": "Bearer {{agentToken}}"
      }
    ],
    "body": {
      "mode": "raw",
      "raw": "{\n  \"action\": \"agent.matrix_session.create\",\n  \"params\": {\n    \"device_id\": \"DIREXIO_CLI\"\n  }\n}"
    },
    "url": {
      "raw": "{{baseUrl}}/_p2p/command",
      "host": ["{{baseUrl}}"],
      "path": ["_p2p", "command"]
    },
    "description": "Create internal Matrix session for Direxio CLI tooling. Auth: Protected action; use access_token or enabled agent_token. The returned Matrix access token is sensitive and should be consumed by CLI internals."
  }
}
```

Use existing collection structure rather than replacing the file wholesale.

- [ ] **Step 9: Validate docs and tests**

Run:

```powershell
Get-Content docs/postman/direxio-message-server.postman_collection.json | ConvertFrom-Json | Out-Null
go test ./p2p -run "TestAgentMatrixSessionCreate|TestAgentPermissionCatalogCoversMigratedBusinessActions" -count=1
git diff --check -- p2p/service.go p2p/password_test.go p2p/routing_test.go docs/api-interface-change-record.md docs/postman/direxio-message-server.postman_collection.json
```

Expected: JSON parse succeeds, tests PASS, diff check emits no output.

- [ ] **Step 10: Commit**

```powershell
git add p2p/service.go p2p/password_test.go p2p/routing_test.go docs/api-interface-change-record.md docs/postman/direxio-message-server.postman_collection.json
git commit -m "feat: add agent Matrix session action"
```

---

### Task 2: Create Agent Client Config And P2P HTTP Client

**Files:**
- Create: `internal/agentclient/config.go`
- Create: `internal/agentclient/config_test.go`
- Create: `internal/agentclient/client.go`
- Create: `internal/agentclient/client_test.go`

- [ ] **Step 1: Write config tests**

Create `internal/agentclient/config_test.go`:

```go
package agentclient

import "testing"

func TestConfigFromEnvNormalizesDomain(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com/")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Domain != "https://example.com" {
		t.Fatalf("expected normalized domain, got %q", cfg.Domain)
	}
	if cfg.P2PBaseURL() != "https://example.com/_p2p" {
		t.Fatalf("unexpected p2p base: %q", cfg.P2PBaseURL())
	}
	if cfg.MatrixBaseURL() != "https://example.com/_matrix/client" {
		t.Fatalf("unexpected matrix base: %q", cfg.MatrixBaseURL())
	}
}

func TestConfigRejectsRoutePrefixedDomain(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com/_p2p")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("expected route-prefixed domain to fail")
	}
}

func TestConfigRejectsMissingAgentToken(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")

	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("expected missing agent token to fail")
	}
}
```

- [ ] **Step 2: Run config tests and verify failure**

Run:

```powershell
go test ./internal/agentclient -run TestConfig -count=1
```

Expected: FAIL because the package does not exist.

- [ ] **Step 3: Implement config**

Create `internal/agentclient/config.go`:

```go
package agentclient

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Domain     string
	AgentToken string
}

func ConfigFromEnv() (Config, error) {
	return NewConfig(os.Getenv("DIREXIO_DOMAIN"), os.Getenv("DIREXIO_AGENT_TOKEN"))
}

func NewConfig(domain, agentToken string) (Config, error) {
	domain = strings.TrimRight(strings.TrimSpace(domain), "/")
	agentToken = strings.TrimSpace(agentToken)
	if domain == "" {
		return Config{}, errors.New("DIREXIO_DOMAIN is required")
	}
	if agentToken == "" {
		return Config{}, errors.New("DIREXIO_AGENT_TOKEN is required")
	}
	u, err := url.Parse(domain)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return Config{}, errors.New("DIREXIO_DOMAIN must be an absolute site origin such as https://example.com")
	}
	if strings.Contains(strings.Trim(u.Path, "/"), "/") || strings.HasPrefix(u.Path, "/_p2p") || strings.HasPrefix(u.Path, "/_matrix") {
		return Config{}, errors.New("DIREXIO_DOMAIN must not include /_p2p or /_matrix")
	}
	if strings.Trim(u.Path, "/") != "" {
		return Config{}, errors.New("DIREXIO_DOMAIN must be the site origin without a route prefix")
	}
	u.Path = ""
	u.RawPath = ""
	u.RawQuery = ""
	u.Fragment = ""
	return Config{Domain: strings.TrimRight(u.String(), "/"), AgentToken: agentToken}, nil
}

func (c Config) P2PBaseURL() string {
	return c.Domain + "/_p2p"
}

func (c Config) MatrixBaseURL() string {
	return c.Domain + "/_matrix/client"
}
```

- [ ] **Step 4: Run config tests**

Run:

```powershell
go test ./internal/agentclient -run TestConfig -count=1
```

Expected: PASS.

- [ ] **Step 5: Write P2P client tests**

Create `internal/agentclient/client_test.go`:

```go
package agentclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCallP2PActionPostsEnvelopeWithAgentToken(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	resp, err := client.CallP2PAction(context.Background(), "channels.list", map[string]any{"limit": 5}, P2PQuery)
	if err != nil {
		t.Fatal(err)
	}

	if gotPath != "/_p2p/query" {
		t.Fatalf("expected query path, got %q", gotPath)
	}
	if gotAuth != "Bearer agent-token" {
		t.Fatalf("expected bearer agent token, got %q", gotAuth)
	}
	if gotBody["action"] != "channels.list" {
		t.Fatalf("expected action body, got %#v", gotBody)
	}
	if resp["ok"] != true {
		t.Fatalf("expected ok response, got %#v", resp)
	}
}

func TestCallP2PActionReportsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": "M_UNKNOWN_TOKEN"})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "bad-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	_, err = client.CallP2PAction(context.Background(), "channels.list", nil, P2PQuery)
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "p2p channels.list failed with 401: M_UNKNOWN_TOKEN" {
		t.Fatalf("unexpected error: %v", err)
	}
}
```

- [ ] **Step 6: Implement P2P client**

Create `internal/agentclient/client.go`:

```go
package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type P2PRoute string

const (
	P2PQuery   P2PRoute = "query"
	P2PCommand P2PRoute = "command"
)

type Client struct {
	cfg  Config
	http *http.Client
}

func New(cfg Config, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{cfg: cfg, http: httpClient}
}

func (c *Client) CallP2PAction(ctx context.Context, action string, params map[string]any, route P2PRoute) (map[string]any, error) {
	action = strings.TrimSpace(action)
	if action == "" {
		return nil, fmt.Errorf("action is required")
	}
	if params == nil {
		params = map[string]any{}
	}
	path := string(route)
	if path == "" {
		path = string(P2PCommand)
	}
	body, err := json.Marshal(map[string]any{"action": action, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.P2PBaseURL()+"/"+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.cfg.AgentToken)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return decodeObjectResponse(res, "p2p "+action)
}

func decodeObjectResponse(res *http.Response, label string) (map[string]any, error) {
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if len(bytes.TrimSpace(data)) != 0 {
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("%s returned invalid json with status %d", label, res.StatusCode)
		}
	} else {
		payload = map[string]any{}
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		msg, _ := payload["error"].(string)
		if msg == "" {
			msg = strings.TrimSpace(string(data))
		}
		return nil, fmt.Errorf("%s failed with %d: %s", label, res.StatusCode, msg)
	}
	return payload, nil
}
```

- [ ] **Step 7: Run client tests**

Run:

```powershell
go test ./internal/agentclient -run "TestConfig|TestCallP2PAction" -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```powershell
git add internal/agentclient/config.go internal/agentclient/config_test.go internal/agentclient/client.go internal/agentclient/client_test.go
git commit -m "feat: add Direxio agent HTTP client"
```

---

### Task 3: Add Matrix Client Helpers

**Files:**
- Create: `internal/agentclient/matrix.go`
- Create: `internal/agentclient/matrix_test.go`
- Modify: `internal/agentclient/client.go`

- [ ] **Step 1: Write Matrix helper tests**

Create `internal/agentclient/matrix_test.go`:

```go
package agentclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCreateMatrixSessionUsesP2PAction(t *testing.T) {
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_p2p/command" {
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "matrix-token",
			"device_id":    "DIREXIO_CLI",
			"user_id":      "@owner:example.com",
			"homeserver":   serverURLHost(r),
		})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	session, err := client.CreateMatrixSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if gotBody["action"] != "agent.matrix_session.create" {
		t.Fatalf("expected agent.matrix_session.create, got %#v", gotBody)
	}
	if session.AccessToken != "matrix-token" || session.DeviceID != "DIREXIO_CLI" {
		t.Fatalf("unexpected session: %#v", session)
	}
}

func TestSendTextMessageUsesMatrixEndpoint(t *testing.T) {
	var gotAuth string
	var gotPath string
	var gotBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatal(err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"event_id": "$event"})
	}))
	defer server.Close()

	cfg, err := NewConfig(server.URL, "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	client := New(cfg, server.Client())
	resp, err := client.SendTextMessage(context.Background(), MatrixSession{AccessToken: "matrix-token"}, "!room:example.com", "hello")
	if err != nil {
		t.Fatal(err)
	}

	if gotAuth != "Bearer matrix-token" {
		t.Fatalf("expected matrix bearer token, got %q", gotAuth)
	}
	if gotPath != "/_matrix/client/v3/rooms/!room:example.com/send/m.room.message" {
		t.Fatalf("unexpected matrix send path %q", gotPath)
	}
	if gotBody["msgtype"] != "m.text" || gotBody["body"] != "hello" {
		t.Fatalf("unexpected message body %#v", gotBody)
	}
	if resp["event_id"] != "$event" {
		t.Fatalf("unexpected response %#v", resp)
	}
}

func serverURLHost(r *http.Request) string {
	return "http://" + r.Host
}
```

- [ ] **Step 2: Run Matrix helper tests and verify failure**

Run:

```powershell
go test ./internal/agentclient -run "TestCreateMatrixSession|TestSendTextMessage" -count=1
```

Expected: FAIL because Matrix helper types and methods are not defined.

- [ ] **Step 3: Implement Matrix helpers**

Create `internal/agentclient/matrix.go`:

```go
package agentclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const defaultAgentDeviceID = "DIREXIO_CLI"

type MatrixSession struct {
	AccessToken string `json:"access_token"`
	DeviceID    string `json:"device_id"`
	UserID      string `json:"user_id"`
	Homeserver  string `json:"homeserver"`
}

func (c *Client) CreateMatrixSession(ctx context.Context) (MatrixSession, error) {
	resp, err := c.CallP2PAction(ctx, "agent.matrix_session.create", map[string]any{"device_id": defaultAgentDeviceID}, P2PCommand)
	if err != nil {
		return MatrixSession{}, err
	}
	session := MatrixSession{
		AccessToken: stringValue(resp["access_token"]),
		DeviceID:    stringValue(resp["device_id"]),
		UserID:      stringValue(resp["user_id"]),
		Homeserver:  stringValue(resp["homeserver"]),
	}
	if session.AccessToken == "" {
		return MatrixSession{}, fmt.Errorf("agent.matrix_session.create did not return access_token")
	}
	return session, nil
}

func (c *Client) SendTextMessage(ctx context.Context, session MatrixSession, roomID, text string) (map[string]any, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, fmt.Errorf("room-id is required")
	}
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("text is required")
	}
	path := "/v3/rooms/" + url.PathEscape(roomID) + "/send/m.room.message"
	return c.matrixJSON(ctx, session, http.MethodPost, path, map[string]any{
		"msgtype": "m.text",
		"body":    text,
	})
}

func (c *Client) RoomMessages(ctx context.Context, session MatrixSession, roomID string, limit int) (map[string]any, error) {
	if strings.TrimSpace(roomID) == "" {
		return nil, fmt.Errorf("room-id is required")
	}
	if limit <= 0 {
		limit = 50
	}
	path := "/v3/rooms/" + url.PathEscape(roomID) + "/messages?dir=b&limit=" + strconv.Itoa(limit)
	return c.matrixJSON(ctx, session, http.MethodGet, path, nil)
}

func (c *Client) Sync(ctx context.Context, session MatrixSession, timeoutMS int, since string) (map[string]any, error) {
	values := url.Values{}
	if timeoutMS > 0 {
		values.Set("timeout", strconv.Itoa(timeoutMS))
	}
	if strings.TrimSpace(since) != "" {
		values.Set("since", strings.TrimSpace(since))
	}
	path := "/v3/sync"
	if encoded := values.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return c.matrixJSON(ctx, session, http.MethodGet, path, nil)
}

func (c *Client) matrixJSON(ctx context.Context, session MatrixSession, method, path string, body any) (map[string]any, error) {
	if strings.TrimSpace(session.AccessToken) == "" {
		return nil, fmt.Errorf("matrix session access token is required")
	}
	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.MatrixBaseURL()+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+session.AccessToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return decodeObjectResponse(res, "matrix "+method+" "+path)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}
```

- [ ] **Step 4: Run Matrix helper tests**

Run:

```powershell
go test ./internal/agentclient -run "TestCreateMatrixSession|TestSendTextMessage|TestConfig|TestCallP2PAction" -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add internal/agentclient/matrix.go internal/agentclient/matrix_test.go internal/agentclient/client.go
git commit -m "feat: add Matrix helpers for agent client"
```

---

### Task 4: Add JSON Output Rendering

**Files:**
- Create: `internal/agentclient/output.go`
- Create: `internal/agentclient/output_test.go`

- [ ] **Step 1: Write output tests**

Create `internal/agentclient/output_test.go`:

```go
package agentclient

import (
	"bytes"
	"testing"
)

func TestWriteJSONPrettyByDefault(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, map[string]any{"ok": true}, false); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\n  \"ok\": true\n}\n" {
		t.Fatalf("unexpected pretty JSON: %q", out.String())
	}
}

func TestWriteJSONRaw(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, map[string]any{"ok": true}, true); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"ok\":true}\n" {
		t.Fatalf("unexpected raw JSON: %q", out.String())
	}
}

func TestWriteErrorUsesStderrShape(t *testing.T) {
	var out bytes.Buffer
	WriteError(&out, "direxio: failed")
	if out.String() != "direxio: failed\n" {
		t.Fatalf("unexpected error output: %q", out.String())
	}
}
```

- [ ] **Step 2: Run output tests and verify failure**

Run:

```powershell
go test ./internal/agentclient -run TestWrite -count=1
```

Expected: FAIL because output functions are not defined.

- [ ] **Step 3: Implement output helpers**

Create `internal/agentclient/output.go`:

```go
package agentclient

import (
	"encoding/json"
	"fmt"
	"io"
)

func WriteJSON(w io.Writer, value any, raw bool) error {
	encoder := json.NewEncoder(w)
	if !raw {
		encoder.SetIndent("", "  ")
	}
	return encoder.Encode(value)
}

func WriteError(w io.Writer, message string) {
	_, _ = fmt.Fprintln(w, message)
}
```

- [ ] **Step 4: Run output tests**

Run:

```powershell
go test ./internal/agentclient -run TestWrite -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add internal/agentclient/output.go internal/agentclient/output_test.go
git commit -m "feat: add agent CLI JSON output helpers"
```

---

### Task 5: Build The CLI Router And Root Help

**Files:**
- Create: `cmd/direxio-cli/main.go`
- Create: `cmd/direxio-cli/cli.go`
- Create: `cmd/direxio-cli/cli_test.go`

- [ ] **Step 1: Write CLI root tests**

Create `cmd/direxio-cli/cli_test.go`:

```go
package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRootHelpMentionsCredentialsAndDomains(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected help exit 0, got %d stderr=%s", code, stderr.String())
	}
	text := stdout.String()
	for _, want := range []string{"DIREXIO_DOMAIN", "DIREXIO_AGENT_TOKEN", "contacts", "channels", "matrix", "p2p action"} {
		if !strings.Contains(text, want) {
			t.Fatalf("help missing %q:\n%s", want, text)
		}
	}
}

func TestUnknownCommandReturnsNonZero(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"missing"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected non-zero exit")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must stay empty on errors, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("expected unknown command error, got %q", stderr.String())
	}
}
```

- [ ] **Step 2: Run CLI tests and verify failure**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestRootHelp|TestUnknownCommand" -count=1
```

Expected: FAIL because the command does not exist.

- [ ] **Step 3: Implement CLI root**

Create `cmd/direxio-cli/main.go`:

```go
package main

import (
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
```

Create `cmd/direxio-cli/cli.go`:

```go
package main

import (
	"fmt"
	"io"

	"github.com/YingSuiAI/direxio-message-server/internal/agentclient"
)

const rootHelp = `direxio - CLI for Direxio P2P and Matrix agent workflows

Credentials:
  DIREXIO_DOMAIN       Site origin, for example https://example.com
  DIREXIO_AGENT_TOKEN  Portal Agent token

Commands:
  auth status
  init
  p2p action <action> --params '{}'
  p2p apis
  p2p sync-bootstrap
  contacts list
  channels list
  channels public-search
  groups list
  matrix session init
  matrix messages send --room-id ROOM --text TEXT
  matrix messages list --room-id ROOM --limit 50
  matrix sync --timeout 30s
  matrix listen

Use "direxio <domain> help" for subcommand examples.
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		_, _ = fmt.Fprint(stdout, rootHelp)
		return 0
	}
	switch args[0] {
	case "auth", "init", "p2p", "contacts", "channels", "groups", "matrix":
		return runKnown(args, stdout, stderr)
	default:
		agentclient.WriteError(stderr, "direxio: unknown command "+args[0])
		return 2
	}
}

func runKnown(args []string, stdout, stderr io.Writer) int {
	if len(args) > 1 && (args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
		_, _ = fmt.Fprint(stdout, helpFor(args[0]))
		return 0
	}
	agentclient.WriteError(stderr, "direxio: unsupported command; run direxio help")
	return 2
}

func helpFor(group string) string {
	return "direxio " + group + " help\n\nSee direxio help for available command groups.\n"
}
```

- [ ] **Step 4: Run CLI root tests**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestRootHelp|TestUnknownCommand" -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add cmd/direxio-cli/main.go cmd/direxio-cli/cli.go cmd/direxio-cli/cli_test.go
git commit -m "feat: add Direxio CLI shell"
```

---

### Task 6: Implement P2P And Product Domain Commands

**Files:**
- Modify: `cmd/direxio-cli/cli.go`
- Modify: `cmd/direxio-cli/cli_test.go`

- [ ] **Step 1: Add CLI command tests for P2P routing**

Append to `cmd/direxio-cli/cli_test.go`:

```go
func TestP2PActionRequiresParamsJSON(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")
	var stdout, stderr bytes.Buffer
	code := run([]string{"p2p", "action", "channels.list", "--params", "not-json"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected invalid params to fail")
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout must be empty on failure, got %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "invalid params json") {
		t.Fatalf("unexpected stderr %q", stderr.String())
	}
}

func TestChannelsHelpIncludesExample(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"channels", "help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected help success, got %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "direxio channels list") {
		t.Fatalf("channels help missing list example:\n%s", stdout.String())
	}
}
```

- [ ] **Step 2: Run tests and verify failure**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestP2PActionRequiresParamsJSON|TestChannelsHelpIncludesExample" -count=1
```

Expected: FAIL because P2P parsing and useful help are missing from the current CLI shell.

- [ ] **Step 3: Implement command helpers and product command dispatch**

Replace `runKnown` and `helpFor` in `cmd/direxio-cli/cli.go` with this implementation, and add the imports `context`, `encoding/json`, `flag`, and `time`:

```go
func runKnown(args []string, stdout, stderr io.Writer) int {
	if len(args) > 1 && (args[1] == "help" || args[1] == "--help" || args[1] == "-h") {
		_, _ = fmt.Fprint(stdout, helpFor(args[0]))
		return 0
	}
	cfg, err := agentclient.ConfigFromEnv()
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 2
	}
	client := agentclient.New(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw := hasRawFlag(args)
	var result map[string]any

	switch args[0] {
	case "auth":
		result, err = runAuth(ctx, client, args[1:])
	case "init":
		result, err = client.CallP2PAction(ctx, "portal.status", nil, agentclient.P2PQuery)
	case "p2p":
		result, err = runP2P(ctx, client, args[1:])
	case "contacts":
		result, err = runSimpleDomain(ctx, client, args[1:], "contacts.list", agentclient.P2PQuery)
	case "channels":
		result, err = runChannels(ctx, client, args[1:])
	case "groups":
		result, err = runSimpleDomain(ctx, client, args[1:], "groups.list", agentclient.P2PQuery)
	case "matrix":
		result, err = runMatrix(ctx, client, args[1:])
	}
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	if err := agentclient.WriteJSON(stdout, result, raw); err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	return 0
}

func runAuth(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "status" {
		return client.CallP2PAction(ctx, "portal.status", nil, agentclient.P2PQuery)
	}
	return nil, fmt.Errorf("usage: direxio auth status")
}

func runP2P(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "apis" {
		return client.CallP2PAction(ctx, "apis.status", nil, agentclient.P2PQuery)
	}
	if len(args) == 1 && args[0] == "sync-bootstrap" {
		return client.CallP2PAction(ctx, "sync.bootstrap", nil, agentclient.P2PQuery)
	}
	if len(args) >= 2 && args[0] == "action" {
		fs := flag.NewFlagSet("p2p action", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		paramsJSON := fs.String("params", "{}", "JSON params")
		raw := fs.Bool("raw", false, "compact JSON output")
		if err := fs.Parse(args[2:]); err != nil {
			return nil, err
		}
		_ = raw
		var params map[string]any
		if err := json.Unmarshal([]byte(*paramsJSON), &params); err != nil {
			return nil, fmt.Errorf("invalid params json: %w", err)
		}
		return client.CallP2PAction(ctx, args[1], params, agentclient.P2PCommand)
	}
	return nil, fmt.Errorf("usage: direxio p2p action <action> --params '{}'")
}

func runSimpleDomain(ctx context.Context, client *agentclient.Client, args []string, action string, route agentclient.P2PRoute) (map[string]any, error) {
	if len(args) == 1 && args[0] == "list" {
		return client.CallP2PAction(ctx, action, nil, route)
	}
	return nil, fmt.Errorf("unsupported command")
}

func runChannels(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "list" {
		return client.CallP2PAction(ctx, "channels.list", nil, agentclient.P2PQuery)
	}
	if len(args) >= 1 && args[0] == "public-search" {
		return client.CallP2PAction(ctx, "channels.public.search", nil, agentclient.P2PQuery)
	}
	return nil, fmt.Errorf("unsupported channels command")
}

func hasRawFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--raw" {
			return true
		}
	}
	return false
}
```

Replace `helpFor` with:

```go
func helpFor(group string) string {
	switch group {
	case "channels":
		return `direxio channels - channel workflows

Examples:
  direxio channels list
  direxio channels public-search

Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
	case "p2p":
		return `direxio p2p - raw P2P action fallback

Examples:
  direxio p2p apis
  direxio p2p sync-bootstrap
  direxio p2p action channels.list --params "{}"

Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
	case "contacts":
		return "direxio contacts list\n\nRequires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.\n"
	case "groups":
		return "direxio groups list\n\nRequires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.\n"
	case "auth":
		return "direxio auth status\n\nChecks portal status through the configured Agent token.\n"
	case "matrix":
		return "direxio matrix help\n\nMatrix commands are implemented in the Matrix task.\n"
	default:
		return rootHelp
	}
}
```

- [ ] **Step 4: Run CLI tests**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestP2PActionRequiresParamsJSON|TestChannelsHelpIncludesExample|TestRootHelp|TestUnknownCommand" -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add cmd/direxio-cli/cli.go cmd/direxio-cli/cli_test.go
git commit -m "feat: add P2P domain CLI commands"
```

---

### Task 7: Implement Matrix CLI Commands

**Files:**
- Modify: `cmd/direxio-cli/cli.go`
- Modify: `cmd/direxio-cli/cli_test.go`

- [ ] **Step 1: Add Matrix CLI tests**

Append to `cmd/direxio-cli/cli_test.go`:

```go
func TestMatrixHelpIncludesMessageExamples(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "help"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("expected matrix help success, got %d stderr=%s", code, stderr.String())
	}
	help := stdout.String()
	for _, want := range []string{"matrix session init", "matrix messages send", "matrix messages list", "matrix sync", "matrix listen"} {
		if !strings.Contains(help, want) {
			t.Fatalf("matrix help missing %q:\n%s", want, help)
		}
	}
}

func TestMatrixMessagesSendRequiresRoomID(t *testing.T) {
	t.Setenv("DIREXIO_DOMAIN", "https://example.com")
	t.Setenv("DIREXIO_AGENT_TOKEN", "agent-token")
	var stdout, stderr bytes.Buffer
	code := run([]string{"matrix", "messages", "send", "--text", "hello"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("expected missing room-id to fail")
	}
	if !strings.Contains(stderr.String(), "room-id is required") {
		t.Fatalf("unexpected stderr %q", stderr.String())
	}
}
```

- [ ] **Step 2: Run Matrix CLI tests and verify failure**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestMatrixHelpIncludesMessageExamples|TestMatrixMessagesSendRequiresRoomID" -count=1
```

Expected: FAIL because Matrix help and Matrix command parsing are missing from the current CLI shell.

- [ ] **Step 3: Implement Matrix command parsing**

Add this `runMatrix` implementation to `cmd/direxio-cli/cli.go`:

```go
func runMatrix(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) >= 2 && args[0] == "session" && args[1] == "init" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"device_id":  session.DeviceID,
			"user_id":    session.UserID,
			"homeserver": session.Homeserver,
			"status":     "ok",
		}, nil
	}
	if len(args) >= 2 && args[0] == "messages" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		switch args[1] {
		case "send":
			fs := flag.NewFlagSet("matrix messages send", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			roomID := fs.String("room-id", "", "Matrix room id")
			text := fs.String("text", "", "message text")
			if err := fs.Parse(args[2:]); err != nil {
				return nil, err
			}
			return client.SendTextMessage(ctx, session, *roomID, *text)
		case "list":
			fs := flag.NewFlagSet("matrix messages list", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			roomID := fs.String("room-id", "", "Matrix room id")
			limit := fs.Int("limit", 50, "message limit")
			if err := fs.Parse(args[2:]); err != nil {
				return nil, err
			}
			return client.RoomMessages(ctx, session, *roomID, *limit)
		}
	}
	if len(args) >= 1 && args[0] == "sync" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		fs := flag.NewFlagSet("matrix sync", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		timeout := fs.Int("timeout-ms", 30000, "sync timeout in milliseconds")
		since := fs.String("since", "", "sync token")
		if err := fs.Parse(args[1:]); err != nil {
			return nil, err
		}
		return client.Sync(ctx, session, *timeout, *since)
	}
	if len(args) >= 1 && args[0] == "listen" {
		return nil, fmt.Errorf("matrix listen is implemented after one-shot sync; use matrix sync --timeout-ms 30000 in this task")
	}
	return nil, fmt.Errorf("unsupported matrix command")
}
```

Update `helpFor("matrix")` to:

```go
	case "matrix":
		return `direxio matrix - Matrix Client-Server workflows

Examples:
  direxio matrix session init
  direxio matrix messages send --room-id "!room:example.com" --text "hello"
  direxio matrix messages list --room-id "!room:example.com" --limit 50
  direxio matrix sync --timeout-ms 30000
  direxio matrix listen

The Matrix access token is obtained internally with the Agent token and is not printed.
Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
```

- [ ] **Step 4: Run Matrix CLI tests**

Run:

```powershell
go test ./cmd/direxio-cli -run "TestMatrixHelpIncludesMessageExamples|TestMatrixMessagesSendRequiresRoomID|TestRootHelp" -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add cmd/direxio-cli/cli.go cmd/direxio-cli/cli_test.go
git commit -m "feat: add Matrix CLI commands"
```

---

### Task 8: Add Matrix Listen NDJSON

**Files:**
- Modify: `internal/agentclient/output.go`
- Modify: `internal/agentclient/output_test.go`
- Modify: `internal/agentclient/matrix.go`
- Modify: `internal/agentclient/matrix_test.go`
- Modify: `cmd/direxio-cli/cli.go`

- [ ] **Step 1: Add NDJSON output test**

Append to `internal/agentclient/output_test.go`:

```go
func TestWriteNDJSONWritesOneLine(t *testing.T) {
	var out bytes.Buffer
	if err := WriteNDJSON(&out, map[string]any{"type": "m.room.message"}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "{\"type\":\"m.room.message\"}\n" {
		t.Fatalf("unexpected ndjson output: %q", out.String())
	}
}
```

- [ ] **Step 2: Add sync event extraction tests**

Append to `internal/agentclient/matrix_test.go`:

```go
func TestExtractSyncTimelineEvents(t *testing.T) {
	events := ExtractSyncTimelineEvents(map[string]any{
		"rooms": map[string]any{
			"join": map[string]any{
				"!room:example.com": map[string]any{
					"timeline": map[string]any{
						"events": []any{
							map[string]any{"event_id": "$event", "type": "m.room.message"},
						},
					},
				},
			},
		},
	})
	if len(events) != 1 {
		t.Fatalf("expected one event, got %#v", events)
	}
	if events[0]["room_id"] != "!room:example.com" || events[0]["event_id"] != "$event" {
		t.Fatalf("expected room id to be attached to event, got %#v", events[0])
	}
}
```

- [ ] **Step 3: Implement NDJSON helper and sync event extraction**

Add to `internal/agentclient/output.go`:

```go
func WriteNDJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(value)
}
```

Add to `internal/agentclient/matrix.go`:

```go
func ExtractSyncTimelineEvents(sync map[string]any) []map[string]any {
	rooms, _ := sync["rooms"].(map[string]any)
	join, _ := rooms["join"].(map[string]any)
	var out []map[string]any
	for roomID, rawRoom := range join {
		room, _ := rawRoom.(map[string]any)
		timeline, _ := room["timeline"].(map[string]any)
		events, _ := timeline["events"].([]any)
		for _, rawEvent := range events {
			event, ok := rawEvent.(map[string]any)
			if !ok {
				continue
			}
			copy := make(map[string]any, len(event)+1)
			for key, value := range event {
				copy[key] = value
			}
			copy["room_id"] = roomID
			out = append(out, copy)
		}
	}
	return out
}
```

- [ ] **Step 4: Run output and extraction tests**

Run:

```powershell
go test ./internal/agentclient -run "TestWriteJSON|TestWriteNDJSON|TestWriteError|TestExtractSyncTimelineEvents" -count=1
```

Expected: PASS.

- [ ] **Step 5: Implement `matrix listen` as NDJSON output**

In `cmd/direxio-cli/cli.go`, add this branch to `runKnown` immediately after creating `client`, `ctx`, and `raw`:

```go
	if args[0] == "matrix" && len(args) >= 2 && args[1] == "listen" {
		if raw {
			return runMatrixListen(ctx, client, args[2:], stdout, stderr)
		}
		return runMatrixListen(ctx, client, args[2:], stdout, stderr)
	}
```

Add this function to `cmd/direxio-cli/cli.go`:

```go
func runMatrixListen(ctx context.Context, client *agentclient.Client, args []string, stdout, stderr io.Writer) int {
	session, err := client.CreateMatrixSession(ctx)
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	fs := flag.NewFlagSet("matrix listen", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeout := fs.Int("timeout-ms", 30000, "sync timeout in milliseconds")
	since := fs.String("since", "", "sync token")
	if err := fs.Parse(args); err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 2
	}
	sync, err := client.Sync(ctx, session, *timeout, *since)
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	for _, event := range agentclient.ExtractSyncTimelineEvents(sync) {
		if err := agentclient.WriteNDJSON(stdout, event); err != nil {
			agentclient.WriteError(stderr, "direxio: "+err.Error())
			return 1
		}
	}
	return 0
}
```

Replace the existing `matrix listen` branch inside `runMatrix` with:

```go
	if len(args) >= 1 && args[0] == "listen" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		return client.Sync(ctx, session, 30000, "")
	}
```

The `runMatrix` branch is only a fallback for tests that call it directly. Normal CLI execution takes the `runMatrixListen` branch and writes NDJSON events to stdout.

- [ ] **Step 6: Run package tests**

Run:

```powershell
go test ./internal/agentclient ./cmd/direxio-cli -count=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```powershell
git add internal/agentclient/output.go internal/agentclient/output_test.go internal/agentclient/matrix.go internal/agentclient/matrix_test.go cmd/direxio-cli/cli.go
git commit -m "feat: add Matrix listen NDJSON output"
```

---

### Task 9: Add Build Scripts

**Files:**
- Create: `scripts/build-agent-tools.ps1`
- Create: `scripts/build-agent-tools.sh`

- [ ] **Step 1: Create PowerShell build script**

Create `scripts/build-agent-tools.ps1`:

```powershell
param(
  [string]$OutputDir = "dist/agent-tools"
)

$ErrorActionPreference = "Stop"
$targets = @(
  @{ GOOS = "windows"; GOARCH = "amd64"; Ext = ".exe" },
  @{ GOOS = "windows"; GOARCH = "arm64"; Ext = ".exe" },
  @{ GOOS = "linux"; GOARCH = "amd64"; Ext = "" },
  @{ GOOS = "linux"; GOARCH = "arm64"; Ext = "" },
  @{ GOOS = "darwin"; GOARCH = "amd64"; Ext = "" },
  @{ GOOS = "darwin"; GOARCH = "arm64"; Ext = "" }
)

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

foreach ($target in $targets) {
  $env:GOOS = $target.GOOS
  $env:GOARCH = $target.GOARCH
  $name = "direxio-cli-$($target.GOOS)-$($target.GOARCH)$($target.Ext)"
  go build -o (Join-Path $OutputDir $name) ./cmd/direxio-cli
}

Remove-Item Env:\GOOS -ErrorAction SilentlyContinue
Remove-Item Env:\GOARCH -ErrorAction SilentlyContinue
```

- [ ] **Step 2: Create POSIX build script**

Create `scripts/build-agent-tools.sh`:

```sh
#!/usr/bin/env sh
set -eu

output_dir="${1:-dist/agent-tools}"
mkdir -p "$output_dir"

build_one() {
  goos="$1"
  goarch="$2"
  ext="$3"
  GOOS="$goos" GOARCH="$goarch" go build -o "$output_dir/direxio-cli-$goos-$goarch$ext" ./cmd/direxio-cli
}

build_one windows amd64 .exe
build_one windows arm64 .exe
build_one linux amd64 ""
build_one linux arm64 ""
build_one darwin amd64 ""
build_one darwin arm64 ""
```

- [ ] **Step 3: Run local build checks**

Run:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\build-agent-tools.ps1 -OutputDir dist\agent-tools-test
Get-ChildItem dist\agent-tools-test
```

Expected: six `direxio-cli-*` binaries.

- [ ] **Step 4: Commit**

```powershell
git add scripts/build-agent-tools.ps1 scripts/build-agent-tools.sh
git commit -m "build: add cross-platform agent tool builds"
```

---

### Task 10: Add Agent Skill Recipes

**Files:**
- Create: `docs/agent-skills/codex-direxio-cli.md`
- Create: `docs/agent-skills/claude-code-direxio-cli.md`
- Create: `docs/agent-skills/openclaw-direxio-cli.md`
- Create: `docs/agent-skills/hermes-direxio-cli.md`
- Create: `.codex/skills/direxio-cli/SKILL.md`

- [ ] **Step 1: Create the shared Codex recipe**

Create `docs/agent-skills/codex-direxio-cli.md`:

```md
# Direxio CLI Recipe for Codex

Use `direxio-cli` when the user asks to inspect or operate a Direxio P2P node.

Required environment:

```powershell
$env:DIREXIO_DOMAIN="https://example.com"
$env:DIREXIO_AGENT_TOKEN="<agent token>"
```

Start by checking:

```powershell
direxio auth status
direxio p2p apis
```

Common workflows:

```powershell
direxio contacts list
direxio channels list
direxio groups list
direxio matrix session init
direxio matrix messages send --room-id "!room:example.com" --text "hello"
direxio matrix messages list --room-id "!room:example.com" --limit 50
direxio matrix sync --timeout-ms 30000
```

Use `direxio p2p action <action> --params '{}'` only when no domain command exists. Ask the user before delete, dissolve, remove, mute, redaction, approval, or other high-risk mutation actions.
```

- [ ] **Step 2: Create tool-specific recipe aliases**

Create `docs/agent-skills/claude-code-direxio-cli.md`:

```md
# Direxio CLI Recipe for Claude Code

Follow the Codex recipe in `docs/agent-skills/codex-direxio-cli.md`. Prefer domain commands over raw P2P actions, keep stdout as machine-readable JSON, and ask for confirmation before high-risk mutations.
```

Create `docs/agent-skills/openclaw-direxio-cli.md`:

```md
# Direxio CLI Recipe for OpenClaw

Follow the Codex recipe in `docs/agent-skills/codex-direxio-cli.md`. Configure `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN`, inspect `direxio help`, and use domain commands for contacts, channels, groups, and Matrix messages.
```

Create `docs/agent-skills/hermes-direxio-cli.md`:

```md
# Direxio CLI Recipe for Hermes-Style Plugins

Use the CLI as the process boundary for Direxio node operations. Configure `DIREXIO_DOMAIN` and `DIREXIO_AGENT_TOKEN`, run `direxio auth status`, then use Matrix message commands for send/list/sync workflows. Do not request or display Matrix access tokens.
```

- [ ] **Step 3: Create project-local Codex skill**

Create `.codex/skills/direxio-cli/SKILL.md`:

```md
---
name: direxio-cli
description: Use Direxio CLI to operate this P2P Matrix app through DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
---

# Direxio CLI

Use this skill when a user asks an agent to interact with a Direxio node through the first-party CLI.

Before running commands, verify both environment variables are set:

- `DIREXIO_DOMAIN`
- `DIREXIO_AGENT_TOKEN`

Start with:

```powershell
direxio auth status
direxio p2p apis
```

Prefer domain commands:

```powershell
direxio contacts list
direxio channels list
direxio groups list
direxio matrix messages list --room-id "!room:example.com" --limit 50
```

Use `direxio p2p action <action> --params '{}'` only when no domain command exists. Ask the user before destructive or moderation actions.
```

- [ ] **Step 4: Validate docs**

Run:

```powershell
rg -n "DIREXIO_DOMAIN|DIREXIO_AGENT_TOKEN|direxio auth status" docs/agent-skills .codex/skills/direxio-cli/SKILL.md
git diff --check -- docs/agent-skills .codex/skills/direxio-cli/SKILL.md
```

Expected: each recipe mentions the required credentials or points to the Codex recipe; diff check emits no output.

- [ ] **Step 5: Commit**

```powershell
git add docs/agent-skills .codex/skills/direxio-cli/SKILL.md
git commit -m "docs: add Direxio CLI agent recipes"
```

---

### Task 11: Final Verification Pass

**Files:**
- No new files unless test failures require focused fixes.

- [ ] **Step 1: Run focused Go tests**

Run:

```powershell
go test ./internal/agentclient ./cmd/direxio-cli ./p2p -count=1
```

Expected: PASS.

- [ ] **Step 2: Build the CLI**

Run:

```powershell
go build ./cmd/direxio-cli
```

Expected: PASS.

- [ ] **Step 3: Validate Postman JSON**

Run:

```powershell
Get-Content docs/postman/direxio-message-server.postman_collection.json | ConvertFrom-Json | Out-Null
```

Expected: no output and exit code 0.

- [ ] **Step 4: Validate compose files are unaffected**

Run:

```powershell
docker compose -f docker-compose.p2p.yml config
docker compose -f docker-compose.p2p-dual.yml config
```

Expected: both commands produce normalized compose config. If Docker is unavailable, record the failure and do not block the CLI implementation on local Docker availability.

- [ ] **Step 5: Run lint if available**

Run:

```powershell
golangci-lint run
```

Expected: PASS. If `golangci-lint` is not installed, run:

```powershell
gofmt -w p2p/service.go p2p/password_test.go p2p/routing_test.go internal/agentclient cmd/direxio-cli
```

Then record that full lint was skipped because `golangci-lint` was unavailable.

- [ ] **Step 6: Check staged/untracked boundaries**

Run:

```powershell
git status --short
```

Expected: only intended files remain modified. Existing unrelated files such as `Dockerfile` or `docs/client-ai-interface-migration-2026-06-22.md` must not be staged unless the user explicitly includes them.

- [ ] **Step 7: Final commit**

If verification changes files, commit them:

```powershell
git add p2p internal/agentclient cmd/direxio-cli scripts/build-agent-tools.ps1 scripts/build-agent-tools.sh docs/api-interface-change-record.md docs/postman/direxio-message-server.postman_collection.json docs/agent-skills .codex/skills/direxio-cli/SKILL.md
git commit -m "chore: verify Direxio agent CLI"
```

If no files changed after verification, do not create an empty commit.

---

## Self-Review

Spec coverage:

- Developer Operator credentials are covered by Task 2 and Task 5.
- Server-side Agent Matrix Session contract is covered by Task 1.
- Shared Go client is covered by Tasks 2 and 3.
- Domain CLI commands and fallback P2P action are covered by Tasks 5, 6, and 7.
- Pretty JSON, `--raw`, stderr, and non-zero exit behavior are covered by Task 4 and CLI tests.
- Matrix send/list/sync/listen are covered by Tasks 3, 7, and 8.
- Help text and examples are covered by Tasks 5, 6, 7, and 10.
- Cross-platform builds are covered by Task 9.
- Agent recipes are covered by Task 10.
- Thin MCP is intentionally outside this first implementation plan because the approved design makes CLI the primary first phase and MCP optional after the CLI stabilizes.

Placeholder scan:

- No placeholder markers or postponed-work instructions are present.
- MCP is treated as a separate follow-up plan, matching the spec phasing.

Type consistency:

- `agentclient.Config`, `agentclient.Client`, `agentclient.MatrixSession`, `P2PQuery`, and `P2PCommand` are introduced before use by CLI tasks.
- CLI command names match the approved design: `auth status`, `init`, `p2p action`, `p2p apis`, `p2p sync-bootstrap`, `contacts list`, `channels list`, `channels public-search`, `groups list`, `matrix session init`, `matrix messages send/list`, `matrix sync`, and `matrix listen`.
