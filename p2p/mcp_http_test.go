package p2p

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
)

func TestMCPHTTPInitializeAndToolsListRequireAgentToken(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	initialize := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
		},
	}

	for _, tc := range []struct {
		name  string
		token string
	}{
		{name: "missing bearer"},
		{name: "owner token", token: service.AccessToken()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := jsonRequest(t, "/mcp", initialize)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("expected MCP request to return 401, got %d body=%s", rec.Code, rec.Body.String())
			}
		})
	}

	agentAuth := jsonRequest(t, "/mcp", initialize)
	agentAuth.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentAuthRec := httptest.NewRecorder()
	router.ServeHTTP(agentAuthRec, agentAuth)
	if agentAuthRec.Code != http.StatusOK {
		t.Fatalf("expected initialize to succeed with agent token, got %d body=%s", agentAuthRec.Code, agentAuthRec.Body.String())
	}
	initializeResult := jsonRPCResult(t, agentAuthRec)
	if initializeResult["protocolVersion"] == "" {
		t.Fatalf("expected initialize protocolVersion, got %#v", initializeResult)
	}
	serverInfo, ok := initializeResult["serverInfo"].(map[string]any)
	if !ok || serverInfo["version"] != "v1.0.3" {
		t.Fatalf("expected canonical MCP server version, got %#v", initializeResult["serverInfo"])
	}
	if _, ok := initializeResult["capabilities"].(map[string]any)["tools"]; !ok {
		t.Fatalf("expected initialize tools capability, got %#v", initializeResult["capabilities"])
	}

	toolsList := jsonRequest(t, "/mcp", map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
	})
	toolsList.Header.Set("Authorization", "Bearer "+service.AgentToken())
	toolsRec := httptest.NewRecorder()
	router.ServeHTTP(toolsRec, toolsList)
	if toolsRec.Code != http.StatusOK {
		t.Fatalf("expected tools/list to succeed, got %d body=%s", toolsRec.Code, toolsRec.Body.String())
	}
	toolsResult := jsonRPCResult(t, toolsRec)
	tools, ok := toolsResult["tools"].([]any)
	if !ok || len(tools) == 0 {
		t.Fatalf("expected tools/list result tools, got %#v", toolsResult)
	}
	if !mcpToolsContain(tools, "dirextalk_messages_list") {
		t.Fatalf("expected dirextalk_messages_list in tools/list, got %#v", tools)
	}
	allowedCloudTools := map[string]bool{
		"dirextalk_cloud_workloads_list": true,
		"dirextalk_cloud_workloads_get":  true,
		"dirextalk_cloud_status":         true,
	}
	for _, tool := range tools {
		item, ok := tool.(map[string]any)
		if !ok {
			t.Fatalf("unexpected MCP tool shape: %#v", tool)
		}
		name, _ := item["name"].(string)
		if strings.Contains(strings.ToLower(name), "cloud") && !allowedCloudTools[name] {
			t.Fatalf("public MCP must expose only read-only Cloud tools, got %#v", item)
		}
	}
	for name := range allowedCloudTools {
		if !mcpToolsContain(tools, name) {
			t.Fatalf("expected read-only Cloud MCP tool %q, got %#v", name, tools)
		}
	}
}

func TestMCPHTTPCloudToolsAreReadOnlyAndDoNotExposeGoalPrompts(t *testing.T) {
	service := newCloudConnectedService(t)
	router := newP2PTestRouter(service)
	goal := "Deploy a private knowledge node with internal source constraints."
	create := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.goals.create",
		"params": map[string]any{
			"goal":                goal,
			"cloud_connection_id": cloudTestConnectionID,
			"idempotency_key":     "bdf0ee8d-9ec2-4f4b-9bbd-a00000000001",
		},
	})
	create.Header.Set("Authorization", "Bearer "+service.AccessToken())
	createRecorder := httptest.NewRecorder()
	router.ServeHTTP(createRecorder, create)
	if createRecorder.Code != http.StatusOK {
		t.Fatalf("create cloud goal = %d body=%s", createRecorder.Code, createRecorder.Body.String())
	}
	created := decodeJSONMap(t, createRecorder.Body.String())
	goalID := created["goal"].(map[string]any)["goal_id"].(string)
	planID := created["plan"].(map[string]any)["plan_id"].(string)

	for _, toolCall := range []map[string]any{
		{"name": "dirextalk_cloud_status", "arguments": map[string]any{}},
		{"name": "dirextalk_cloud_workloads_list", "arguments": map[string]any{"kind": "plan"}},
		{"name": "dirextalk_cloud_workloads_get", "arguments": map[string]any{"kind": "plan", "id": planID}},
	} {
		req := jsonRequest(t, "/mcp", map[string]any{
			"jsonrpc": "2.0", "id": toolCall["name"], "method": "tools/call", "params": toolCall,
		})
		req.Header.Set("Authorization", "Bearer "+service.AgentToken())
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK {
			t.Fatalf("MCP %s = %d body=%s", toolCall["name"], recorder.Code, recorder.Body.String())
		}
		result := jsonRPCResult(t, recorder)
		if result["isError"] == true || strings.Contains(recorder.Body.String(), goal) {
			t.Fatalf("MCP %s must return a de-secretsed Cloud projection, got %#v", toolCall["name"], result)
		}
	}

	for _, arguments := range []map[string]any{
		{"kind": "goal", "id": goalID},
		{"kind": "goal"},
	} {
		req := jsonRequest(t, "/mcp", map[string]any{
			"jsonrpc": "2.0", "id": "cloud-goal-denied", "method": "tools/call",
			"params": map[string]any{"name": "dirextalk_cloud_workloads_get", "arguments": arguments},
		})
		req.Header.Set("Authorization", "Bearer "+service.AgentToken())
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		if recorder.Code != http.StatusOK || !jsonRPCResult(t, recorder)["isError"].(bool) || strings.Contains(recorder.Body.String(), goal) {
			t.Fatalf("MCP must reject Goal projection access, status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
}

func TestMCPHTTPToolsCallInvokesUnifiedService(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	invoker := &recordingDirextalkMCPInvoker{}
	service.mcpCapabilities = dirextalkmcp.NewService(invoker)
	router := newP2PTestRouter(service)

	req := jsonRequest(t, "/mcp", map[string]any{
		"jsonrpc": "2.0",
		"id":      "call-1",
		"method":  "tools/call",
		"params": map[string]any{
			"name": "dirextalk_messages_list",
			"arguments": map[string]any{
				"room_id": "!room:example.com",
			},
		},
	})
	req.Header.Set("Authorization", "Bearer "+service.AgentToken())
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected tools/call to succeed, got %d body=%s", rec.Code, rec.Body.String())
	}
	result := jsonRPCResult(t, rec)
	if result["isError"] == true {
		t.Fatalf("expected successful MCP tool result, got %#v", result)
	}
	structured, ok := result["structuredContent"].(map[string]any)
	if !ok {
		t.Fatalf("expected structuredContent map, got %#v", result)
	}
	if structured["via"] != "unified-dirextalkmcp" || structured["action"] != dirextalkmcp.ActionMessagesList {
		t.Fatalf("expected tools/call to use unified MCP service, got %#v", structured)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) == 0 {
		t.Fatalf("expected text content, got %#v", result)
	}
	textContent, ok := content[0].(map[string]any)
	if !ok || textContent["type"] != "text" || !strings.Contains(textContent["text"].(string), "unified-dirextalkmcp") {
		t.Fatalf("expected JSON text content for MCP client, got %#v", content)
	}
	if invoker.action != dirextalkmcp.ActionMessagesList || invoker.params["room_id"] != "!room:example.com" {
		t.Fatalf("expected unified MCP invoker params, action=%s params=%#v", invoker.action, invoker.params)
	}
	for _, key := range []string{"Authorization", "authorization", "access_token", "agent_token", "token"} {
		if _, ok := invoker.params[key]; ok {
			t.Fatalf("inbound bearer token leaked into MCP arguments under %q: %#v", key, invoker.params)
		}
	}
}

func TestMCPHTTPRejectsQueryTokensBadOriginAndGET(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	router := newP2PTestRouter(service)

	for _, tc := range []struct {
		name, method, path, origin string
		want                       int
	}{
		{"GET", http.MethodGet, "/mcp", "", http.StatusMethodNotAllowed},
		{"query token", http.MethodPost, "/mcp?access_token=owner-token", "", http.StatusBadRequest},
		{"cross origin", http.MethodPost, "/mcp", "https://evil.example", http.StatusForbidden},
		{"same origin", http.MethodPost, "/mcp", "http://example.com", http.StatusOK},
		{"retired path", http.MethodPost, "/_p2p/mcp", "", http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.method == http.MethodGet {
				req = httptest.NewRequest(tc.method, tc.path, nil)
			} else {
				req = jsonRequest(t, tc.path, mcpInitializeRequest())
			}
			req.Host = "example.com"
			req.Header.Set("Authorization", "Bearer "+service.AgentToken())
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func mcpInitializeRequest() map[string]any {
	return map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
	}
}

func jsonRPCResult(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["error"] != nil {
		t.Fatalf("expected JSON-RPC result, got error response %#v", got)
	}
	result, ok := got["result"].(map[string]any)
	if !ok {
		t.Fatalf("expected JSON-RPC result object, got %#v", got)
	}
	return result
}

func mcpToolsContain(tools []any, name string) bool {
	for _, rawTool := range tools {
		tool, ok := rawTool.(map[string]any)
		if ok && tool["name"] == name {
			return true
		}
	}
	return false
}
