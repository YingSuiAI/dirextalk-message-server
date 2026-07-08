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

	missingAuth := jsonRequest(t, "/mcp", initialize)
	missingAuthRec := httptest.NewRecorder()
	router.ServeHTTP(missingAuthRec, missingAuth)
	if missingAuthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected missing MCP bearer to return 401, got %d body=%s", missingAuthRec.Code, missingAuthRec.Body.String())
	}

	ownerAuth := jsonRequest(t, "/mcp", initialize)
	ownerAuth.Header.Set("Authorization", "Bearer "+service.AccessToken())
	ownerAuthRec := httptest.NewRecorder()
	router.ServeHTTP(ownerAuthRec, ownerAuth)
	if ownerAuthRec.Code != http.StatusUnauthorized {
		t.Fatalf("expected owner token to be rejected by MCP HTTP endpoint, got %d body=%s", ownerAuthRec.Code, ownerAuthRec.Body.String())
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

	getReq := httptest.NewRequest(http.MethodGet, "/mcp", nil)
	getReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected MCP GET to return 405, got %d body=%s", getRec.Code, getRec.Body.String())
	}

	queryTokenReq := jsonRequest(t, "/mcp?access_token=owner-token", mcpInitializeRequest())
	queryTokenReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	queryTokenRec := httptest.NewRecorder()
	router.ServeHTTP(queryTokenRec, queryTokenReq)
	if queryTokenRec.Code != http.StatusBadRequest {
		t.Fatalf("expected MCP query tokens to return 400, got %d body=%s", queryTokenRec.Code, queryTokenRec.Body.String())
	}

	badOriginReq := jsonRequest(t, "/mcp", mcpInitializeRequest())
	badOriginReq.Host = "example.com"
	badOriginReq.Header.Set("Origin", "https://evil.example")
	badOriginReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	badOriginRec := httptest.NewRecorder()
	router.ServeHTTP(badOriginRec, badOriginReq)
	if badOriginRec.Code != http.StatusForbidden {
		t.Fatalf("expected bad MCP Origin to return 403, got %d body=%s", badOriginRec.Code, badOriginRec.Body.String())
	}

	sameOriginReq := jsonRequest(t, "/mcp", mcpInitializeRequest())
	sameOriginReq.Host = "example.com"
	sameOriginReq.Header.Set("Origin", "http://example.com")
	sameOriginReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	sameOriginRec := httptest.NewRecorder()
	router.ServeHTTP(sameOriginRec, sameOriginReq)
	if sameOriginRec.Code != http.StatusOK {
		t.Fatalf("expected same MCP Origin to succeed, got %d body=%s", sameOriginRec.Code, sameOriginRec.Body.String())
	}

	oldPathReq := jsonRequest(t, "/_p2p/mcp", mcpInitializeRequest())
	oldPathReq.Header.Set("Authorization", "Bearer "+service.AgentToken())
	oldPathRec := httptest.NewRecorder()
	router.ServeHTTP(oldPathRec, oldPathReq)
	if oldPathRec.Code != http.StatusNotFound {
		t.Fatalf("expected old /_p2p/mcp path to stay unavailable, got %d body=%s", oldPathRec.Code, oldPathRec.Body.String())
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
