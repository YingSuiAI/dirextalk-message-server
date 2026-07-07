package nativeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPMCPToolCanBeInstalledAndUsedByModelLoop(t *testing.T) {
	mcpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode mcp request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch payload["method"] {
		case "tools/list":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"echo","description":"Echo text.","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]}}`))
		case "tools/call":
			_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"mcp-echo"}]}}`))
		default:
			t.Fatalf("unexpected mcp method %#v", payload["method"])
		}
	}))
	defer mcpServer.Close()

	var modelCalls int
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		modelCalls++
		w.Header().Set("Content-Type", "application/json")
		if modelCalls == 1 {
			_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"mcp__echo-server__echo","arguments":"{\"text\":\"hi\"}"}}]}}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"MCP 工具返回 mcp-echo。"}}]}`))
	}))
	defer modelServer.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	ctx := context.Background()
	install, err := runtime.Invoke(ctx, "agent.mcp.servers.install", map[string]any{
		"id":             "echo-server",
		"transport":      "http",
		"url":            mcpServer.URL,
		"discover_tools": true,
	})
	if err != nil {
		t.Fatalf("install http mcp: %v", err)
	}
	serverRecord := install["server"].(map[string]any)
	if len(configList(serverRecord, "tools")) != 1 {
		t.Fatalf("expected discovered mcp tool, got %#v", install)
	}
	list, err := runtime.Invoke(ctx, "agent.mcp.servers.list", nil)
	if err != nil {
		t.Fatalf("list mcp servers: %v", err)
	}
	servers := list["servers"].([]map[string]any)
	if len(servers) != 1 || servers[0]["id"] != "echo-server" {
		t.Fatalf("expected installed mcp server in list, got %#v", list)
	}
	result, err := runtime.Invoke(ctx, "agent.chat", map[string]any{
		"prompt":        "call mcp",
		"enabled_tools": []any{"all"},
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": modelServer.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat with mcp tool: %v", err)
	}
	if result["text"] != "MCP 工具返回 mcp-echo。" || modelCalls != 2 {
		t.Fatalf("expected mcp model loop final answer, calls=%d result=%#v", modelCalls, result)
	}
	if _, err := runtime.Invoke(ctx, "agent.mcp.servers.disable", map[string]any{"id": "echo-server"}); err != nil {
		t.Fatalf("disable mcp server: %v", err)
	}
	if _, err := runtime.Invoke(ctx, "agent.mcp.servers.enable", map[string]any{"id": "echo-server"}); err != nil {
		t.Fatalf("enable mcp server: %v", err)
	}
	if _, err := runtime.Invoke(ctx, "agent.mcp.servers.uninstall", map[string]any{"id": "echo-server"}); err != nil {
		t.Fatalf("uninstall mcp server: %v", err)
	}
	list, err = runtime.Invoke(ctx, "agent.mcp.servers.list", nil)
	if err != nil {
		t.Fatalf("list mcp servers after uninstall: %v", err)
	}
	if servers := list["servers"].([]map[string]any); len(servers) != 0 {
		t.Fatalf("expected mcp server removed, got %#v", list)
	}
}

func TestStdioMCPInstallDiscoversTools(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "mcp-test.py")
	if err := os.WriteFile(script, []byte(`#!/usr/bin/env python3
import json, sys

def read_frame():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        line = line.decode().strip()
        if not line:
            break
        key, value = line.split(":", 1)
        headers[key.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    return json.loads(sys.stdin.buffer.read(length).decode())

def write_frame(payload):
    data = json.dumps(payload).encode()
    sys.stdout.buffer.write(f"Content-Length: {len(data)}\r\n\r\n".encode() + data)
    sys.stdout.buffer.flush()

while True:
    frame = read_frame()
    if frame is None:
        break
    method = frame.get("method")
    if method == "initialize":
        write_frame({"jsonrpc":"2.0","id":frame.get("id"),"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"test","version":"1"}}})
    elif method == "notifications/initialized":
        pass
    elif method == "tools/list":
        write_frame({"jsonrpc":"2.0","id":frame.get("id"),"result":{"tools":[{"name":"ping","description":"Ping","inputSchema":{"type":"object","properties":{}}}]}})
    elif method == "tools/call":
        write_frame({"jsonrpc":"2.0","id":frame.get("id"),"result":{"content":[{"type":"text","text":"pong"}]}})
`), 0o700); err != nil {
		t.Fatalf("write mcp script: %v", err)
	}

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	install, err := runtime.Invoke(context.Background(), "agent.mcp.servers.install", map[string]any{
		"id":             "stdio-test",
		"transport":      "stdio",
		"command":        script,
		"discover_tools": true,
	})
	if err != nil {
		t.Fatalf("install stdio mcp: %v", err)
	}
	serverRecord := install["server"].(map[string]any)
	if len(configList(serverRecord, "tools")) != 1 {
		t.Fatalf("expected stdio mcp tool discovery, got %#v", install)
	}
}
