package nativeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestHTTPMCPToolCanBeInstalledAndUsedByModelLoop(t *testing.T) {
	mcpServer := newHTTPTestMCPServer("mcp-echo")
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
		"prompt":                  "call mcp",
		"enabled_tools":           []any{"all"},
		"dangerous_tools_confirm": "allow_native_agent_dangerous_tools",
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

func TestModelLoopCanInstallMCPServerFromDialogue(t *testing.T) {
	mcpServer := newHTTPTestMCPServer("mcp-installed-by-dialogue")
	defer mcpServer.Close()

	var requestCount int
	var sawInstallResult bool
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode model request: %v", err)
		}
		messages, _ := payload["messages"].([]any)
		for _, raw := range messages {
			message, _ := raw.(map[string]any)
			if message["role"] == "tool" && strings.Contains(trimString(message["content"]), "dialogue-mcp") {
				sawInstallResult = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if requestCount == 1 {
			arguments, err := json.Marshal(map[string]any{
				"id":             "dialogue-mcp",
				"transport":      "http",
				"url":            mcpServer.URL,
				"discover_tools": true,
			})
			if err != nil {
				t.Fatalf("marshal tool arguments: %v", err)
			}
			response, err := json.Marshal(map[string]any{
				"choices": []map[string]any{{
					"message": map[string]any{
						"role":    "assistant",
						"content": "",
						"tool_calls": []map[string]any{{
							"id":   "call_mcp_install",
							"type": "function",
							"function": map[string]any{
								"name":      "native_agent_mcp_servers_install",
								"arguments": string(arguments),
							},
						}},
					},
				}},
			})
			if err != nil {
				t.Fatalf("marshal model response: %v", err)
			}
			_, _ = w.Write(response)
			return
		}
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"MCP 已安装，下一轮对话可使用。"}}]}`))
	}))
	defer modelServer.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	result, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
		"prompt":                  "安装一个 MCP server",
		"dangerous_tools_confirm": "allow_native_agent_dangerous_tools",
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-model",
			"base_url": modelServer.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("agent chat failed: %v", err)
	}
	if requestCount != 2 || !sawInstallResult || result["text"] != "MCP 已安装，下一轮对话可使用。" {
		t.Fatalf("expected dialogue MCP install loop, requestCount=%d sawInstallResult=%v result=%#v", requestCount, sawInstallResult, result)
	}
	steps, ok := result["steps"].([]map[string]any)
	if !ok || !traceHasStep(steps, "tool_call", "native_agent_mcp_servers_install") {
		t.Fatalf("expected MCP install trace step, got %#v", result["steps"])
	}
	list, err := runtime.mcpServersList(context.Background())
	if err != nil {
		t.Fatalf("list MCP servers: %v", err)
	}
	servers := list["servers"].([]map[string]any)
	if len(servers) != 1 || servers[0]["id"] != "dialogue-mcp" || len(configList(servers[0], "tools")) != 1 {
		t.Fatalf("expected dialogue MCP server installed with discovered tools, got %#v", list)
	}
}

func TestStdioMCPInstallDiscoversTools(t *testing.T) {
	dir := t.TempDir()
	binary := buildStdioTestMCPServer(t, dir)

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	install, err := runtime.Invoke(context.Background(), "agent.mcp.servers.install", map[string]any{
		"id":             "stdio-test",
		"transport":      "stdio",
		"command":        binary,
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

func newHTTPTestMCPServer(response string) *httptest.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "echo-server", Version: "v0.0.1"}, nil)
	type echoArgs struct {
		Text string `json:"text" jsonschema:"text to echo"`
	}
	mcp.AddTool(server, &mcp.Tool{Name: "echo", Description: "Echo text."}, func(ctx context.Context, req *mcp.CallToolRequest, args echoArgs) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: response}}}, nil, nil
	})
	return httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return server
	}, nil))
}

func buildStdioTestMCPServer(t *testing.T, dir string) string {
	t.Helper()
	source := filepath.Join(dir, "main.go")
	if err := os.WriteFile(source, []byte(`package main

import (
	"context"
	"log"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	server := mcp.NewServer(&mcp.Implementation{Name: "stdio-test", Version: "v0.0.1"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "ping", Description: "Ping"}, func(ctx context.Context, req *mcp.CallToolRequest, args struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "pong"}}}, nil, nil
	})
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		log.Fatal(err)
	}
}
`), 0o600); err != nil {
		t.Fatalf("write stdio mcp source: %v", err)
	}
	binary := filepath.Join(dir, "mcp-test")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	cmd := exec.Command("go", "build", "-o", binary, source)
	cmd.Dir = filepath.Join("..", "..")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build stdio mcp server: %v\n%s", err, string(output))
	}
	return binary
}
