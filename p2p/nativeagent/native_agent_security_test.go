package nativeagent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestEinoToolsExposeWriteRuntimeAndManagementToolsWithoutConfirmation(t *testing.T) {
	runtime := New(Config{
		DataDir: filepath.Join(t.TempDir(), "agent"),
		Store: &testConfigStore{config: map[string]any{
			"runtime_tools": []any{
				map[string]any{"id": "deploy", "command": "deploy"},
			},
		}},
		Tools: []Tool{
			{Name: "safe_read", Description: "read", Handler: func(context.Context, map[string]any) (any, error) { return map[string]any{}, nil }},
			{Name: "danger_write", Description: "write", Write: true, Handler: func(context.Context, map[string]any) (any, error) { return map[string]any{}, nil }},
		},
	})

	tools, cleanup, err := runtime.enabledEinoTools(context.Background(), map[string]any{
		"runtime_tools": []any{
			map[string]any{"id": "deploy", "command": "deploy"},
		},
	}, map[string]any{"enabled_tools": []any{"all"}})
	if err != nil {
		t.Fatalf("enabled tools: %v", err)
	}
	defer cleanup()
	names := einoToolNames(t, tools)
	if !names["safe_read"] {
		t.Fatalf("expected safe read tool, got %#v", names)
	}
	for _, name := range []string{
		"danger_write",
		"runtime__shell",
		"runtime__deploy",
		"native_agent_skills_install",
		"native_agent_mcp_servers_install",
	} {
		if !names[name] {
			t.Fatalf("tool %s should be available without request confirmation, got %#v", name, names)
		}
	}
}

func TestRuntimeSubprocessEnvDoesNotInheritServiceSecrets(t *testing.T) {
	t.Setenv("DIREXTALK_AGENT_TOKEN", "agent-secret")
	t.Setenv("P2P_PORTAL_PASSWORD", "portal-secret")

	env := runtimeEnv(filepath.Join(t.TempDir(), "agent"))
	if envHasPrefix(env, "DIREXTALK_AGENT_TOKEN=") || envHasPrefix(env, "P2P_PORTAL_PASSWORD=") {
		t.Fatalf("runtime env must not inherit service secrets, got %#v", env)
	}
}

func TestStdioMCPTransportDoesNotInheritServiceSecrets(t *testing.T) {
	t.Setenv("DIREXTALK_AGENT_TOKEN", "agent-secret")
	t.Setenv("P2P_PORTAL_PASSWORD", "portal-secret")

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	transport, err := runtime.mcpTransport(map[string]any{
		"id":        "stdio",
		"transport": "stdio",
		"command":   "test-command",
		"env":       map[string]any{"EXPLICIT_KEY": "explicit-value"},
	})
	if err != nil {
		t.Fatalf("stdio transport: %v", err)
	}
	commandTransport, ok := transport.(*mcp.CommandTransport)
	if !ok {
		t.Fatalf("expected command transport, got %T", transport)
	}
	env := commandTransport.Command.Env
	if envHasPrefix(env, "DIREXTALK_AGENT_TOKEN=") || envHasPrefix(env, "P2P_PORTAL_PASSWORD=") {
		t.Fatalf("stdio MCP env must not inherit service secrets, got %#v", env)
	}
	if !envHasPrefix(env, "EXPLICIT_KEY=explicit-value") {
		t.Fatalf("stdio MCP env must keep explicit server env, got %#v", env)
	}
}

func TestFetchTextRejectsLocalOrPrivateURLs(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	for _, rawURL := range []string{
		"http://127.0.0.1/skill.md",
		"https://localhost/skill.md",
		"https://10.0.0.1/skill.md",
		"https://169.254.169.254/latest/meta-data",
	} {
		if _, err := runtime.fetchText(context.Background(), rawURL); err == nil {
			t.Fatalf("expected %s to be rejected before fetch", rawURL)
		}
	}
}

func einoToolNames(t *testing.T, tools []einotool.BaseTool) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for _, tool := range tools {
		info, err := tool.Info(context.Background())
		if err != nil {
			t.Fatalf("tool info: %v", err)
		}
		names[info.Name] = true
	}
	return names
}

func envHasPrefix(env []string, prefix string) bool {
	for _, value := range env {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
