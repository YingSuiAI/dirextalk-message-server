package nativeagent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestRuntimeSubprocessEnvUsesAnIsolatedHomeAndExcludesAWSSettings(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "agent")
	t.Setenv("HOME", "not-the-native-agent-home")
	t.Setenv("USERPROFILE", "not-the-native-agent-profile")
	t.Setenv("AWS_ACCESS_KEY_ID", "not-a-real-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "not-a-real-secret")
	t.Setenv("AWS_SESSION_TOKEN", "not-a-real-session-token")

	env := runtimeEnv(dataDir)
	for _, prefix := range []string{"AWS_ACCESS_KEY_ID=", "AWS_SECRET_ACCESS_KEY=", "AWS_SESSION_TOKEN="} {
		if envHasPrefix(env, prefix) {
			t.Fatalf("runtime env must not inherit AWS setting %q: %#v", prefix, env)
		}
	}
	wantHome := filepath.Join(dataDir, "runtime", "home")
	if got := envValue(env, "HOME"); got != "" && got != wantHome {
		t.Fatalf("runtime HOME = %q, want isolated %q", got, wantHome)
	}
	if got := envValue(env, "USERPROFILE"); got != "" && got != wantHome {
		t.Fatalf("runtime USERPROFILE = %q, want isolated %q", got, wantHome)
	}
	if envValue(env, "HOME") == os.Getenv("HOME") && os.Getenv("HOME") != "" {
		t.Fatalf("runtime environment inherited host HOME: %#v", env)
	}
	if envValue(env, "USERPROFILE") == os.Getenv("USERPROFILE") && os.Getenv("USERPROFILE") != "" {
		t.Fatalf("runtime environment inherited host USERPROFILE: %#v", env)
	}
}

func TestRuntimeRefusesAWSCLIThroughDirectAndShellEntryPoints(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	if _, err := runtime.runtimeRun(context.Background(), map[string]any{"command": "aws"}); err == nil || !strings.Contains(err.Error(), "AWS CLI execution is not available") {
		t.Fatalf("runtime direct command must reject AWS CLI before execution, err=%v", err)
	}
	for _, command := range []string{
		"aws ec2 describe-instances",
		"sh -c 'aws ec2 describe-instances'",
		"env AWS_PROFILE=ignored aws ec2 describe-instances",
		"command aws ec2 describe-instances",
	} {
		if _, err := runtime.runShell(context.Background(), command, time.Second); err == nil || !strings.Contains(err.Error(), "AWS CLI execution is not available") {
			t.Fatalf("runtime shell must reject wrapped AWS CLI command %q before execution, err=%v", command, err)
		}
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

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, value := range env {
		if strings.HasPrefix(value, prefix) {
			return strings.TrimPrefix(value, prefix)
		}
	}
	return ""
}
