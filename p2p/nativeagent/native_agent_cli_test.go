package nativeagent

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeCLIInstallWhichAndRun(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	ctx := context.Background()
	_, err := runtime.Invoke(ctx, "agent.runtime.install", map[string]any{
		"id":       "hello-agent",
		"filename": "hello-agent",
		"content":  "#!/bin/sh\necho native-cli\n",
	})
	if err != nil {
		t.Fatalf("install runtime tool: %v", err)
	}
	which, err := runtime.Invoke(ctx, "agent.runtime.which", map[string]any{"command": "hello-agent"})
	if err != nil {
		t.Fatalf("runtime which: %v", err)
	}
	if which["ok"] != true || !strings.HasSuffix(trimString(which["path"]), "hello-agent") {
		t.Fatalf("expected installed tool path, got %#v", which)
	}
	run, err := runtime.Invoke(ctx, "agent.runtime.run", map[string]any{"command": "hello-agent"})
	if err != nil {
		t.Fatalf("runtime run: %v", err)
	}
	if run["ok"] != true || strings.TrimSpace(trimString(run["stdout"])) != "native-cli" {
		t.Fatalf("expected runtime tool output, got %#v", run)
	}
	_, err = runtime.Invoke(ctx, "agent.runtime.install", map[string]any{
		"id":              "from-install",
		"install_command": "printf '#!/bin/sh\\necho install-command\\n' > bin/from-install && chmod +x bin/from-install",
	})
	if err != nil {
		t.Fatalf("runtime install command: %v", err)
	}
	run, err = runtime.Invoke(ctx, "agent.runtime.run", map[string]any{"command": "from-install"})
	if err != nil {
		t.Fatalf("runtime run installed command: %v", err)
	}
	if run["ok"] != true || strings.TrimSpace(trimString(run["stdout"])) != "install-command" {
		t.Fatalf("expected install command output, got %#v", run)
	}
}
