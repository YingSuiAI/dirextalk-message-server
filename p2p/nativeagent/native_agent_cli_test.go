package nativeagent

import (
	"context"
	"os"
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

func TestRuntimeInstallCommandFallsBackToShWhenBashMissing(t *testing.T) {
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o700); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	shPath := filepath.Join(binDir, "sh")
	if err := os.WriteFile(shPath, []byte("#!/bin/sh\nexec /bin/sh \"$@\"\n"), 0o700); err != nil {
		t.Fatalf("write fake sh: %v", err)
	}
	chmodPath := filepath.Join(binDir, "chmod")
	if err := os.WriteFile(chmodPath, []byte("#!/bin/sh\nexec /bin/chmod \"$@\"\n"), 0o700); err != nil {
		t.Fatalf("write fake chmod: %v", err)
	}
	t.Setenv("PATH", binDir)

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	result, err := runtime.Invoke(context.Background(), "agent.runtime.install", map[string]any{
		"id":              "sh-only",
		"install_command": "printf '#!/bin/sh\\necho sh-only\\n' > bin/sh-only && chmod +x bin/sh-only",
	})
	if err != nil {
		t.Fatalf("runtime install with sh fallback: %v", err)
	}
	if result["ok"] != true {
		t.Fatalf("expected install ok, got %#v", result)
	}
	run, err := runtime.Invoke(context.Background(), "agent.runtime.run", map[string]any{"command": "sh-only"})
	if err != nil {
		t.Fatalf("runtime run sh-only: %v", err)
	}
	if run["ok"] != true || strings.TrimSpace(trimString(run["stdout"])) != "sh-only" {
		t.Fatalf("expected sh-only output, got %#v", run)
	}
}

func TestRuntimeInstallCommandFailureReturnsError(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent"), Store: &testConfigStore{config: map[string]any{}}})
	_, err := runtime.Invoke(context.Background(), "agent.runtime.install", map[string]any{
		"id":              "bad-install",
		"install_command": "printf failed >&2; exit 9",
	})
	if err == nil || !strings.Contains(err.Error(), "runtime install command failed") {
		t.Fatalf("expected install command failure, got %v", err)
	}
}
