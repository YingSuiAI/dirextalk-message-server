package nativeagent

import (
	"context"
	"path/filepath"
	"testing"
)

func TestManagedConfigRecordErrorsAndMissingUninstallRemainStable(t *testing.T) {
	runtime := New(Config{
		DataDir: filepath.Join(t.TempDir(), "agent"),
		Store:   &testConfigStore{config: map[string]any{}},
	})
	ctx := context.Background()

	for _, testCase := range []struct {
		name    string
		action  string
		params  map[string]any
		wantErr string
	}{
		{name: "skill id required", action: "agent.skills.enable", wantErr: "skill id is required"},
		{name: "skill missing", action: "agent.skills.enable", params: map[string]any{"id": "missing"}, wantErr: `skill "missing" is not installed`},
		{name: "mcp id required", action: "agent.mcp.servers.enable", wantErr: "mcp server id is required"},
		{name: "mcp missing", action: "agent.mcp.servers.enable", params: map[string]any{"id": "missing"}, wantErr: `mcp server "missing" is not installed`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := runtime.Invoke(ctx, testCase.action, testCase.params)
			if err == nil || err.Error() != testCase.wantErr {
				t.Fatalf("%s error = %v, want %q", testCase.action, err, testCase.wantErr)
			}
		})
	}

	for _, action := range []string{"agent.skills.uninstall", "agent.mcp.servers.uninstall"} {
		result, err := runtime.Invoke(ctx, action, map[string]any{"id": "missing"})
		if err != nil {
			t.Fatalf("%s missing record: %v", action, err)
		}
		if result["ok"] != false || result["id"] != "missing" {
			t.Fatalf("%s missing result = %#v", action, result)
		}
	}
}
