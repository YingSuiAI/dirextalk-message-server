package nativeagent

import (
	"context"
	"testing"
)

func TestEnabledToolsExposeRuntimeInspectWithoutDangerousConfirmation(t *testing.T) {
	runtime := &Runtime{}

	tools := runtime.enabledTools(context.Background(), nil, nil)
	if !toolEnabled(tools, "native_agent_runtime_inspect") {
		t.Fatalf("native_agent_runtime_inspect was not enabled by default")
	}
	if toolEnabled(tools, "native_agent_skills_install") {
		t.Fatalf("native_agent_skills_install should require dangerous confirmation")
	}

	einoTools := runtime.enabledRuntimeEinoTools(nil, nil)
	if len(einoTools) != 0 {
		t.Fatalf("runtime Eino tools should require dangerous confirmation, got %d", len(einoTools))
	}
}

func toolEnabled(tools []Tool, name string) bool {
	for _, tool := range tools {
		if tool.Name == name {
			return true
		}
	}
	return false
}
