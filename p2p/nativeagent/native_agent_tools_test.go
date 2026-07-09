package nativeagent

import (
	"context"
	"testing"
)

func TestEnabledToolsExposeManagementAndRuntimeToolsWithoutConfirmation(t *testing.T) {
	runtime := &Runtime{}

	tools := runtime.enabledTools(context.Background(), nil, nil)
	if !toolEnabled(tools, "native_agent_runtime_inspect") {
		t.Fatalf("native_agent_runtime_inspect was not enabled by default")
	}
	if !toolEnabled(tools, "native_agent_skills_install") {
		t.Fatalf("native_agent_skills_install should be enabled by default")
	}

	einoTools := runtime.enabledRuntimeEinoTools(nil, nil)
	if len(einoTools) == 0 {
		t.Fatalf("runtime Eino tools should be exposed without confirmation")
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
