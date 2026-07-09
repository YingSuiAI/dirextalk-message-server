package nativeagent

import "testing"

func TestNativeAgentMaxStepsDefaultsToMultiToolWorkflowBudget(t *testing.T) {
	got := nativeAgentMaxSteps(nil, nil)
	want := 100
	if got != want {
		t.Fatalf("default max steps = %d, want %d", got, want)
	}
}

func TestNativeAgentMaxStepsAllowsLongConfiguredWorkflowsWithCap(t *testing.T) {
	got := nativeAgentMaxSteps(nil, map[string]any{"max_tool_calls": 80})
	want := 164
	if got != want {
		t.Fatalf("max steps from max_tool_calls = %d, want %d", got, want)
	}

	got = nativeAgentMaxSteps(nil, map[string]any{"max_steps": 999})
	if got != nativeAgentMaxStepLimit {
		t.Fatalf("clamped max steps = %d, want %d", got, nativeAgentMaxStepLimit)
	}
}
