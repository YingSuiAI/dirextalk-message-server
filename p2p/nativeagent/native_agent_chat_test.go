package nativeagent

import (
	"context"
	"strings"
	"testing"
)

func TestAgentSystemPromptPrependsNativeProductRules(t *testing.T) {
	runtime := New(Config{})
	prompt := runtime.agentSystemPrompt(
		context.Background(),
		map[string]any{"system_prompt": "User configured system prompt."},
		map[string]any{"system_prompt": "Request scoped system prompt."},
		"Extra prompt block.",
	)

	for _, marker := range []string{
		"Dirextalk Native Agent",
		"native_agent_skills_*",
		"npx skills add <repo> --skill <name>",
		"User configured system prompt.",
		"Request scoped system prompt.",
		"Extra prompt block.",
	} {
		if !strings.Contains(prompt, marker) {
			t.Fatalf("expected system prompt to contain %q, got %q", marker, prompt)
		}
	}
	if strings.Index(prompt, "Dirextalk Native Agent") > strings.Index(prompt, "User configured system prompt.") {
		t.Fatalf("native product rules must come before user system prompt, got %q", prompt)
	}
}
