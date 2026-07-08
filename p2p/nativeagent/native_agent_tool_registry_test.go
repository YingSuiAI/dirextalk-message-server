package nativeagent

import (
	"context"
	"testing"
)

func TestDefaultEnabledToolsUseWriteMetadata(t *testing.T) {
	runtime := New(Config{Tools: []Tool{
		{Name: "custom_read", Description: "read", Handler: func(context.Context, map[string]any) (any, error) { return map[string]any{}, nil }},
		{Name: "custom_write", Description: "write", Write: true, Handler: func(context.Context, map[string]any) (any, error) { return map[string]any{}, nil }},
	}})

	tools := runtime.enabledTools(context.Background(), nil, nil)
	seen := map[string]bool{}
	for _, tool := range tools {
		seen[tool.Name] = true
	}
	if !seen["custom_read"] {
		t.Fatalf("expected read tool to be enabled by default, got %#v", tools)
	}
	if seen["custom_write"] {
		t.Fatalf("expected write tool metadata to keep tool disabled by default, got %#v", tools)
	}
}
