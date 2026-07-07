package nativeagent

import (
	"context"
	"path/filepath"
	"testing"
)

func TestContextCompressSummarizesOlderMemoryTurns(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	ctx := context.Background()
	for i := 0; i < 8; i++ {
		runtime.rememberTurn(ctx, map[string]any{}, map[string]any{
			"conversation_id": "compress-test",
			"prompt":          "用户轮次",
			"memory_window":   2,
		}, "助手轮次")
	}

	result, err := runtime.Invoke(ctx, "agent.context.compress", map[string]any{
		"conversation_id": "compress-test",
		"memory_window":   2,
	})
	if err != nil {
		t.Fatalf("compress memory: %v", err)
	}
	if trimString(result["summary"]) == "" {
		t.Fatalf("expected compressed summary, got %#v", result)
	}
	turns, ok := result["turns"].([]nativeAgentMemoryTurn)
	if !ok || len(turns) != 2 {
		t.Fatalf("expected only recent turns after compression, got %#v", result["turns"])
	}
}
