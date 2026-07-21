package nativeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudwego/eino/schema"
)

func TestRememberEinoMessagesReportsPersistenceFailure(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime := New(Config{DataDir: blocked})
	run := nativeAgentRunContext{conversationID: "conversation", memory: nativeAgentMemory{ConversationID: "conversation"}}
	err := runtime.rememberEinoMessages(context.Background(), map[string]any{}, map[string]any{}, nativeModelProfile{}, run, nil)
	if err == nil {
		t.Fatal("memory persistence failure was silently ignored")
	}
}

func TestContextCompressSummarizesOlderMemoryTurns(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	ctx := context.Background()
	memory := nativeAgentMemory{ConversationID: "compress-test"}
	for i := 0; i < 8; i++ {
		memory.Messages = append(memory.Messages, schema.UserMessage("用户轮次"), schema.AssistantMessage("助手轮次", nil))
	}
	if err := runtime.saveMemory(ctx, memory); err != nil {
		t.Fatalf("save memory: %v", err)
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
	messages, ok := result["messages"].([]*schema.Message)
	if !ok || len(messages) != 2 {
		t.Fatalf("expected only recent Eino messages after compression, got %#v", result["messages"])
	}
}

func TestContextCompressCanUseEinoModelSummary(t *testing.T) {
	var sawCompressionPrompt bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode compression payload: %v", err)
		}
		if strings.Contains(jsonValue(payload["messages"]), "compress Dirextalk Agent conversation memory") {
			sawCompressionPrompt = true
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"模型压缩摘要"}}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	ctx := context.Background()
	memory := nativeAgentMemory{ConversationID: "model-compress"}
	for i := 0; i < 4; i++ {
		memory.Messages = append(memory.Messages, schema.UserMessage("用户偏好中文"), schema.AssistantMessage("已记录偏好", nil))
	}
	if err := runtime.saveMemory(ctx, memory); err != nil {
		t.Fatalf("save memory: %v", err)
	}
	result, err := runtime.Invoke(ctx, "agent.context.compress", map[string]any{
		"conversation_id": "model-compress",
		"memory_window":   2,
		"model_profile": map[string]any{
			"provider": "openai_compatible",
			"model":    "mock-compress",
			"base_url": server.URL,
			"api_key":  "test-key",
		},
	})
	if err != nil {
		t.Fatalf("model compress memory: %v", err)
	}
	if !sawCompressionPrompt || result["summary"] != "模型压缩摘要" || result["compression"] != "eino_model" {
		t.Fatalf("expected Eino model compression, saw=%v result=%#v", sawCompressionPrompt, result)
	}
}
