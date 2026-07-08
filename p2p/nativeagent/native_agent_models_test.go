package nativeagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelsListFetchesOpenAICompatibleProvider(t *testing.T) {
	var gotPath string
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"provider/model-a","name":"Model A","context_length":131072},{"id":"provider/model-b"}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "openai_compatible",
		"base_url": server.URL,
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	if gotPath != "/v1/models" {
		t.Fatalf("expected /v1/models request, got %q", gotPath)
	}
	if gotAuth != "Bearer test-key" {
		t.Fatalf("expected bearer auth, got %q", gotAuth)
	}
	models, ok := result["models"].([]map[string]any)
	if !ok || len(models) != 2 {
		t.Fatalf("expected two models, got %#v", result["models"])
	}
	if models[0]["id"] != "provider/model-a" || models[0]["name"] != "Model A" || models[0]["context_length"] == nil {
		t.Fatalf("unexpected first model: %#v", models[0])
	}
	data, _ := json.Marshal(result)
	if strings.Contains(string(data), "test-key") {
		t.Fatalf("models response must not echo api key: %s", data)
	}
}

func TestModelsListRequiresAPIKeyForDynamicFetch(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	_, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "openai",
		"base_url": "https://api.openai.com/v1",
	})
	if err == nil || !strings.Contains(err.Error(), "api_key is required") {
		t.Fatalf("expected api_key required error, got %v", err)
	}
}

func TestModelsListWithoutProviderReturnsMetadataOnly(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{})
	if err != nil {
		t.Fatalf("agent.models.list metadata: %v", err)
	}
	models, ok := result["models"].([]map[string]any)
	if !ok || len(models) != 0 {
		t.Fatalf("expected empty models without provider, got %#v", result["models"])
	}
	providers, ok := result["providers"].([]map[string]any)
	if !ok || len(providers) == 0 {
		t.Fatalf("expected provider metadata, got %#v", result["providers"])
	}
}
