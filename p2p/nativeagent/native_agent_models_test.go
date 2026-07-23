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
		_, _ = w.Write([]byte(`{"data":[{"id":"provider/model-a","name":"Model A","context_length":131072,"api_key":"test-key","authorization":"Bearer test-key","token":"token-value","secret":"secret-value","metadata":{"api_key":"nested-key"}},{"id":"provider/model-b"}]}`))
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
	if _, ok := models[0]["temperature"]; ok {
		t.Fatalf("models.list must not invent temperature defaults: %#v", models[0])
	}
	if _, ok := models[0]["top_p"]; ok {
		t.Fatalf("models.list must not invent top_p defaults: %#v", models[0])
	}
	for _, key := range []string{"api_key", "authorization", "token", "secret", "metadata"} {
		if _, ok := models[0][key]; ok {
			t.Fatalf("models.list must not expose upstream %s: %#v", key, models[0])
		}
	}
	data, _ := json.Marshal(result)
	for _, secret := range []string{"test-key", "token-value", "secret-value", "nested-key"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("models response must not echo upstream credentials: %s", data)
		}
	}
}

func TestModelsListDoesNotInventOpenAIMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"gpt-5.5"}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "openai",
		"base_url": server.URL,
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	models, ok := result["models"].([]map[string]any)
	if !ok || len(models) != 1 {
		t.Fatalf("expected one model, got %#v", result["models"])
	}
	for _, key := range []string{"temperature", "top_p", "context_length", "max_output_tokens", "reasoning_modes", "reasoning_mode"} {
		if _, ok := models[0][key]; ok {
			t.Fatalf("models.list must not invent %s: %#v", key, models[0])
		}
	}
}

func TestModelsListFetchesAnthropicProvider(t *testing.T) {
	var gotPath, gotAPIKey, gotVersion string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAPIKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"claude-sonnet-4-5","display_name":"Claude Sonnet 4.5","max_input_tokens":200000,"max_tokens":64000}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "anthropic",
		"base_url": server.URL + "/v1",
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	if gotPath != "/v1/models" || gotAPIKey != "test-key" || gotVersion != anthropicVersion {
		t.Fatalf("unexpected Anthropic request path=%q api_key=%q version=%q", gotPath, gotAPIKey, gotVersion)
	}
	models := result["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "claude-sonnet-4-5" || models[0]["name"] != "Claude Sonnet 4.5" || models[0]["max_input_tokens"] != float64(200000) {
		t.Fatalf("unexpected Anthropic models: %#v", models)
	}
}

func TestModelsListFetchesGeminiNativeProvider(t *testing.T) {
	var gotPath, gotAPIKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAPIKey = r.Header.Get("x-goog-api-key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"models":[{"name":"models/gemini-3.6-flash","displayName":"Gemini 3.6 Flash"}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "gemini",
		"base_url": server.URL + "/v1beta",
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	if gotPath != "/v1beta/models" || gotAPIKey != "test-key" {
		t.Fatalf("unexpected Gemini request path=%q api_key=%q", gotPath, gotAPIKey)
	}
	models := result["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "gemini-3.6-flash" || models[0]["name"] != "Gemini 3.6 Flash" {
		t.Fatalf("unexpected Gemini models: %#v", models)
	}
}

func TestModelsListFetchesGeminiNativeProviderFromCustomHost(t *testing.T) {
	var gotPath, gotGoogleKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotGoogleKey = r.Header.Get("x-goog-api-key")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"models":[{"name":"models/gemini-custom","displayName":"Gemini Custom"}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "gemini",
		"base_url": server.URL,
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	if gotPath != "/v1beta/models" || gotGoogleKey != "test-key" {
		t.Fatalf("unexpected custom Gemini request path=%q google_key=%q", gotPath, gotGoogleKey)
	}
	models := result["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "gemini-custom" {
		t.Fatalf("unexpected custom Gemini models: %#v", models)
	}
}

func TestGeminiModelProfileDoesNotDefaultRequiredFields(t *testing.T) {
	profile := New(Config{}).resolveModelProfile(map[string]any{
		"model_profile": map[string]any{"provider": "gemini"},
	})
	if profile.Model != "" || profile.BaseURL != "" {
		t.Fatalf("Gemini profile must not receive server defaults: %#v", profile)
	}
}

func TestModelsListFetchesXAIProvider(t *testing.T) {
	var gotPath, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":"grok-4.5","context_length":256000,"owned_by":"xai"}]}`))
	}))
	defer server.Close()

	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	result, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "xai",
		"base_url": server.URL + "/v1",
		"api_key":  "test-key",
	})
	if err != nil {
		t.Fatalf("agent.models.list: %v", err)
	}
	if gotPath != "/v1/models" || gotAuth != "Bearer test-key" {
		t.Fatalf("unexpected xAI request path=%q authorization=%q", gotPath, gotAuth)
	}
	models := result["models"].([]map[string]any)
	if len(models) != 1 || models[0]["id"] != "grok-4.5" || models[0]["name"] != "grok-4.5" || models[0]["context_length"] != float64(256000) {
		t.Fatalf("unexpected xAI models: %#v", models)
	}
}

func TestXAIModelProfileDoesNotDefaultRequiredFields(t *testing.T) {
	profile := New(Config{}).resolveModelProfile(map[string]any{
		"model_profile": map[string]any{"provider": "xai"},
	})
	if profile.Model != "" || profile.BaseURL != "" {
		t.Fatalf("xAI profile must not receive server defaults: %#v", profile)
	}
}

func TestChatRequiresExplicitModelProfileFields(t *testing.T) {
	testCases := []struct {
		name    string
		profile map[string]any
		want    string
	}{
		{name: "provider", profile: map[string]any{"model": "model-a", "base_url": "https://models.example/v1", "api_key": "test-key"}, want: "model_profile.provider is required"},
		{name: "model", profile: map[string]any{"provider": "openai_compatible", "base_url": "https://models.example/v1", "api_key": "test-key"}, want: "model_profile.model is required; select a model"},
		{name: "base url", profile: map[string]any{"provider": "openai_compatible", "model": "model-a", "api_key": "test-key"}, want: "model_profile.base_url is required; configure the model API address"},
		{name: "api key", profile: map[string]any{"provider": "openai_compatible", "model": "model-a", "base_url": "https://models.example/v1"}, want: "model_profile.api_key is required"},
	}
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
				"prompt":        "hello",
				"model_profile": testCase.profile,
			})
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("agent.chat error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func TestRemovedModelProvidersAreRejected(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	for _, provider := range []string{"litellm", "vertex"} {
		t.Run(provider, func(t *testing.T) {
			profile := map[string]any{
				"provider": provider,
				"model":    "test-model",
				"base_url": "https://models.example/v1",
				"api_key":  "test-key",
			}
			_, err := runtime.Invoke(context.Background(), "agent.chat", map[string]any{
				"prompt":        "hello",
				"model_profile": profile,
			})
			if err == nil || err.Error() != "model_profile.provider is not supported" {
				t.Fatalf("agent.chat provider %q error = %v", provider, err)
			}
			_, err = runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
				"provider": provider,
				"base_url": profile["base_url"],
				"api_key":  profile["api_key"],
			})
			if err == nil || !strings.Contains(err.Error(), "model list is not supported") {
				t.Fatalf("agent.models.list provider %q error = %v", provider, err)
			}
		})
	}
}

func TestChatStreamRequiresExplicitModelAndBaseURL(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	var events []Event
	err := runtime.Stream(context.Background(), "agent.chat.stream", map[string]any{
		"prompt": "hello",
		"model_profile": map[string]any{
			"provider": "xai",
			"api_key":  "test-key",
		},
	}, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("agent.chat.stream: %v", err)
	}
	if len(events) != 2 || events[0].Event != "error" || events[0].Data["error"] != "model_profile.model is required; select a model" || events[1].Event != "done" || events[1].Data["ok"] != false {
		t.Fatalf("agent.chat.stream events = %#v", events)
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

func TestModelsListRequiresExplicitBaseURL(t *testing.T) {
	runtime := New(Config{DataDir: filepath.Join(t.TempDir(), "agent")})
	_, err := runtime.Invoke(context.Background(), "agent.models.list", map[string]any{
		"provider": "xai",
		"api_key":  "test-key",
	})
	if err == nil || !strings.Contains(err.Error(), "base_url is required") {
		t.Fatalf("expected base_url required error, got %v", err)
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
	for _, provider := range providers {
		if provider["provider"] == "litellm" || provider["provider"] == "vertex" {
			t.Fatalf("removed provider leaked through metadata: %#v", provider)
		}
	}
}
