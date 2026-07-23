package nativeagent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cloudwego/eino/components/model"
)

func TestDirectModelHTTPFailuresRemainProviderIndependent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("  provider busy  "))
	}))
	defer server.Close()

	for _, testCase := range directModelTestCases(server.URL) {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := testCase.model.Generate(context.Background(), nil); err == nil || err.Error() != "model provider returned 429: provider busy" {
				t.Fatalf("Generate error = %v", err)
			}
			if _, err := testCase.model.Stream(context.Background(), nil); err == nil || err.Error() != "model provider returned 429: provider busy" {
				t.Fatalf("Stream error = %v", err)
			}
		})
	}
}

func TestDirectModelStreamsDecodeProviderEvents(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(": keepalive\n"))
		_, _ = w.Write([]byte("data: not-json\n"))
		if r.URL.Path == "/v1/messages" {
			_, _ = w.Write([]byte("data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"anthropic\"}}\n"))
		} else {
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"openai\"}}]}\n"))
		}
		_, _ = w.Write([]byte("data: [DONE]\n"))
	}))
	defer server.Close()

	for _, testCase := range directModelTestCases(server.URL) {
		t.Run(testCase.name, func(t *testing.T) {
			stream, err := testCase.model.Stream(context.Background(), nil)
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			defer stream.Close()
			message, err := stream.Recv()
			if err != nil {
				t.Fatalf("Recv: %v", err)
			}
			if message == nil || message.Content != testCase.wantText {
				t.Fatalf("message = %#v, want content %q", message, testCase.wantText)
			}
			if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("final Recv error = %v, want EOF", err)
			}
		})
	}
}

func TestDirectModelPayloadsOmitUnsetOptionalTuning(t *testing.T) {
	runtime := New(Config{})
	profile := runtime.resolveModelProfile(map[string]any{
		"model_profile": map[string]any{
			"provider":          "openai_compatible",
			"model":             "test-model",
			"base_url":          "https://models.example",
			"api_key":           "test-key",
			"temperature":       "",
			"top_p":             nil,
			"max_output_tokens": "",
			"context_window":    "",
		},
	})
	if profile.Temperature != nil || profile.TopP != nil || profile.MaxOutputTokens != 0 || profile.ContextWindow != 0 {
		t.Fatalf("empty tuning values must remain unset: %#v", profile)
	}

	openAI := newOpenAICompatibleDirectChatModel(runtime, profile).(*openAICompatibleDirectChatModel)
	payload, err := openAI.requestPayload(nil, false)
	if err != nil {
		t.Fatalf("OpenAI-compatible request payload: %v", err)
	}
	for _, key := range []string{"temperature", "top_p", "max_tokens", "context_length", "context_window"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("unset %s must be omitted from OpenAI-compatible request: %#v", key, payload)
		}
	}

	anthropic := newAnthropicDirectChatModel(runtime, nativeModelProfile{
		Provider: "anthropic",
		Model:    "test-model",
		BaseURL:  "https://api.anthropic.com",
		APIKey:   "test-key",
	}).(*anthropicDirectChatModel)
	payload, err = anthropic.requestPayload(nil, false)
	if err != nil {
		t.Fatalf("Anthropic request payload: %v", err)
	}
	for _, key := range []string{"temperature", "top_p", "context_length", "context_window"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("unset %s must be omitted from Anthropic request: %#v", key, payload)
		}
	}
	if payload["max_tokens"] != anthropicRequiredMaxTokens {
		t.Fatalf("Anthropic requires a positive max_tokens value: %#v", payload)
	}
}

func directModelTestCases(baseURL string) []struct {
	name     string
	model    model.ToolCallingChatModel
	wantText string
} {
	runtime := New(Config{})
	return []struct {
		name     string
		model    model.ToolCallingChatModel
		wantText string
	}{
		{
			name: "openai compatible",
			model: newOpenAICompatibleDirectChatModel(runtime, nativeModelProfile{
				Provider: "openai_compatible",
				Model:    "test-model",
				BaseURL:  baseURL,
				APIKey:   "test-key",
			}),
			wantText: "openai",
		},
		{
			name: "anthropic",
			model: newAnthropicDirectChatModel(runtime, nativeModelProfile{
				Provider: "anthropic",
				Model:    "test-model",
				BaseURL:  baseURL,
				APIKey:   "test-key",
			}),
			wantText: "anthropic",
		},
	}
}
