package researcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestOpenAICompatiblePlannerUsesSecretOnlyForAuthorization(t *testing.T) {
	input := runtime.ResearchInput{GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Deploy a private knowledge workload."}
	want := validResearchOutput(t, time.Now().UTC(), input)
	const apiKey = "test-private-model-token"
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" || request.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Fatalf("model request method=%s path=%s authorization present=%t", request.Method, request.URL.Path, request.Header.Get("Authorization") != "")
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["model"] != "research-model" || payload["api_key"] != nil || strings.Contains(mustJSON(t, payload), apiKey) {
			t.Fatalf("model payload must not contain the API key")
		}
		content, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		encodedContent, err := json.Marshal(string(content))
		if err != nil {
			t.Fatal(err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"choices":[{"message":{"content":` + string(encodedContent) + `}}]}`))
	}))
	defer server.Close()
	planner, err := NewOpenAICompatiblePlanner(OpenAICompatibleConfig{Endpoint: server.URL + "/v1/chat/completions", Model: "research-model", APIKey: apiKey, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	output, err := planner.Research(context.Background(), input)
	if err != nil || output.Plan.PlanID != input.PlanID || output.Quote.QuoteID != want.Quote.QuoteID {
		t.Fatalf("model research output valid=%t err=%v", output.Plan.PlanID == input.PlanID, err)
	}
}

func TestOpenAICompatiblePlannerClassifiesTemporaryFailureWithoutProviderBody(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte("provider diagnostic must not be persisted"))
	}))
	defer server.Close()
	planner, err := NewOpenAICompatiblePlanner(OpenAICompatibleConfig{Endpoint: server.URL + "/v1/chat/completions", Model: "research-model", APIKey: "test-private-model-token", Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Research(context.Background(), runtime.ResearchInput{GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Deploy a private knowledge workload."})
	if err == nil || !strings.HasPrefix(err.Error(), "model_unavailable:") || strings.Contains(err.Error(), "diagnostic") {
		t.Fatalf("temporary model error = %v", err)
	}
}

func TestOpenAICompatiblePlannerDefaultClientDoesNotUseEnvironmentProxy(t *testing.T) {
	planner, err := NewOpenAICompatiblePlanner(OpenAICompatibleConfig{
		Endpoint: "https://model.example/v1/chat/completions",
		Model:    "research-model",
		APIKey:   "test-private-model-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := planner.client.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatal("default model client must not route a mounted API key through environment proxy settings")
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
