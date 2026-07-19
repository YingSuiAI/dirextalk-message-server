package researcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
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
		messages, ok := payload["messages"].([]any)
		if !ok || len(messages) < 2 {
			t.Fatalf("model messages = %#v", payload["messages"])
		}
		systemMessage, ok := messages[0].(map[string]any)
		if !ok {
			t.Fatalf("model system message = %#v", messages[0])
		}
		system, ok := systemMessage["content"].(string)
		if !ok {
			t.Fatalf("model system prompt = %#v", messages[0])
		}
		for _, required := range []string{"ResearchDraftV1", "PlanV1", "QuoteV1", "price", "approval", "hash"} {
			if !strings.Contains(system, required) {
				t.Fatalf("model system prompt must constrain %q: %q", required, system)
			}
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
	if err != nil || output.Draft.Region != want.Draft.Region || output.Recipe.RecipeID != want.Recipe.RecipeID {
		t.Fatalf("model research output valid=%t err=%v", output.Draft.Region == want.Draft.Region, err)
	}
}

func TestOpenAICompatiblePlannerSelectedRecipeRejectsModelRecipeFieldAndInjectsTrustedRecipe(t *testing.T) {
	now := time.Now().UTC()
	baseInput := runtime.ResearchInput{GoalID: "goal-selected-1", PlanID: "plan-selected-1", ConnectionID: "connection-selected-1", PlanRevision: 1, Prompt: "Deploy selected recipe."}
	want := validResearchOutput(t, now, baseInput)
	digest, err := want.Recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	baseInput.SelectedRecipe = &runtime.SelectedRecipeInput{RecipeID: want.Recipe.RecipeID, Revision: 2, Digest: digest, Recipe: want.Recipe}
	want.Draft.Candidates = []cloudcontracts.QuoteRequestCandidateV1{
		{CandidateID: "economy", Tier: cloudcontracts.QuoteTierEconomy, InstanceType: "m7i.large", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80},
		{CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80},
		{CandidateID: "performance", Tier: cloudcontracts.QuoteTierPerformance, InstanceType: "m7i.2xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80},
	}
	for _, tc := range []struct {
		name          string
		includeRecipe bool
		wantErr       bool
	}{{"trusted injection", false, false}, {"model recipe rejected", true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var requestPayload map[string]any
				_ = json.NewDecoder(r.Body).Decode(&requestPayload)
				if !strings.Contains(mustJSON(t, requestPayload), `selected_recipe`) {
					t.Fatal("full selected recipe missing from model input")
				}
				model := map[string]any{"draft": want.Draft, "title": want.Title, "summary": want.Summary}
				if tc.includeRecipe {
					model["recipe"] = want.Recipe
				}
				content, _ := json.Marshal(model)
				encoded, _ := json.Marshal(string(content))
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":` + string(encoded) + `}}]}`))
			}))
			defer server.Close()
			planner, e := NewOpenAICompatiblePlanner(OpenAICompatibleConfig{Endpoint: server.URL + "/v1/chat/completions", Model: "research-model", APIKey: "private", Client: server.Client()})
			if e != nil {
				t.Fatal(e)
			}
			output, e := planner.Research(t.Context(), baseInput)
			if tc.wantErr {
				if e == nil {
					t.Fatal("model recipe field accepted")
				}
				return
			}
			if e != nil || output.Recipe.RecipeID != want.Recipe.RecipeID {
				t.Fatalf("output=%#v err=%v", output, e)
			}
		})
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
