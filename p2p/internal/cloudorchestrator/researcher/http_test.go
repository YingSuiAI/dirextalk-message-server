package researcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestHTTPPlannerPostsOnlyResearchInputToExactHTTPSPath(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/cloud-research" {
			t.Fatalf("research request = %s %s", request.Method, request.URL.Path)
		}
		var input map[string]any
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			t.Fatal(err)
		}
		if input["goal_id"] != "goal-1" || input["plan_id"] != "plan-1" || input["cloud_connection_id"] != "connection-1" || input["goal"] != "Private knowledge service" {
			t.Fatalf("research input = %#v", input)
		}
		for _, forbidden := range []string{"api_key", "aws_access_key_id", "authorization", "secret"} {
			if _, found := input[forbidden]; found {
				t.Fatalf("research input leaked %q: %#v", forbidden, input)
			}
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"title":"Private knowledge service","summary":"Official source review pending owner confirmation.","plan":{"plan_id":"plan-1"},"recipe":{"recipe_id":"recipe-1"},"quote":{"quote_id":"quote-1"}}`))
	}))
	defer server.Close()

	planner, err := NewHTTP(HTTPConfig{Endpoint: server.URL + "/v1/cloud-research", Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	output, err := planner.Research(context.Background(), runtime.ResearchInput{
		GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Private knowledge service",
	})
	if err != nil || output.Title != "Private knowledge service" || output.Plan.PlanID != "plan-1" {
		t.Fatalf("research output=%#v err=%v", output, err)
	}
}

func TestHTTPPlannerRejectsUnpinnedEndpointAndClassifiesTemporaryFailure(t *testing.T) {
	if _, err := NewHTTP(HTTPConfig{Endpoint: "http://researcher.example/v1/cloud-research"}); err == nil {
		t.Fatal("non-HTTPS researcher endpoint must be rejected")
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	planner, err := NewHTTP(HTTPConfig{Endpoint: server.URL + "/v1/cloud-research", Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Research(context.Background(), runtime.ResearchInput{GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Private knowledge service"})
	if err == nil || !strings.HasPrefix(err.Error(), "researcher_unavailable:") {
		t.Fatalf("temporary researcher error = %v", err)
	}
}

func TestHTTPPlannerRejectsInvalidInputBeforePrivateTransport(t *testing.T) {
	calls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls++
	}))
	defer server.Close()
	planner, err := NewHTTP(HTTPConfig{Endpoint: server.URL + "/v1/cloud-research", Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = planner.Research(context.Background(), runtime.ResearchInput{
		GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1,
		Prompt: "aws_secret_access_key=redacted",
	})
	if err == nil || calls != 0 {
		t.Fatalf("invalid research input was sent=%t err=%v", calls != 0, err)
	}
}
