package researcher

import (
	"bytes"
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

func TestResearchHTTPHandlerValidatesInputAndReturnsOnlyTypedOutput(t *testing.T) {
	input := runtime.ResearchInput{GoalID: "goal-1", PlanID: "plan-1", ConnectionID: "connection-1", PlanRevision: 1, Prompt: "Deploy a private knowledge workload."}
	planner := &recordingResearchPlanner{output: validResearchOutput(t, time.Now().UTC(), input)}
	handler := NewResearchHTTPHandler(planner)
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, cloudResearchPath, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || planner.calls != 1 {
		t.Fatalf("research handler status=%d calls=%d body=%s", recorder.Code, planner.calls, recorder.Body.String())
	}
	var output runtime.ResearchOutput
	if err := json.NewDecoder(recorder.Body).Decode(&output); err != nil || output.Draft.Region == "" || output.Title == "" {
		t.Fatalf("research output=%#v err=%v", output, err)
	}
	legacyPathRequest := httptest.NewRequest(http.MethodPost, "/v1/cloud-research", bytes.NewReader(body))
	legacyPathRequest.Header.Set("Content-Type", "application/json")
	legacyPathRecorder := httptest.NewRecorder()
	handler.ServeHTTP(legacyPathRecorder, legacyPathRequest)
	if legacyPathRecorder.Code != http.StatusNotFound || planner.calls != 1 {
		t.Fatalf("legacy researcher path status=%d calls=%d", legacyPathRecorder.Code, planner.calls)
	}

	secretRequest := httptest.NewRequest(http.MethodPost, cloudResearchPath, bytes.NewBufferString(`{"goal_id":"goal-1","plan_id":"plan-1","cloud_connection_id":"connection-1","plan_revision":1,"goal":"api_key=sk-0123456789abcdefghijklmnop"}`))
	secretRequest.Header.Set("Content-Type", "application/json")
	secretRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secretRecorder, secretRequest)
	if secretRecorder.Code != http.StatusBadRequest || planner.calls != 1 || bytes.Contains(secretRecorder.Body.Bytes(), []byte("sk-")) {
		t.Fatalf("secret request status=%d calls=%d body=%s", secretRecorder.Code, planner.calls, secretRecorder.Body.String())
	}

	overlargeBody := append(append([]byte{}, body...), []byte(strings.Repeat(" ", maxResearchRequest+1))...)
	overlargeRequest := httptest.NewRequest(http.MethodPost, cloudResearchPath, bytes.NewReader(overlargeBody))
	overlargeRequest.Header.Set("Content-Type", "application/json")
	overlargeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(overlargeRecorder, overlargeRequest)
	if overlargeRecorder.Code != http.StatusBadRequest || planner.calls != 1 {
		t.Fatalf("overlarge request status=%d calls=%d body=%s", overlargeRecorder.Code, planner.calls, overlargeRecorder.Body.String())
	}

	healthRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRecorder := httptest.NewRecorder()
	handler.ServeHTTP(healthRecorder, healthRequest)
	if healthRecorder.Code != http.StatusNoContent || planner.calls != 1 {
		t.Fatalf("researcher health status=%d calls=%d", healthRecorder.Code, planner.calls)
	}
}

type recordingResearchPlanner struct {
	output runtime.ResearchOutput
	calls  int
}

func (p *recordingResearchPlanner) Research(_ context.Context, _ runtime.ResearchInput) (runtime.ResearchOutput, error) {
	p.calls++
	return p.output, nil
}

func validResearchOutput(t *testing.T, now time.Time, input runtime.ResearchInput) runtime.ResearchOutput {
	t.Helper()
	recipe := cloudcontracts.RecipeV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1,
		RecipeID:      "recipe-knowledge-1",
		Name:          "Private knowledge workload",
		Maturity:      cloudcontracts.RecipeExperimental,
		Sources: []cloudcontracts.RecipeSourceV1{{
			URL: "https://github.com/example/knowledge-workload", Version: "v1.0.0", Commit: "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", License: "Apache-2.0", RetrievedAt: now, Official: true,
		}},
		Requirements: cloudcontracts.ResourceRequirementsV1{MinVCPU: 4, MinMemoryMiB: 8192, MinDiskGiB: 80, Architecture: cloudcontracts.ArchitectureAMD64},
		Install:      cloudcontracts.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: []string{"artifact-ready", "service-ready"}, Steps: []cloudcontracts.InstallStepV1{{ID: "install", Summary: "Install the signed workload artifact", TimeoutSeconds: 900}}},
		Health:       cloudcontracts.HealthContractV1{Liveness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/healthz"}, Readiness: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeHTTP, Target: "/readyz"}, Semantic: cloudcontracts.ProbeV1{Kind: cloudcontracts.ProbeCommand, Target: "verify-service"}},
		Lifecycle:    cloudcontracts.LifecycleContractV1{Start: "start", Stop: "stop", Restart: "restart", Upgrade: "upgrade", Rollback: "rollback", Backup: "backup", Restore: "restore", Destroy: "destroy"},
		DataSlots:    []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}},
		SecretSlots:  []cloudcontracts.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider access", Delivery: cloudcontracts.SecretDeliveryFile}},
	}
	draft := cloudcontracts.ResearchDraftV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1,
		Region:        "ap-south-1",
		Candidates: []cloudcontracts.QuoteRequestCandidateV1{
			{CandidateID: "economy", Tier: cloudcontracts.QuoteTierEconomy, InstanceType: "m7i.large", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80},
			{CandidateID: "recommended", Tier: cloudcontracts.QuoteTierRecommended, InstanceType: "m7i.xlarge", PurchaseOption: cloudcontracts.PurchaseOnDemand, EstimatedDiskGiB: 80},
		},
	}
	return runtime.ResearchOutput{Recipe: recipe, Draft: draft, Title: "Private knowledge workload", Summary: "Official-source private single-VM research draft; obtain a broker quote before creating billable resources."}
}
