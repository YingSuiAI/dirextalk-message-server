package cloud

import (
	"context"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestModuleServiceSecretBootstrapAcceptsOnlyServerDerivedEvidence(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	approval, err := cloudcontracts.NewServiceSecretApprovalV1(cloudcontracts.ServiceSecretApprovalV1{
		ApprovalID: "approval-service-secret-module-1", ChallengeID: "challenge-service-secret-module-1", SignerKeyID: "device-service-secret-module-1",
		SessionID: "session-service-secret-module-1", ConnectionID: "connection-service-secret-module-1", DeploymentID: "deployment-service-secret-module-1",
		TaskID: "task-service-secret-module-1", ExecutionID: "execution-service-secret-module-1",
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		RecipeDigest:   "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ArtifactDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		SlotID:         "model_token", SecretRef: "secret_ref:plan/model_token", Purpose: "model provider access", Delivery: "environment",
		IssuedAt: now, ExpiresAt: now.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &serviceSecretBootstrapModuleStore{result: PrepareServiceSecretBootstrapResult{
		Confirmation: ServiceSecretBootstrapConfirmation{Approval: approval},
		StackBaseURL: "https://abcdefghij.execute-api.ap-south-1.amazonaws.com/prod", Created: true,
	}}
	module := New(store, Config{
		OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now },
		NewID: func(kind string) string { return kind + "-generated-1" },
	})
	params := map[string]any{
		"deployment_id": "deployment-service-secret-module-1", "slot_id": "model_token", "expected_revision": float64(7),
		"idempotency_key": "11111111-1111-4111-8111-111111111111",
	}
	result, apiErr := module.Handlers()[actionSecretsBootstrapPlan](t.Context(), params)
	if apiErr != nil || result == nil || store.calls != 1 {
		t.Fatalf("result=%#v request=%#v calls=%d err=%v", result, store.request, store.calls, apiErr)
	}
	if store.request.OwnerMXID != "@owner:example.com" || store.request.DeploymentID != params["deployment_id"] || store.request.SlotID != "model_token" ||
		store.request.ExpectedRevision != 7 || store.request.ExpiresAt-store.request.CreatedAt != int64((10*time.Minute).Milliseconds()) ||
		store.request.SessionID != "service_secret_session-generated-1" || store.request.ApprovalID != "service_secret_approval-generated-1" ||
		store.request.ChallengeID != "service_secret_challenge-generated-1" {
		t.Fatalf("unexpected derived request: %#v", store.request)
	}
	response, ok := result.(map[string]any)
	if !ok || response["stack_base_url"] != store.result.StackBaseURL {
		t.Fatalf("unexpected response: %#v", result)
	}

	injected := map[string]any{}
	for key, value := range params {
		injected[key] = value
	}
	injected["secret_ref"] = "secret_ref:attacker/value"
	if _, apiErr = module.Handlers()[actionSecretsBootstrapPlan](t.Context(), injected); apiErr == nil || store.calls != 1 {
		t.Fatalf("client evidence injection was accepted: calls=%d err=%v", store.calls, apiErr)
	}
}

func TestStackBaseURLFromBrokerCommandURLAllowsOnlySafeOptionalStage(t *testing.T) {
	for raw, want := range map[string]string{
		"https://broker.example.com/v2/commands":      "https://broker.example.com",
		"https://broker.example.com/prod/v2/commands": "https://broker.example.com/prod",
	} {
		if got, err := StackBaseURLFromBrokerCommandURL(raw); err != nil || got != want {
			t.Fatalf("StackBaseURLFromBrokerCommandURL(%q)=%q,%v want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{
		"http://broker.example.com/prod/v2/commands", "https://user@broker.example.com/prod/v2/commands",
		"https://broker.example.com/prod/v2/commands?token=x", "https://broker.example.com/prod/v2/commands#x",
		"https://broker.example.com/a/b/v2/commands", "https://broker.example.com/prod/v2/commands/sessions",
		"https://broker.example.com/prod%2Fhidden/v2/commands", "https://broker.example.com:443/prod/v2/commands",
	} {
		if _, err := StackBaseURLFromBrokerCommandURL(raw); err == nil {
			t.Fatalf("unsafe stack base input accepted: %q", raw)
		}
	}
}

type serviceSecretBootstrapModuleStore struct {
	Store
	request PrepareServiceSecretBootstrapRequest
	result  PrepareServiceSecretBootstrapResult
	calls   int
}

func (s *serviceSecretBootstrapModuleStore) PrepareCloudServiceSecretBootstrap(_ context.Context, request PrepareServiceSecretBootstrapRequest) (PrepareServiceSecretBootstrapResult, error) {
	s.calls++
	s.request = request
	return s.result, nil
}
