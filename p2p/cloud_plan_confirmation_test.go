package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
	"github.com/google/uuid"
)

type cloudConfirmationRecordingStore struct {
	*cloudQuoteMemoryStore
	prepareRequests []cloudmodule.PreparePlanConfirmationRequest
	approveRequests []cloudmodule.ApproveCloudPlanRequest
}

func (s *cloudConfirmationRecordingStore) PrepareCloudPlanConfirmation(_ context.Context, request cloudmodule.PreparePlanConfirmationRequest) (cloudmodule.PreparePlanConfirmationResult, error) {
	s.prepareRequests = append(s.prepareRequests, request)
	return cloudmodule.PreparePlanConfirmationResult{
		Confirmation: cloudmodule.PlanConfirmation{
			Plan:     cloudmodule.Plan{PlanID: request.PlanID, Status: cloudmodule.PlanStatusReadyForConfirmation, Revision: request.ExpectedRevision + 1, PlanHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			Approval: cloudcontracts.ApprovalV1{ApprovalID: request.ApprovalID, ChallengeID: request.ChallengeID, SignerKeyID: "device-confirmation-1"},
		},
		EventID: "event-confirmation-1", Created: true,
	}, nil
}

func (s *cloudConfirmationRecordingStore) ApproveCloudPlan(_ context.Context, request cloudmodule.ApproveCloudPlanRequest) (cloudmodule.ApproveCloudPlanResult, error) {
	s.approveRequests = append(s.approveRequests, request)
	return cloudmodule.ApproveCloudPlanResult{
		Plan:       cloudmodule.Plan{PlanID: request.PlanID, Status: cloudmodule.PlanStatusApproved, Revision: request.ExpectedRevision + 1},
		Deployment: request.Deployment,
		Job:        request.Job,
		Created:    true,
	}, nil
}

func TestCloudPlanConfirmationAndApprovalStayOwnerOnlyAndBindOneRequest(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	store := &cloudConfirmationRecordingStore{cloudQuoteMemoryStore: &cloudQuoteMemoryStore{
		MemoryStore: p2pstorage.NewMemoryStore(), quoteFound: true,
		quote: cloudmodule.QuoteView{QuoteID: "quote-confirmation-1", ConnectionID: cloudTestConnectionID, Region: "ap-south-1", Currency: "USD", QuotedAt: now, ValidUntil: now.Add(10 * time.Minute)},
	}}
	service, err := NewServiceWithStore(context.Background(), Config{ServerName: "example.com"}, store)
	if err != nil {
		t.Fatal(err)
	}
	router := newP2PTestRouter(service)
	planID := "cloud-plan-confirmation-1"
	prepareKey := uuid.NewString()
	prepared := cloudCommand(t, router, service, "cloud.plans.confirmation.prepare", map[string]any{
		"plan_id": planID, "expected_revision": 3, "quote_id": "quote-confirmation-1", "candidate_tier": "recommended", "idempotency_key": prepareKey,
	})
	confirmation, ok := prepared["confirmation"].(map[string]any)
	if !ok || confirmation["plan"] == nil || confirmation["approval"] == nil || len(store.prepareRequests) != 1 {
		t.Fatalf("prepare response=%#v requests=%#v", prepared, store.prepareRequests)
	}
	if request := store.prepareRequests[0]; request.OwnerMXID == "" || request.PlanID != planID || request.ExpectedRevision != 3 || request.CandidateTier != "recommended" || request.ExpiresAt <= request.CreatedAt || request.ExpiresAt-request.CreatedAt > int64((5*time.Minute).Milliseconds()) {
		t.Fatalf("prepared request did not bind the reviewed scope: %#v", request)
	}

	approval := testCloudApprovalForModule(t, now, planID)
	approvalParams := approvalMap(t, approval)
	approved := cloudCommand(t, router, service, "cloud.plans.approve", map[string]any{
		"plan_id": planID, "expected_revision": 4, "approval": approvalParams, "idempotency_key": uuid.NewString(),
	})
	if approved["deployment"] == nil || approved["job"] == nil || len(store.approveRequests) != 1 {
		t.Fatalf("approval response=%#v requests=%#v", approved, store.approveRequests)
	}
	if request := store.approveRequests[0]; request.PlanID != planID || request.ExpectedRevision != 4 || request.Approval.Signature != approval.Signature || request.Deployment.Resource != "none" || request.Job.Kind != "provision" || request.Outbox.Kind != cloudmodule.OutboxKindDeploymentProvisionRequested {
		t.Fatalf("approval request did not bind provision transition: %#v", request)
	}

	agentRequest := jsonRequest(t, "/_p2p/command", map[string]any{
		"action": "cloud.plans.confirmation.prepare",
		"params": map[string]any{"plan_id": planID, "expected_revision": 3, "quote_id": "quote-confirmation-1", "candidate_tier": "recommended", "idempotency_key": uuid.NewString()},
	})
	agentRequest.Header.Set("Authorization", "Bearer "+service.AgentToken())
	agentRecorder := httptest.NewRecorder()
	router.ServeHTTP(agentRecorder, agentRequest)
	if agentRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("agent token must not prepare a cloud confirmation: status=%d body=%s", agentRecorder.Code, agentRecorder.Body.String())
	}
}

func testCloudApprovalForModule(t *testing.T, now time.Time, planID string) cloudcontracts.ApprovalV1 {
	t.Helper()
	private, err := testCloudApprovalPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	plan := cloudcontracts.PlanV1{
		SchemaVersion: cloudcontracts.SchemaVersionV1, PlanID: planID, Revision: 4, Status: cloudcontracts.PlanReadyForConfirmation, CloudConnectionID: cloudTestConnectionID,
		Recipe:        cloudcontracts.RecipeBindingV1{RecipeID: "recipe-module-confirmation-1", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Maturity: cloudcontracts.RecipeExperimental},
		Quote:         cloudcontracts.QuoteBindingV1{QuoteID: "quote-confirmation-1", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", ValidUntil: now.Add(10 * time.Minute), CandidateID: "candidate-module-confirmation-1"},
		ResourceScope: cloudcontracts.ResourceScopeV1{Region: "ap-south-1", AvailabilityZones: []string{"ap-south-1a"}, InstanceType: "m7i.xlarge", Architecture: cloudcontracts.ArchitectureAMD64, VCPU: 4, MemoryMiB: 16384, DiskGiB: 80, PurchaseOption: cloudcontracts.PurchaseOnDemand},
		NetworkScope:  cloudcontracts.NetworkScopeV1{PublicIngress: false, EntryPoint: cloudcontracts.EntryPointNone, TLSRequired: false, AuthenticationRequired: false},
		SecretScope:   []cloudcontracts.SecretReferenceV1{}, IntegrationScope: []cloudcontracts.IntegrationScopeV1{},
	}
	approval, err := cloudcontracts.NewApprovalV1(plan, "approval-module-confirmation-1", "challenge-module-confirmation-1", "device-confirmation-1", now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(private, now)
	if err != nil {
		t.Fatal(err)
	}
	return approval
}

func testCloudApprovalPrivateKey() (ed25519.PrivateKey, error) {
	_, private, err := ed25519.GenerateKey(rand.Reader)
	return private, err
}

func approvalMap(t *testing.T, approval cloudcontracts.ApprovalV1) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(approval)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
