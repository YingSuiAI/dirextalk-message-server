package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/serviceapi"
)

func TestModulePairingResumeUsesDeviceChallengeAndExistingWorkload(t *testing.T) {
	now := time.Date(2026, 7, 16, 6, 0, 0, 0, time.UTC)
	target := cloudcontracts.PairingResumeTargetV1{DeploymentID: "deployment-pairing-module", DeploymentRevision: 7, PlanID: "plan-pairing-module", CloudConnectionID: "connection-pairing-module", ExecutionID: "execution-pairing-module", RecipeExecutionManifestDigest: "sha256:" + string(bytes.Repeat([]byte{'a'}, 64)), JobID: "job-pairing-module", JobRevision: 3}
	approval, err := cloudcontracts.NewPairingResumeApprovalV1(target, "approval-pairing-module", "challenge-pairing-module", "device-pairing-module", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	store := &pairingResumeModuleStore{prepared: PreparePairingResumeResult{Confirmation: PairingResumeConfirmation{Deployment: Deployment{DeploymentID: target.DeploymentID, Execution: "waiting_user", Revision: 7}, Job: Job{JobID: target.JobID, Execution: "waiting_user", Revision: 3}, Approval: approval}, Created: true}}
	published := 0
	m := New(store, Config{OwnerMXID: func() string { return "@owner:example.com" }, Now: func() time.Time { return now }, NewID: func(kind string) string { return kind + "-generated" }, Publish: func(context.Context, string, string, map[string]any) error { published++; return nil }})
	result, apiErr := m.Handlers()[serviceapi.CloudDeploymentPairingResumeAction](t.Context(), map[string]any{"deployment_id": target.DeploymentID, "expected_revision": float64(7), "idempotency_key": "11111111-1111-4111-8111-111111111111"})
	if apiErr != nil || result == nil || store.prepare.ExpectedRevision != 7 || published != 0 {
		t.Fatalf("prepare=%#v request=%#v published=%d err=%v", result, store.prepare, published, apiErr)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, 32))
	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	store.approved = ApprovePairingResumeResult{Deployment: Deployment{DeploymentID: target.DeploymentID, Execution: "queued", Revision: 8}, Job: Job{JobID: target.JobID, Execution: "queued", Revision: 4}, Created: true}
	result, apiErr = m.Handlers()[serviceapi.CloudDeploymentPairingResumeAction](t.Context(), map[string]any{"deployment_id": target.DeploymentID, "expected_revision": float64(7), "approval": signed, "idempotency_key": "22222222-2222-4222-8222-222222222222"})
	if apiErr != nil || result == nil || store.approve.Approval.Intent != cloudcontracts.PairingResumeIntent || published != 2 {
		t.Fatalf("approve=%#v request=%#v published=%d err=%v", result, store.approve, published, apiErr)
	}
	if _, apiErr = m.Handlers()[serviceapi.CloudDeploymentPairingResumeAction](t.Context(), map[string]any{"deployment_id": target.DeploymentID, "expected_revision": float64(7), "pairing_url": "https://secret.invalid", "idempotency_key": "33333333-3333-4333-8333-333333333333"}); apiErr == nil || apiErr.Code != cloudInvalidParamsCode {
		t.Fatalf("pairing resume accepted an unknown pairing-material field: %#v", apiErr)
	}
}

type pairingResumeModuleStore struct {
	Store
	prepared PreparePairingResumeResult
	approved ApprovePairingResumeResult
	prepare  PreparePairingResumeRequest
	approve  ApprovePairingResumeRequest
}

func (s *pairingResumeModuleStore) PrepareCloudPairingResume(_ context.Context, r PreparePairingResumeRequest) (PreparePairingResumeResult, error) {
	s.prepare = r
	return s.prepared, nil
}

func (s *pairingResumeModuleStore) ApproveCloudPairingResume(_ context.Context, r ApprovePairingResumeRequest) (ApprovePairingResumeResult, error) {
	s.approve = r
	return s.approved, nil
}
