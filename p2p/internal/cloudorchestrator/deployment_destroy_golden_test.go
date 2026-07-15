package cloudorchestrator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const deploymentDestroyApprovalPayloadGolden = "b4e372c59fb8ea526fa0cf4b91fca57e0476192d85f33cd4dc9b8971eae5aa45"

func TestDeploymentDestroyApprovalSigningPayloadGolden(t *testing.T) {
	now := time.Date(2026, time.July, 16, 8, 0, 0, 0, time.UTC)
	approval, err := cloudorchestrator.NewDeploymentDestroyApprovalV1(cloudorchestrator.DeploymentDestroyTargetV1{
		DeploymentID: "deployment-destroy-golden-0001", DeploymentRevision: 12,
		PlanID: "plan-destroy-golden-0001", CloudConnectionID: "connection-destroy-golden-0001", ResourceStatus: "orphaned",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"},
		NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"}, SecretRefs: []string{"secret_ref:plan/registry", "secret_ref:plan/model"},
	}, "deployment-destroy-approval-golden-0001", "deployment-destroy-challenge-golden-0001", "owner-device-golden-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != deploymentDestroyApprovalPayloadGolden {
		t.Fatalf("update DeploymentDestroyApprovalV1 payload golden digest: %s", got)
	}
}
