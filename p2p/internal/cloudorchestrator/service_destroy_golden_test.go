package cloudorchestrator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const serviceDestroyApprovalPayloadGolden = "740a7d50231910bcd74e55a4ea66b8118076946b0f168cd5fcc9d8eef3dca091"

func TestServiceDestroyApprovalSigningPayloadGolden(t *testing.T) {
	now := time.Date(2026, time.July, 15, 6, 0, 0, 0, time.UTC)
	approval, err := cloudorchestrator.NewServiceDestroyApprovalV1(cloudorchestrator.ServiceDestroyTargetV1{
		ServiceID: "service-destroy-0001", ServiceRevision: 3, DeploymentID: "deployment-destroy-0001", DeploymentRevision: 8,
		CloudConnectionID: "connection-destroy-0001", RecipeID: "recipe-destroy-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0bbbbbbbbbbbbbbbb", "vol-0aaaaaaaaaaaaaaaa"}, NetworkInterfaceIDs: []string{"eni-0bbbbbbbbbbbbbbbb", "eni-0aaaaaaaaaaaaaaaa"},
	}, "destroy-approval-0001", "destroy-challenge-0001", "owner-device-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if got := hex.EncodeToString(sum[:]); got != serviceDestroyApprovalPayloadGolden {
		t.Fatalf("update ServiceDestroyApprovalV1 payload golden digest: %s", got)
	}
}
