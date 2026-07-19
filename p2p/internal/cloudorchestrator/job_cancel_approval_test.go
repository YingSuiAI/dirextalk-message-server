package cloudorchestrator

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestJobCancelApprovalGoldenAndTamperResistance(t *testing.T) {
	now := time.Date(2026, time.July, 16, 6, 0, 0, 0, time.UTC)
	target := JobCancelTargetV1{JobID: "job-cancel-golden-1", JobRevision: 7, JobKind: "install", PlanID: "plan-cancel-golden-1", DeploymentID: "deployment-cancel-golden-1", DeploymentRevision: 11, CloudConnectionID: "connection-cancel-golden-1", ResourceStatus: "active"}
	approval, err := NewJobCancelApprovalV1(target, "approval-cancel-golden-1", "challenge-cancel-golden-1", "device-cancel-golden-1", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const wantPayloadSHA256 = "203a3fa8412a66d7aec6a29616fb667148a0af41124871690aa042ad52db4e53"
	if got := hex.EncodeToString(sum[:]); got != wantPayloadSHA256 {
		t.Fatalf("payload sha256=%s", got)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	signed, err := approval.Sign(privateKey, now.Add(time.Minute))
	if err != nil || signed.Verify(privateKey.Public().(ed25519.PublicKey), now.Add(time.Minute)) != nil {
		t.Fatalf("signed approval err=%v", err)
	}
	tampered := signed
	tampered.ResourceStatus = "retained_tracked"
	if tampered.Verify(privateKey.Public().(ed25519.PublicKey), now.Add(time.Minute)) == nil {
		t.Fatal("resource scope tamper verified")
	}
}
