package cloudorchestrator

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestServiceSecretApprovalStackGolden(t *testing.T) {
	seed, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	private := ed25519.NewKeyFromSeed(seed)
	issued := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	approval, err := NewServiceSecretApprovalV1(ServiceSecretApprovalV1{
		ApprovalID: "approval-secret-0001", ChallengeID: "challenge-secret-0001", SignerKeyID: "device-secret-0001",
		SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-secret-0001", ExecutionID: "execution-0001",
		ManifestDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", RecipeDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", ArtifactDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		SlotID: "model_token", SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment", IssuedAt: issued, ExpiresAt: issued.Add(10 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if got := hex.EncodeToString(digest[:]); got != "2441e1f58e58e33ecd81608a5a8ad6038c9a19f21a23a1df517d8b1adbbe4b96" {
		t.Fatalf("Stack signing payload SHA256=%s", got)
	}
	if approval.ContextDigest != "sha256:43a1feb8f2ef1a20fd795d184b1ff29317c5e699730d68e6649d4b534305b211" {
		t.Fatalf("Stack context digest=%s", approval.ContextDigest)
	}
	signed, err := approval.Sign(private, issued)
	if err != nil || signed.Verify(private.Public().(ed25519.PublicKey), issued) != nil || signed.Signature != "_63R1mYbLDsi2GHnUtcMRBH-X4G3lJlwLNp5ExfPBQ6VmxYlXxReHMfpi6xnPV9Tk3NBxtCHzR6OiXe52ZNHCA" {
		t.Fatalf("Stack signature=%q err=%v", signed.Signature, err)
	}
}
