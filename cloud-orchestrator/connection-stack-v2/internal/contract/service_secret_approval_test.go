package contract

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

func TestServiceSecretApprovalDeterministicCrossLanguageVector(t *testing.T) {
	seed, _ := hex.DecodeString("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
	private := ed25519.NewKeyFromSeed(seed)
	proof := ServiceSecretApprovalProof{SchemaVersion: "cloud-orchestrator/v1", Intent: ServiceSecretApprovalIntent, ApprovalID: "approval-secret-0001", ChallengeID: "challenge-secret-0001", SignerKeyID: "device-secret-0001", SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-secret-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", RecipeDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", ArtifactDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222", SlotID: "model_token", SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment", IssuedAt: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, 7, 15, 12, 10, 0, 0, time.UTC), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))}
	proof.ContextDigest, _ = proof.Context().Digest()
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, payload))
	raw, _ := json.Marshal(proof)
	parsed, err := ParseServiceSecretApprovalProof(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = parsed.Verify(private.Public().(ed25519.PublicKey), proof.IssuedAt); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	assertGolden(t, "approval proof JSON", string(raw), `{"schema_version":"cloud-orchestrator/v1","intent":"service_secret","approval_id":"approval-secret-0001","challenge_id":"challenge-secret-0001","signer_key_id":"device-secret-0001","session_id":"secret-session-0001","connection_id":"connection-0001","deployment_id":"deployment-0001","task_id":"task-secret-0001","execution_id":"execution-0001","manifest_digest":"sha256:3333333333333333333333333333333333333333333333333333333333333333","recipe_digest":"sha256:1111111111111111111111111111111111111111111111111111111111111111","artifact_digest":"sha256:2222222222222222222222222222222222222222222222222222222222222222","slot_id":"model_token","secret_ref":"secret_ref:model-token-001","purpose":"model inference","delivery":"environment","context_digest":"sha256:43a1feb8f2ef1a20fd795d184b1ff29317c5e699730d68e6649d4b534305b211","issued_at":"2026-07-15T12:00:00Z","expires_at":"2026-07-15T12:10:00Z","signature":"_63R1mYbLDsi2GHnUtcMRBH-X4G3lJlwLNp5ExfPBQ6VmxYlXxReHMfpi6xnPV9Tk3NBxtCHzR6OiXe52ZNHCA"}`)
	assertGolden(t, "approval signing payload SHA256", hex.EncodeToString(digest[:]), "2441e1f58e58e33ecd81608a5a8ad6038c9a19f21a23a1df517d8b1adbbe4b96")
	assertGolden(t, "approval public key", hex.EncodeToString(private.Public().(ed25519.PublicKey)), "03a107bff3ce10be1d70dd18e74bc09967e4d6309ba50d5f1ddc8664125531b8")
	assertGolden(t, "approval signature", proof.Signature, "_63R1mYbLDsi2GHnUtcMRBH-X4G3lJlwLNp5ExfPBQ6VmxYlXxReHMfpi6xnPV9Tk3NBxtCHzR6OiXe52ZNHCA")
}
