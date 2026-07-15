package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestWorkerServiceSecretRequestGoldenOmitsProviderVersion(t *testing.T) {
	request := WorkerServiceSecretRequest{TaskID: "task-secret-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ArtifactDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", SlotID: "model_token", SecretRef: "secret_ref:model-token"}
	raw, _ := json.Marshal(request)
	parsed, err := ParseWorkerServiceSecretRequest(raw)
	if err != nil || parsed != request {
		t.Fatalf("parse=%#v err=%v", parsed, err)
	}
	digest := sha256.Sum256(raw)
	if got := hex.EncodeToString(digest[:]); got != "1a4eace76735611ff2761dbb6adf2ec06c20affb38cef4d5985e22c8a6159631" {
		t.Fatalf("worker service-secret request drift: %s", got)
	}
	var expanded map[string]any
	_ = json.Unmarshal(raw, &expanded)
	expanded["provider_version"] = "worker-selected"
	expandedRaw, _ := json.Marshal(expanded)
	if _, err = ParseWorkerServiceSecretRequest(expandedRaw); err == nil {
		t.Fatal("worker-selected provider_version was accepted")
	}
}
