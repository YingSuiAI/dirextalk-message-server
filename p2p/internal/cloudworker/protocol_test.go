package cloudworker

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestOutboundWorkerPayloadsRejectSecretsAndFreeformOutput(t *testing.T) {
	manifest := validTestManifest("https://broker.example.invalid/v2/worker-sessions")
	claim, err := NewClaimRequest(manifest, InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString([]byte("instance-document")),
		SignatureB64: base64.StdEncoding.EncodeToString([]byte("instance-signature")),
	})
	if err != nil {
		t.Fatalf("NewClaimRequest() error = %v", err)
	}
	claimRaw, err := json.Marshal(claim)
	if err != nil {
		t.Fatalf("marshal claim: %v", err)
	}
	if _, err := ParseClaimRequest([]byte(strings.TrimSuffix(string(claimRaw), "}") + `,"aws_session_token":"forbidden"}`)); err == nil {
		t.Fatal("ParseClaimRequest() accepted a credential-shaped field")
	}

	event := SessionEvent{
		Schema:             WorkerEventV1Schema,
		ConnectionID:       manifest.ConnectionID,
		DeploymentID:       manifest.DeploymentID,
		BootstrapSessionID: manifest.BootstrapSessionID,
		LeaseEpoch:         1,
		Sequence:           1,
		Kind:               EventKindHeartbeat,
		OccurredAt:         "2026-07-14T07:00:00.000Z",
	}
	eventRaw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	if _, err := ParseSessionEvent([]byte(strings.TrimSuffix(string(eventRaw), "}") + `,"raw_worker_log":"forbidden"}`)); err == nil {
		t.Fatal("ParseSessionEvent() accepted a raw log field")
	}
}
