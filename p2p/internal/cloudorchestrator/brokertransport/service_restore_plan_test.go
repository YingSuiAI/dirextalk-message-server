package brokertransport

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestServiceRestorePlanTransportPersistsExactSignedEnvelope(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	transport, err := New(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x62}, 32)), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := broker.ServiceRestorePlanRequest{Schema: broker.ServiceRestorePlanSchema, RestorePlanID: "restore-plan-transport-0001", ServiceID: "service-transport-0001", DeploymentID: "deployment-transport-0001", BackupID: "backup-transport-0001", InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", ImageID: "ami-0123456789abcdef0", SnapshotRefs: []broker.ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0"}}}
	digest, err := runtime.ServiceRestorePlanRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	command := runtime.ServiceRestorePlanCommand{CommandID: "command-transport-0001", RestorePlanID: request.RestorePlanID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, ConnectionID: "connection-transport-0001", NodeKeyID: "node-transport-0001", ExpectedGeneration: 1, NodeCounter: 3, RequestDigest: digest}
	signed, err := transport.BuildServiceRestorePlanCommand(command, request)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.ValidateSignedServiceRestorePlanCommand(signed) != nil {
		t.Fatal("signed command invalid")
	}
	parsed, err := broker.ParseServiceRestorePlanCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.RequestSHA256() != signed.RequestSHA256 || parsed.PayloadSHA256 != signed.PayloadSHA256 {
		t.Fatal("durable hashes drifted")
	}
	tampered := signed
	tampered.PayloadJSON += " "
	if _, err = transport.RequestServiceRestorePlan(t.Context(), "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", command, tampered, request); err == nil {
		t.Fatal("tampered durable payload must fail before I/O")
	}
}
