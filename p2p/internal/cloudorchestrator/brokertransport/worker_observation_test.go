package brokertransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestBuildWorkerBootstrapObservationCommandBindsOnlyDeployment(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.July, 15, 8, 0, 0, 0, time.UTC)
	transport, err := New(privateKey, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	request := runtime.WorkerBootstrapObservationRequest{DeploymentID: "deployment-create-0001"}
	digest, err := request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	logical := runtime.WorkerBootstrapObservationCommand{
		CommandID: "command-observe-0001", DeploymentID: request.DeploymentID, ConnectionID: "connection-create-0001", NodeKeyID: "node-key-1",
		ExpectedGeneration: 2, NodeCounter: 10, Attempt: 1, RequestDigest: digest,
	}
	signed, err := transport.BuildWorkerBootstrapObservationCommand(logical, request, now)
	if err != nil {
		t.Fatal(err)
	}
	if signed.RequestSHA256 == signed.PayloadSHA256 || len(signed.RequestSHA256) != 64 || signed.IssuedAt != now || signed.ExpiresAt != now.Add(commandLifetime) {
		t.Fatalf("signed worker observation command = %#v", signed)
	}
	parsed, err := broker.ParseDeploymentObserveCommand([]byte(signed.EnvelopeJSON))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnectionID != logical.ConnectionID || parsed.CommandID != logical.CommandID || parsed.NodeCounter != logical.NodeCounter || parsed.RequestSHA256() != signed.RequestSHA256 {
		t.Fatalf("parsed worker observation command does not bind logical identity: %#v", parsed)
	}
	decoded, err := parsed.DeploymentObserveRequest()
	if err != nil || !reflect.DeepEqual(decoded, broker.DeploymentObserveRequest{DeploymentID: request.DeploymentID}) {
		t.Fatalf("decoded worker observation request=%#v err=%v", decoded, err)
	}
}

func TestRuntimeWorkerBootstrapObservationPreservesOptionalFirstHeartbeat(t *testing.T) {
	leaseExpiresAt := "2026-07-15T08:02:00.000Z"
	result := broker.DeploymentObserveResult{Observation: broker.DeploymentObservation{
		Schema: broker.DeploymentObservationSchema, DeploymentID: "deployment-create-0001",
		Resource: broker.DeploymentObservationResource{Status: "provisioning", InstanceID: "i-0123456789abcdef0"},
		Worker: broker.DeploymentObservationWorker{
			BootstrapSessionState: "active", LeaseEpoch: 1, LeaseExpiresAt: &leaseExpiresAt, LastSequence: 0, LastEventAt: nil,
		},
		ObservedAt: "2026-07-15T08:00:30.000Z",
	}}
	got, err := runtimeWorkerBootstrapObservation(result)
	if err != nil {
		t.Fatal(err)
	}
	if got.WorkerSessionState != "active" || got.LeaseExpiresAt.IsZero() || !got.LastEventAt.IsZero() || got.LastSequence != 0 {
		t.Fatalf("runtime worker observation = %#v", got)
	}
}
