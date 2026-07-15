package runtime

import (
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

func TestDeploymentDestroyRunnerPersistsBeforeRequestAndCompletesReadBack(t *testing.T) {
	now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	claim := validDeploymentDestroyClaim(t, now)
	store := &fakeDeploymentDestroyStore{claim: claim}
	transport := &fakeDeploymentDestroyTransport{store: store}
	runner := NewDeploymentDestroyRunner(store, transport, Config{WorkerID: "deployment-destroy-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if !store.started || !store.persisted || !store.completed || !transport.sawPersisted || store.deferred != "" || store.failed != "" {
		t.Fatalf("runner store=%#v transport=%#v", store, transport)
	}
}

func TestDeploymentDestroyRunnerKeepsResourceDestroyingUntilReadBack(t *testing.T) {
	now := time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC)
	store := &fakeDeploymentDestroyStore{claim: validDeploymentDestroyClaim(t, now)}
	transport := &fakeDeploymentDestroyTransport{store: store, requestErr: ServiceDestroyRetryable("deployment_destroy_in_progress", errors.New("read-back pending"))}
	runner := NewDeploymentDestroyRunner(store, transport, Config{WorkerID: "deployment-destroy-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed || store.deferred != "deployment_destroy_in_progress" || store.completed || store.failed != "" {
		t.Fatalf("processed=%v err=%v store=%#v", processed, err, store)
	}
}

type fakeDeploymentDestroyStore struct {
	claim                         DeploymentDestroyClaim
	started, persisted, completed bool
	deferred, failed              string
}

func (s *fakeDeploymentDestroyStore) ClaimDeploymentDestroy(context.Context, string, time.Duration) (DeploymentDestroyClaim, bool, error) {
	return s.claim, true, nil
}
func (s *fakeDeploymentDestroyStore) PersistDeploymentDestroyCommand(context.Context, DeploymentDestroyClaim, SignedServiceDestroyCommand) error {
	s.persisted = true
	return nil
}
func (s *fakeDeploymentDestroyStore) MarkDeploymentDestroyStarted(context.Context, DeploymentDestroyClaim) error {
	s.started = true
	return nil
}
func (s *fakeDeploymentDestroyStore) CompleteDeploymentDestroy(context.Context, DeploymentDestroyClaim, ServiceDestroyResult) error {
	s.completed = true
	return nil
}
func (s *fakeDeploymentDestroyStore) DeferDeploymentDestroy(_ context.Context, _ DeploymentDestroyClaim, code string, _ time.Time) error {
	s.deferred = code
	return nil
}
func (s *fakeDeploymentDestroyStore) FailDeploymentDestroy(_ context.Context, _ DeploymentDestroyClaim, code string) error {
	s.failed = code
	return nil
}

type fakeDeploymentDestroyTransport struct {
	store        *fakeDeploymentDestroyStore
	requestErr   error
	sawPersisted bool
}

func (t *fakeDeploymentDestroyTransport) BuildDeploymentDestroyCommand(ServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.DeploymentDestroyApprovalV1) (SignedServiceDestroyCommand, error) {
	return SignedServiceDestroyCommand{EnvelopeJSON: "{}", PayloadJSON: "{}", PayloadSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", IssuedAt: time.Date(2026, time.July, 16, 10, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, time.July, 16, 10, 4, 0, 0, time.UTC)}, nil
}
func (t *fakeDeploymentDestroyTransport) RequestDeploymentDestroy(_ context.Context, _ string, command ServiceDestroyCommand, signed SignedServiceDestroyCommand, request broker.DeploymentDestroyRequest, _ cloudcontracts.DeploymentDestroyApprovalV1) (ServiceDestroyResult, error) {
	t.sawPersisted = t.store != nil && t.store.persisted
	if t.requestErr != nil {
		return ServiceDestroyResult{}, t.requestErr
	}
	return ServiceDestroyResult{Status: "verified_destroyed", DeploymentID: request.DeploymentID, InstanceID: request.InstanceID, VolumeIDs: request.VolumeIDs, NetworkInterfaceIDs: request.NetworkInterfaceIDs, SecretRefs: request.SecretRefs, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: "{}"}, nil
}

func validDeploymentDestroyClaim(t *testing.T, now time.Time) DeploymentDestroyClaim {
	t.Helper()
	target := cloudcontracts.DeploymentDestroyTargetV1{DeploymentID: "deployment-retained-0001", DeploymentRevision: 12, PlanID: "plan-retained-0001", CloudConnectionID: "connection-retained-0001", ResourceStatus: "orphaned", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}, SecretRefs: []string{"secret_ref:plan/model"}}
	approval, err := cloudcontracts.NewDeploymentDestroyApprovalV1(target, "approval-retained-0001", "challenge-retained-0001", "device-retained-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, _ := ed25519.GenerateKey(nil)
	approval, err = approval.Sign(privateKey, now)
	if err != nil {
		t.Fatal(err)
	}
	request := broker.DeploymentDestroyRequest{Schema: broker.DeploymentDestroySchema, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs, SecretRefs: target.SecretRefs}
	digest, err := ServiceDestroyRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	return DeploymentDestroyClaim{OutboxID: "outbox-retained-0001", Kind: DeploymentDestroyRequested, AggregateType: "deployment", AggregateID: target.DeploymentID, DeploymentID: target.DeploymentID, PlanID: target.PlanID, JobID: "job-retained-0001", DeploymentExecution: "finished", DeploymentOutcome: "failed", ConnectionID: target.CloudConnectionID, Region: "ap-south-1", BrokerEndpoint: "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-retained-0001", LeaseToken: "lease-retained-0001", ExpectedGeneration: 1, DeploymentRevision: 13, Attempt: 1, Approval: approval, Request: request, Command: ServiceDestroyCommand{CommandID: "command-retained-0001", DeploymentID: target.DeploymentID, ConnectionID: target.CloudConnectionID, NodeKeyID: "node-retained-0001", ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, RequestDigest: digest, State: "allocated"}}
}
