package runtime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

func TestServiceDestroyRunnerPersistsBeforeRequestAndCompletesVerifiedResult(t *testing.T) {
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	claim := validServiceDestroyClaim(t, now)
	store := &fakeServiceDestroyStore{claim: claim}
	transport := &fakeServiceDestroyTransport{store: store}
	runner := NewServiceDestroyRunner(store, transport, Config{WorkerID: "destroy-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if !store.started || !store.persisted || !store.completed || transport.requests != 1 || store.deferred != "" || store.failed != "" {
		t.Fatalf("runner state=%#v requests=%d", store, transport.requests)
	}
	if !transport.sawPersisted {
		t.Fatal("broker request occurred before the signed command was durably persisted")
	}
}

func TestServiceDestroyRunnerDefersInProgressWithoutReportingDestroyed(t *testing.T) {
	now := time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC)
	claim := validServiceDestroyClaim(t, now)
	store := &fakeServiceDestroyStore{claim: claim}
	transport := &fakeServiceDestroyTransport{requestErr: ServiceDestroyRetryable("deployment_destroy_in_progress", errors.New("read-back pending"))}
	transport.store = store
	runner := NewServiceDestroyRunner(store, transport, Config{WorkerID: "destroy-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if store.deferred != "deployment_destroy_in_progress" || store.completed || store.failed != "" {
		t.Fatalf("in-progress destroy state=%#v", store)
	}
}

type fakeServiceDestroyStore struct {
	claim                         ServiceDestroyClaim
	started, persisted, completed bool
	deferred, failed              string
}

func (s *fakeServiceDestroyStore) ClaimServiceDestroy(context.Context, string, time.Duration) (ServiceDestroyClaim, bool, error) {
	return s.claim, true, nil
}
func (s *fakeServiceDestroyStore) PersistServiceDestroyCommand(_ context.Context, _ ServiceDestroyClaim, _ SignedServiceDestroyCommand) error {
	s.persisted = true
	return nil
}
func (s *fakeServiceDestroyStore) MarkServiceDestroyStarted(context.Context, ServiceDestroyClaim) error {
	s.started = true
	return nil
}
func (s *fakeServiceDestroyStore) CompleteServiceDestroy(context.Context, ServiceDestroyClaim, ServiceDestroyResult) error {
	s.completed = true
	return nil
}
func (s *fakeServiceDestroyStore) DeferServiceDestroy(_ context.Context, _ ServiceDestroyClaim, code string, _ time.Time) error {
	s.deferred = code
	return nil
}
func (s *fakeServiceDestroyStore) FailServiceDestroy(_ context.Context, _ ServiceDestroyClaim, code string) error {
	s.failed = code
	return nil
}

type fakeServiceDestroyTransport struct {
	store        *fakeServiceDestroyStore
	requestErr   error
	requests     int
	sawPersisted bool
}

func (t *fakeServiceDestroyTransport) BuildServiceDestroyCommand(_ ServiceDestroyCommand, _ broker.DeploymentDestroyRequest, _ cloudcontracts.ServiceDestroyApprovalV1) (SignedServiceDestroyCommand, error) {
	return SignedServiceDestroyCommand{EnvelopeJSON: "{}", PayloadJSON: "{}", PayloadSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", RequestSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", IssuedAt: time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, time.July, 15, 10, 4, 0, 0, time.UTC)}, nil
}
func (t *fakeServiceDestroyTransport) RequestServiceDestroy(_ context.Context, _ string, command ServiceDestroyCommand, signed SignedServiceDestroyCommand, request broker.DeploymentDestroyRequest, _ cloudcontracts.ServiceDestroyApprovalV1) (ServiceDestroyResult, error) {
	t.requests++
	if t.store != nil {
		t.sawPersisted = t.store.persisted
	} else {
		t.sawPersisted = true
	}
	if t.requestErr != nil {
		return ServiceDestroyResult{}, t.requestErr
	}
	return ServiceDestroyResult{Status: "verified_destroyed", DeploymentID: request.DeploymentID, InstanceID: request.InstanceID, VolumeIDs: request.VolumeIDs, NetworkInterfaceIDs: request.NetworkInterfaceIDs, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: "{}"}, nil
}

func validServiceDestroyClaim(t *testing.T, now time.Time) ServiceDestroyClaim {
	t.Helper()
	target := cloudcontracts.ServiceDestroyTargetV1{ServiceID: "service-runner-1", ServiceRevision: 2, DeploymentID: "deployment-runner-1", DeploymentRevision: 5, CloudConnectionID: "connection-runner-1", RecipeID: "recipe-runner-1", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, NetworkInterfaceIDs: []string{"eni-0123456789abcdef0"}}
	approval, err := cloudcontracts.NewServiceDestroyApprovalV1(target, "approval-runner-1", "challenge-runner-1", "device-runner-1", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(key, now)
	if err != nil {
		t.Fatal(err)
	}
	request := broker.DeploymentDestroyRequest{Schema: broker.DeploymentDestroySchema, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, NetworkInterfaceIDs: target.NetworkInterfaceIDs}
	digest, err := ServiceDestroyRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	return ServiceDestroyClaim{OutboxID: "outbox-runner-1", Kind: ServiceDestroyRequested, AggregateType: "service", AggregateID: target.ServiceID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, PlanID: "plan-runner-1", JobID: "job-runner-1", ConnectionID: target.CloudConnectionID, Region: "ap-south-1", BrokerEndpoint: "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-runner-1", LeaseToken: "lease-runner-1", ExpectedGeneration: 1, ServiceRevision: 3, DeploymentRevision: 6, Attempt: 1, Approval: approval, Request: request, Command: ServiceDestroyCommand{CommandID: "command-runner-1", ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, ConnectionID: target.CloudConnectionID, NodeKeyID: "node-runner-1", ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, RequestDigest: digest, State: "allocated"}}
}
