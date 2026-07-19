package runtime

import (
	"context"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
	"strings"
	"testing"
	"time"
)

func TestServiceRestorePlanRunnerPersistsEnvelopeBeforeRequest(t *testing.T) {
	now := time.Date(2026, 7, 15, 22, 0, 0, 0, time.UTC)
	claim := restorePlanRunnerClaim(t)
	store := &restorePlanRunnerStore{claim: claim}
	transport := &restorePlanRunnerTransport{store: store}
	runner := NewServiceRestorePlanRunner(store, transport, Config{WorkerID: "restore-plan-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	ran, err := runner.RunOnce(t.Context())
	if err != nil || !ran || !store.persisted || !transport.requested || !store.completed {
		t.Fatalf("ran=%v err=%v persisted=%v requested=%v completed=%v", ran, err, store.persisted, transport.requested, store.completed)
	}
	if transport.requestBeforePersist {
		t.Fatal("broker request preceded durable envelope")
	}
}

type restorePlanRunnerStore struct {
	claim                ServiceRestorePlanClaim
	persisted, completed bool
}

func (s *restorePlanRunnerStore) ClaimServiceRestorePlan(context.Context, string, time.Duration) (ServiceRestorePlanClaim, bool, error) {
	return s.claim, true, nil
}
func (s *restorePlanRunnerStore) PersistServiceRestorePlanCommand(_ context.Context, _ ServiceRestorePlanClaim, _ SignedServiceRestorePlanCommand) error {
	s.persisted = true
	return nil
}
func (*restorePlanRunnerStore) MarkServiceRestorePlanStarted(context.Context, ServiceRestorePlanClaim) error {
	return nil
}
func (s *restorePlanRunnerStore) CompleteServiceRestorePlan(context.Context, ServiceRestorePlanClaim, ServiceRestorePlanResult) error {
	s.completed = true
	return nil
}
func (*restorePlanRunnerStore) DeferServiceRestorePlan(context.Context, ServiceRestorePlanClaim, string, time.Time) error {
	return nil
}
func (*restorePlanRunnerStore) ExpireServiceRestorePlanCommand(context.Context, ServiceRestorePlanClaim) error {
	return nil
}
func (*restorePlanRunnerStore) FailServiceRestorePlan(context.Context, ServiceRestorePlanClaim, string) error {
	return nil
}

type restorePlanRunnerTransport struct {
	store                           *restorePlanRunnerStore
	requested, requestBeforePersist bool
}

func (*restorePlanRunnerTransport) BuildServiceRestorePlanCommand(ServiceRestorePlanCommand, broker.ServiceRestorePlanRequest) (SignedServiceRestorePlanCommand, error) {
	return SignedServiceRestorePlanCommand{EnvelopeJSON: "{}", PayloadJSON: "{}", PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: time.Date(2026, 7, 15, 22, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, 7, 15, 22, 4, 0, 0, time.UTC)}, nil
}
func (t *restorePlanRunnerTransport) RequestServiceRestorePlan(_ context.Context, _ string, c ServiceRestorePlanCommand, s SignedServiceRestorePlanCommand, r broker.ServiceRestorePlanRequest) (ServiceRestorePlanResult, error) {
	t.requested = true
	if t.store != nil && !t.store.persisted {
		t.requestBeforePersist = true
	}
	return ServiceRestorePlanResult{Status: "restore_plan_ready", Plan: broker.ServiceRestorePlan{RestorePlanID: r.RestorePlanID, ServiceID: r.ServiceID, DeploymentID: r.DeploymentID, BackupID: r.BackupID, InstanceID: r.InstanceID, Region: r.Region}, CommandID: c.CommandID, RequestSHA256: s.RequestSHA256, ReceiptJSON: "{}"}, nil
}
func restorePlanRunnerClaim(t *testing.T) ServiceRestorePlanClaim {
	t.Helper()
	r := broker.ServiceRestorePlanRequest{Schema: broker.ServiceRestorePlanSchema, RestorePlanID: "restore-plan-runner-0001", ServiceID: "service-runner-0001", DeploymentID: "deployment-runner-0001", BackupID: "backup-runner-0001", InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", ImageID: "ami-0123456789abcdef0", SnapshotRefs: []broker.ServiceRestoreSnapshotRef{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0"}}}
	d, e := ServiceRestorePlanRequestDigest(r)
	if e != nil {
		t.Fatal(e)
	}
	return ServiceRestorePlanClaim{OutboxID: "outbox-runner-0001", Kind: ServiceRestorePlanRequested, AggregateType: "service_restore_plan", AggregateID: r.RestorePlanID, RestorePlanID: r.RestorePlanID, ServiceID: r.ServiceID, DeploymentID: r.DeploymentID, BackupID: r.BackupID, ConnectionID: "connection-runner-0001", Region: r.Region, BrokerEndpoint: "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-runner-0001", LeaseToken: "lease-runner-0001", JobID: "job-runner-0001", PlanID: "plan-runner-0001", ExpectedGeneration: 1, ServiceRevision: 2, DeploymentRevision: 5, BackupRevision: 2, Attempt: 1, Request: r, Command: ServiceRestorePlanCommand{CommandID: "command-runner-0001", RestorePlanID: r.RestorePlanID, ServiceID: r.ServiceID, DeploymentID: r.DeploymentID, BackupID: r.BackupID, ConnectionID: "connection-runner-0001", NodeKeyID: "node-runner-0001", ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, RequestDigest: d, State: "allocated"}}
}
