package runtime

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

func TestServiceBackupRunnerPersistsExactCommandBeforeRequest(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	claim := validServiceBackupClaim(t, now)
	store := &fakeServiceBackupStore{claim: claim}
	transport := &fakeServiceBackupTransport{store: store}
	runner := NewServiceBackupRunner(store, transport, Config{WorkerID: "backup-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, e := runner.RunOnce(t.Context())
	if e != nil || !processed || !store.persisted || !store.started || !store.completed || transport.calls != 1 || transport.requestedBeforePersist {
		t.Fatalf("processed=%v err=%v store=%#v transport=%#v", processed, e, store, transport)
	}
}

func TestServiceBackupClaimRejectsRequestDrift(t *testing.T) {
	claim := validServiceBackupClaim(t, time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC))
	claim.Request.ServiceID = "service-drifted-0001"
	if ValidateServiceBackupClaim(claim) == nil {
		t.Fatal("service backup request drift was accepted")
	}
}

type fakeServiceBackupStore struct {
	claim                         ServiceBackupClaim
	persisted, started, completed bool
}

func (s *fakeServiceBackupStore) ClaimServiceBackup(context.Context, string, time.Duration) (ServiceBackupClaim, bool, error) {
	return s.claim, true, nil
}
func (s *fakeServiceBackupStore) PersistServiceBackupCommand(_ context.Context, _ ServiceBackupClaim, _ SignedServiceBackupCommand) error {
	s.persisted = true
	return nil
}
func (s *fakeServiceBackupStore) MarkServiceBackupStarted(context.Context, ServiceBackupClaim) error {
	s.started = true
	return nil
}
func (s *fakeServiceBackupStore) CompleteServiceBackup(context.Context, ServiceBackupClaim, ServiceBackupResult) error {
	s.completed = true
	return nil
}
func (*fakeServiceBackupStore) DeferServiceBackup(context.Context, ServiceBackupClaim, string, time.Time) error {
	return nil
}
func (*fakeServiceBackupStore) FailServiceBackup(context.Context, ServiceBackupClaim, string) error {
	return nil
}

type fakeServiceBackupTransport struct {
	store                  *fakeServiceBackupStore
	calls                  int
	requestedBeforePersist bool
}

func (*fakeServiceBackupTransport) BuildServiceBackupCommand(_ ServiceBackupCommand, _ broker.ServiceBackupRequest, _ cloudcontracts.ServiceBackupApprovalV1) (SignedServiceBackupCommand, error) {
	return SignedServiceBackupCommand{EnvelopeJSON: "{}", PayloadJSON: "{}", PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, 7, 15, 18, 4, 0, 0, time.UTC)}, nil
}
func (t *fakeServiceBackupTransport) RequestServiceBackup(_ context.Context, _ string, c ServiceBackupCommand, s SignedServiceBackupCommand, r broker.ServiceBackupRequest, _ cloudcontracts.ServiceBackupApprovalV1) (ServiceBackupResult, error) {
	t.calls++
	if !t.store.persisted {
		t.requestedBeforePersist = true
	}
	return ServiceBackupResult{Status: "backup_available", BackupID: r.BackupID, ServiceID: r.ServiceID, DeploymentID: r.DeploymentID, InstanceID: r.InstanceID, ImageID: "ami-0123456789abcdef0", ReceiptJSON: "{}", Snapshots: []broker.ServiceBackupSnapshot{{VolumeID: r.VolumeIDs[0], SnapshotID: "snap-0123456789abcdef0", State: "completed", Encrypted: true}}, CommandID: c.CommandID, RequestSHA256: s.RequestSHA256}, nil
}
func validServiceBackupClaim(t *testing.T, now time.Time) ServiceBackupClaim {
	t.Helper()
	target := cloudcontracts.ServiceBackupTargetV1{BackupID: "backup-runner-0001", ServiceID: "service-runner-0001", ServiceRevision: 3, DeploymentID: "deployment-runner-0001", DeploymentRevision: 6, CloudConnectionID: "connection-runner-0001", RecipeID: "recipe-runner-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstanceID: "i-0123456789abcdef0", VolumeIDs: []string{"vol-0123456789abcdef0"}, RetentionPolicy: "manual"}
	approval, e := cloudcontracts.NewServiceBackupApprovalV1(target, "approval-runner-0001", "challenge-runner-0001", "device-runner-0001", now, now.Add(5*time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	approval, e = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x33}, 32)), now)
	if e != nil {
		t.Fatal(e)
	}
	request := broker.ServiceBackupRequest{Schema: broker.ServiceBackupSchema, BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, InstanceID: target.InstanceID, VolumeIDs: target.VolumeIDs, RetentionPolicy: "manual"}
	digest, e := ServiceBackupRequestDigest(request)
	if e != nil {
		t.Fatal(e)
	}
	return ServiceBackupClaim{OutboxID: "outbox-runner-0001", Kind: ServiceBackupRequested, AggregateType: "service_backup", AggregateID: target.BackupID, BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, PlanID: "plan-runner-0001", JobID: "job-runner-0001", ConnectionID: target.CloudConnectionID, Region: "ap-south-1", BrokerEndpoint: "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-runner-0001", LeaseToken: "lease-runner-0001", ExpectedGeneration: 1, ServiceRevision: 3, DeploymentRevision: 6, Attempt: 1, Approval: approval, Request: request, Command: ServiceBackupCommand{CommandID: "command-runner-0001", BackupID: target.BackupID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, ConnectionID: target.CloudConnectionID, NodeKeyID: "node-runner-0001", ExpectedGeneration: 1, NodeCounter: 1, Attempt: 1, RequestDigest: digest, State: "allocated"}}
}
