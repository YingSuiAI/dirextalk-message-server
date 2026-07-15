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

func TestServiceRestoreRunnerPersistsExactCommandBeforeAWSRequest(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	claim := validServiceRestoreClaim(t, now)
	store := &fakeServiceRestoreStore{claim: claim}
	transport := &fakeServiceRestoreTransport{store: store}
	runner := NewServiceRestoreRunner(store, transport, Config{WorkerID: "restore-runner", Lease: 2 * time.Minute, AttemptTimeout: time.Minute, RetryDelay: time.Minute, Now: func() time.Time { return now }})
	processed, err := runner.RunOnce(t.Context())
	if err != nil || !processed || !store.persisted || !store.started || !store.completed || transport.calls != 1 || transport.requestedBeforePersist {
		t.Fatalf("processed=%v err=%v store=%#v transport=%#v", processed, err, store, transport)
	}
}

type fakeServiceRestoreStore struct {
	claim                         ServiceRestoreClaim
	persisted, started, completed bool
}

func (store *fakeServiceRestoreStore) ClaimServiceRestore(context.Context, string, time.Duration) (ServiceRestoreClaim, bool, error) {
	return store.claim, true, nil
}
func (store *fakeServiceRestoreStore) PersistServiceRestoreCommand(context.Context, ServiceRestoreClaim, SignedServiceRestoreCommand) error {
	store.persisted = true
	return nil
}
func (store *fakeServiceRestoreStore) MarkServiceRestoreStarted(context.Context, ServiceRestoreClaim) error {
	store.started = true
	return nil
}
func (store *fakeServiceRestoreStore) CompleteServiceRestore(context.Context, ServiceRestoreClaim, ServiceRestoreResult) error {
	store.completed = true
	return nil
}
func (*fakeServiceRestoreStore) DeferServiceRestore(context.Context, ServiceRestoreClaim, string, time.Time) error {
	return nil
}
func (*fakeServiceRestoreStore) FailServiceRestore(context.Context, ServiceRestoreClaim, string) error {
	return nil
}

type fakeServiceRestoreTransport struct {
	store                  *fakeServiceRestoreStore
	calls                  int
	requestedBeforePersist bool
}

func (*fakeServiceRestoreTransport) BuildServiceRestoreCommand(ServiceRestoreCommand, broker.ServiceRestoreRequest, cloudcontracts.ServiceRestoreApprovalV1) (SignedServiceRestoreCommand, error) {
	return SignedServiceRestoreCommand{EnvelopeJSON: "{}", PayloadJSON: "{}", PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, 7, 15, 18, 4, 0, 0, time.UTC)}, nil
}
func (transport *fakeServiceRestoreTransport) RequestServiceRestore(_ context.Context, _ string, command ServiceRestoreCommand, signed SignedServiceRestoreCommand, request broker.ServiceRestoreRequest, _ cloudcontracts.ServiceRestoreApprovalV1) (ServiceRestoreResult, error) {
	transport.calls++
	if !transport.store.persisted {
		transport.requestedBeforePersist = true
	}
	swap := request.VolumeSwaps[0]
	return ServiceRestoreResult{Status: "aws_restore_applied", Evidence: broker.ServiceRestoreAWSEvidence{RestoreID: request.RestoreID, ServiceID: request.ServiceID, DeploymentID: request.DeploymentID, BackupID: request.BackupID, InstanceID: request.InstanceID, Region: request.Region, AvailabilityZone: request.AvailabilityZone, Outcome: "restored", InstanceState: "running", Replacements: []broker.ServiceRestoreReplacementVolume{{OriginalVolumeID: swap.OriginalVolumeID, ReplacementVolumeID: "vol-0fedcba9876543210", SnapshotID: swap.SnapshotID, DeviceName: swap.DeviceName, State: "attached_current", Encrypted: true, DeleteOnTermination: swap.DeleteOnTermination}}}, CommandID: command.CommandID, RequestSHA256: signed.RequestSHA256, ReceiptJSON: "{}"}, nil
}

func validServiceRestoreClaim(t *testing.T, now time.Time) ServiceRestoreClaim {
	t.Helper()
	target := cloudcontracts.ServiceRestoreTargetV1{
		RestoreID: "restore-runner-0001", ServiceID: "service-runner-0001", ServiceRevision: 3,
		DeploymentID: "deployment-runner-0001", DeploymentRevision: 6, CloudConnectionID: "connection-runner-0001",
		BackupID: "backup-runner-0001", BackupRevision: 2, RecipeID: "recipe-runner-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", Region: "ap-south-1", AvailabilityZone: "ap-south-1a", RestoreMode: cloudcontracts.ServiceRestoreModeInPlace, DowntimeRequired: true,
		OriginalVolumeRetention: cloudcontracts.ServiceRestoreRetentionManual, FailurePolicy: cloudcontracts.ServiceRestoreFailureReattachOriginal,
		QuoteID: "quote-runner-0001", Currency: "USD", EstimatedHourlyMinor: 1, EstimatedThirtyDayMinor: 640, QuoteValidUntil: now.Add(15 * time.Minute),
		VolumeSwaps: []cloudcontracts.ServiceRestoreVolumeSwapV1{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}},
	}
	approval, err := cloudcontracts.NewServiceRestoreApprovalV1(target, "approval-runner-0001", "challenge-runner-0001", "device-runner-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	approval, err = approval.Sign(ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x33}, 32)), now)
	if err != nil {
		t.Fatal(err)
	}
	request := broker.ServiceRestoreRequest{Schema: broker.ServiceRestoreSchema, RestoreID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, InstanceID: target.InstanceID, Region: target.Region, AvailabilityZone: target.AvailabilityZone, RestoreMode: target.RestoreMode, DowntimeRequired: true, OriginalVolumeRetention: target.OriginalVolumeRetention, FailurePolicy: target.FailurePolicy, QuoteID: target.QuoteID, QuoteValidUntil: target.QuoteValidUntil.Format("2006-01-02T15:04:05.000Z"), VolumeSwaps: []broker.ServiceRestoreVolumeSwap{{OriginalVolumeID: target.VolumeSwaps[0].OriginalVolumeID, SnapshotID: target.VolumeSwaps[0].SnapshotID, DeviceName: target.VolumeSwaps[0].DeviceName, VolumeType: target.VolumeSwaps[0].VolumeType, SizeGiB: target.VolumeSwaps[0].SizeGiB, IOPS: target.VolumeSwaps[0].IOPS, ThroughputMiB: target.VolumeSwaps[0].ThroughputMiB, Encrypted: true, DeleteOnTermination: true}}}
	digest, err := ServiceRestoreRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	return ServiceRestoreClaim{OutboxID: "outbox-runner-0001", Kind: ServiceRestoreRequested, AggregateType: "service_restore", AggregateID: target.RestoreID, RestoreID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, BackupID: target.BackupID, PlanID: "plan-runner-0001", JobID: "job-runner-0001", ConnectionID: target.CloudConnectionID, Region: target.Region, BrokerEndpoint: "https://a1b2c3d4e5.execute-api.ap-south-1.amazonaws.com/prod/v2/commands", NodeKeyID: "node-runner-0001", LeaseToken: "lease-runner-0001", ExpectedGeneration: 2, ServiceRevision: 3, DeploymentRevision: 6, BackupRevision: 2, Attempt: 1, Approval: approval, Request: request, Command: ServiceRestoreCommand{CommandID: "command-runner-0001", RestoreID: target.RestoreID, ServiceID: target.ServiceID, DeploymentID: target.DeploymentID, ConnectionID: target.CloudConnectionID, NodeKeyID: "node-runner-0001", ExpectedGeneration: 2, NodeCounter: 11, Attempt: 1, RequestDigest: digest, State: "allocated"}}
}
