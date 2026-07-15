package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type serviceMonitorStoreStub struct {
	claim     ServiceMonitorClaim
	found     bool
	claimErr  error
	scheduled int
	deferred  int
}

func (s *serviceMonitorStoreStub) ClaimServiceMonitor(context.Context, string, time.Duration) (ServiceMonitorClaim, bool, error) {
	return s.claim, s.found, s.claimErr
}

func (s *serviceMonitorStoreStub) ScheduleServiceMonitor(context.Context, ServiceMonitorClaim) error {
	s.scheduled++
	return nil
}

func (s *serviceMonitorStoreStub) DeferServiceMonitor(context.Context, ServiceMonitorClaim, string, time.Time) error {
	s.deferred++
	return nil
}

func TestServiceMonitorRunnerSchedulesClaimOnce(t *testing.T) {
	store := &serviceMonitorStoreStub{found: true, claim: validServiceMonitorClaimForTest()}
	runner := NewServiceMonitorRunner(store, Config{WorkerID: "monitor-runner", Lease: time.Minute})
	processed, err := runner.RunOnce(t.Context())
	if err != nil || !processed || store.scheduled != 1 || store.deferred != 0 {
		t.Fatalf("processed=%v scheduled=%d deferred=%d err=%v", processed, store.scheduled, store.deferred, err)
	}
}

func TestServiceMonitorRunnerFailsClosedForInvalidClaim(t *testing.T) {
	store := &serviceMonitorStoreStub{found: true, claim: validServiceMonitorClaimForTest()}
	store.claim.WorkerLeaseEpoch = 0
	runner := NewServiceMonitorRunner(store, Config{WorkerID: "monitor-runner", Lease: time.Minute, RetryDelay: 15 * time.Second})
	processed, err := runner.RunOnce(t.Context())
	if err != nil || !processed || store.scheduled != 0 || store.deferred != 1 {
		t.Fatalf("processed=%v scheduled=%d deferred=%d err=%v", processed, store.scheduled, store.deferred, err)
	}
}

func TestServiceMonitorRunnerRejectsInvalidConfiguration(t *testing.T) {
	runner := NewServiceMonitorRunner(&serviceMonitorStoreStub{}, Config{WorkerID: "", Lease: time.Minute})
	if _, err := runner.RunOnce(t.Context()); !errors.Is(err, ErrServiceMonitorConfigurationInvalid) {
		t.Fatalf("error=%v", err)
	}
}

func validServiceMonitorClaimForTest() ServiceMonitorClaim {
	return ServiceMonitorClaim{
		LeaseToken: "lease-monitor-1", ServiceID: "service-monitor-1", DeploymentID: "deployment-monitor-1",
		ExecutionID: "execution-monitor-1", ConnectionID: "connection-monitor-1", InstanceID: "i-0123456789abcdef0",
		ManifestDigest:            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstallEvidenceDigest:     "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ArtifactDigest:            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		SemanticExpectationDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		SemanticProbe:             cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 18080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},
		ServiceStatus:             "experimental", ResourceStatus: "active", ServiceRevision: 1, DeploymentRevision: 2,
		WorkerLeaseEpoch: 3, Generation: 1, TaskID: "service-monitor-task-1", OutboxID: "service-monitor-outbox-1",
	}
}
