package runtime

import (
	"context"
	"errors"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var ErrServiceMonitorConfigurationInvalid = errors.New("service monitor runner configuration is invalid")

// ServiceMonitorClaim is the immutable snapshot used to schedule one
// continuously monitored semantic-readiness generation. The generated task is
// still executed only by the closed service_readiness transport.
type ServiceMonitorClaim struct {
	LeaseToken                                            string
	ServiceID, DeploymentID, ExecutionID, ConnectionID    string
	InstanceID, ManifestDigest, InstallEvidenceDigest     string
	ArtifactDigest, SemanticExpectationDigest             string
	SemanticProbe                                         cloudcontracts.OCIServiceLoopbackProbeV1
	ServiceStatus, ResourceStatus                         string
	ServiceRevision, DeploymentRevision, WorkerLeaseEpoch int64
	Generation                                            int64
	TaskID, OutboxID                                      string
}

type ServiceMonitorStore interface {
	ClaimServiceMonitor(context.Context, string, time.Duration) (ServiceMonitorClaim, bool, error)
	ScheduleServiceMonitor(context.Context, ServiceMonitorClaim) error
	DeferServiceMonitor(context.Context, ServiceMonitorClaim, string, time.Time) error
}

type ServiceMonitorRunner struct {
	store ServiceMonitorStore
	cfg   Config
}

func NewServiceMonitorRunner(store ServiceMonitorStore, cfg Config) *ServiceMonitorRunner {
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &ServiceMonitorRunner{store: store, cfg: cfg}
}

func (r *ServiceMonitorRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil || strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.RetryDelay <= 0 {
		return false, ErrServiceMonitorConfigurationInvalid
	}
	claim, found, err := r.store.ClaimServiceMonitor(ctx, strings.TrimSpace(r.cfg.WorkerID), r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if ValidateServiceMonitorClaim(claim) != nil {
		return true, r.store.DeferServiceMonitor(ctx, claim, "invalid_service_monitor_claim", r.cfg.Now().UTC().Add(r.cfg.RetryDelay))
	}
	if err := r.store.ScheduleServiceMonitor(ctx, claim); err != nil {
		return true, err
	}
	return true, nil
}

func ValidateServiceMonitorClaim(c ServiceMonitorClaim) error {
	if c.LeaseToken == "" || !validResearchIdentifier("service_id", c.ServiceID) || !validResearchIdentifier("deployment_id", c.DeploymentID) ||
		!validResearchIdentifier("execution_id", c.ExecutionID) || !validResearchIdentifier("cloud_connection_id", c.ConnectionID) ||
		!ec2InstanceIDPattern.MatchString(c.InstanceID) || !deploymentDigestPattern.MatchString(c.ManifestDigest) ||
		!deploymentDigestPattern.MatchString(c.InstallEvidenceDigest) || !deploymentDigestPattern.MatchString(c.ArtifactDigest) ||
		c.SemanticProbe.Validate() != nil || !deploymentDigestPattern.MatchString(c.SemanticExpectationDigest) ||
		c.SemanticExpectationDigest != c.SemanticProbe.BodySHA256 || (c.ServiceStatus != "active" && c.ServiceStatus != "experimental" && c.ServiceStatus != "degraded") ||
		(c.ResourceStatus != "active" && c.ResourceStatus != "retained_tracked") || c.ServiceRevision < 1 || c.DeploymentRevision < 1 ||
		c.WorkerLeaseEpoch < 1 || c.Generation < 1 || !validResearchIdentifier("task_id", c.TaskID) || !validResearchIdentifier("outbox_id", c.OutboxID) {
		return errors.New("service monitor claim is invalid")
	}
	return nil
}
