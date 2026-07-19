package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	invalidDeploymentProvisionClaimCode  = "invalid_deployment_provision_claim"
	invalidDeploymentProvisionResultCode = "invalid_deployment_provision_result"
	deploymentProvisionTransportCode     = "deployment_provision_transport_failed"
)

// DeploymentProvisionRunner is the only Orchestrator path that can submit a
// typed deployment.create command. It deliberately knows neither AWS SDK
// calls nor Worker credentials; the user-owned Connection Stack owns the
// cloud mutation and Worker bootstrap session.
type DeploymentProvisionRunner struct {
	store     DeploymentProvisionStore
	transport DeploymentProvisionTransport
	cfg       Config
}

func NewDeploymentProvisionRunner(store DeploymentProvisionStore, transport DeploymentProvisionTransport, cfg Config) *DeploymentProvisionRunner {
	if cfg.Lease <= 0 {
		cfg.Lease = 2 * time.Minute
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = cfg.Lease / 2
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = time.Minute
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &DeploymentProvisionRunner{store: store, transport: transport, cfg: cfg}
}

// RunOnce claims at most one approved provision outbox. The signed envelope
// is durably persisted before the first HTTP request and is replayed exactly
// after disconnects or response loss. Only the Broker's explicit
// expired_command response permits retiring it and allocating a later command.
func (r *DeploymentProvisionRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud deployment provision store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud deployment provision transport is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud deployment provision timing configuration is invalid")
	}
	claim, found, err := r.store.ClaimDeploymentProvision(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err := validateDeploymentProvisionClaim(claim); err != nil {
		return true, r.store.FailDeploymentProvision(ctx, claim, invalidDeploymentProvisionClaimCode)
	}
	expiryCode := deploymentProvisionExpiryCode(claim, r.now())
	if expiryCode != "" {
		// A quote or device approval can expire while the Orchestrator is
		// disconnected or waiting behind another lease. Never sign or send a
		// billable request from either expired user-approved boundary.
		return true, r.store.FailDeploymentProvision(ctx, claim, expiryCode)
	}
	if err := r.store.MarkDeploymentProvisionStarted(ctx, claim); err != nil {
		// Do not send a command if the durable job transition was not fenced.
		return true, fmt.Errorf("mark cloud deployment provision started: %w", err)
	}
	signed := SignedDeploymentCreateCommand{
		EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON,
		PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256,
		IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt,
	}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildDeploymentCreateCommand(claim.Command, claim.Request, claim.ApprovalProofJSON, claim.QuoteValidUntil)
		if err != nil {
			if code, knownNoCreate := DeploymentProvisionPlanExpiryCode(err); knownNoCreate {
				return true, r.store.FailDeploymentProvision(ctx, claim, code)
			}
			return true, r.store.FailDeploymentProvision(ctx, claim, invalidDeploymentProvisionClaimCode)
		}
		if err := r.store.PersistDeploymentCreateCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist cloud deployment create command: %w", err)
		}
		claim.Command.PayloadJSON = signed.PayloadJSON
		claim.Command.PayloadSHA256 = signed.PayloadSHA256
		claim.Command.RequestSHA256 = signed.RequestSHA256
		claim.Command.SignedEnvelope = signed.EnvelopeJSON
		claim.Command.IssuedAt = signed.IssuedAt
		claim.Command.ExpiresAt = signed.ExpiresAt
	}
	if err := validateSignedDeploymentCreateCommand(claim.Command, signed); err != nil {
		return true, r.store.FailDeploymentProvision(ctx, claim, invalidDeploymentProvisionClaimCode)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, err := r.transport.RequestDeploymentCreate(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		// Shutdown deliberately leaves the lease unsettled. The next runner will
		// re-read and replay the exact persisted command after lease expiry.
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferDeploymentProvision(ctx, claim, "deployment_provision_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if code, knownNoCreate := DeploymentProvisionPlanExpiryCode(err); knownNoCreate {
			return true, r.store.FailDeploymentProvision(ctx, claim, code)
		}
		if deploymentCreateCommandExpired(err) {
			return true, r.store.ExpireDeploymentCreateCommand(ctx, claim)
		}
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferDeploymentProvision(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailDeploymentProvision(ctx, claim, deploymentProvisionTransportCode)
	}
	if err := ValidateBrokerDeployment(claim, signed, result); err != nil {
		return true, r.store.FailDeploymentProvision(ctx, claim, invalidDeploymentProvisionResultCode)
	}
	if err := r.store.CommitDeploymentProvision(ctx, claim, result); err != nil {
		return true, fmt.Errorf("commit cloud deployment provision: %w", err)
	}
	return true, nil
}

func deploymentProvisionExpiryCode(claim DeploymentProvisionClaim, now time.Time) string {
	if !claim.QuoteValidUntil.After(now) {
		return DeploymentProvisionQuoteExpired
	}
	if !claim.ApprovalValidUntil.After(now) {
		return DeploymentProvisionApprovalExpired
	}
	return ""
}

func (r *DeploymentProvisionRunner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
