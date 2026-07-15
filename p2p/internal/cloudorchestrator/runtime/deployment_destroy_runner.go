package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type DeploymentDestroyRunner struct {
	store     DeploymentDestroyStore
	transport DeploymentDestroyTransport
	cfg       Config
}

func NewDeploymentDestroyRunner(store DeploymentDestroyStore, transport DeploymentDestroyTransport, cfg Config) *DeploymentDestroyRunner {
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
	return &DeploymentDestroyRunner{store: store, transport: transport, cfg: cfg}
}

func (r *DeploymentDestroyRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud deployment destroy store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud deployment destroy transport is unavailable")
	}
	if strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud deployment destroy configuration is invalid")
	}
	claim, found, err := r.store.ClaimDeploymentDestroy(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if ValidateDeploymentDestroyClaim(claim) != nil {
		return true, r.store.FailDeploymentDestroy(ctx, claim, "invalid_deployment_destroy_claim")
	}
	if err = r.store.MarkDeploymentDestroyStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark deployment destroy started: %w", err)
	}
	signed := SignedServiceDestroyCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildDeploymentDestroyCommand(claim.Command, claim.Request, claim.Approval)
		if err != nil {
			return true, r.store.FailDeploymentDestroy(ctx, claim, "invalid_deployment_destroy_claim")
		}
		if err = r.store.PersistDeploymentDestroyCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist deployment destroy command: %w", err)
		}
		claim.Command.SignedEnvelope, claim.Command.PayloadJSON, claim.Command.PayloadSHA256, claim.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		claim.Command.IssuedAt, claim.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
	}
	if ValidateSignedServiceDestroyCommand(signed) != nil {
		return true, r.store.FailDeploymentDestroy(ctx, claim, "invalid_deployment_destroy_claim")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, requestErr := r.transport.RequestDeploymentDestroy(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request, claim.Approval)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferDeploymentDestroy(ctx, claim, "deployment_destroy_attempt_timed_out", r.cfg.Now().Add(r.cfg.RetryDelay))
	}
	if requestErr != nil {
		if code, retry := retryCode(requestErr); retry {
			return true, r.store.DeferDeploymentDestroy(ctx, claim, code, r.cfg.Now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailDeploymentDestroy(ctx, claim, "deployment_destroy_transport_failed")
	}
	if ValidateDeploymentDestroyResult(claim, signed, result) != nil {
		return true, r.store.FailDeploymentDestroy(ctx, claim, "invalid_deployment_destroy_result")
	}
	if err = r.store.CompleteDeploymentDestroy(ctx, claim, result); err != nil {
		return true, fmt.Errorf("complete deployment destroy: %w", err)
	}
	return true, nil
}
