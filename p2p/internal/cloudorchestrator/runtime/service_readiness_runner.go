package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ServiceReadinessRunner struct {
	store     ServiceReadinessStore
	transport ServiceReadinessTransport
	cfg       Config
}

func NewServiceReadinessRunner(store ServiceReadinessStore, transport ServiceReadinessTransport, cfg Config) *ServiceReadinessRunner {
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
	return &ServiceReadinessRunner{store: store, transport: transport, cfg: cfg}
}

func (r *ServiceReadinessRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil || r.transport == nil {
		return false, errors.New("service readiness runner is unavailable")
	}
	if strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("service readiness runner configuration is invalid")
	}
	claim, found, err := r.store.ClaimServiceReadiness(ctx, strings.TrimSpace(r.cfg.WorkerID), r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if ValidateServiceReadinessClaim(claim) != nil {
		return true, r.store.FailServiceReadiness(ctx, claim, "invalid_service_readiness_claim")
	}
	if err = r.store.MarkServiceReadinessStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark service readiness started: %w", err)
	}
	signed := SignedServiceReadinessCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		if claim.Phase == ServiceReadinessPhaseIssue {
			signed, err = r.transport.BuildServiceReadinessIssueCommand(claim.Command, claim.IssueRequest, r.now())
		} else {
			signed, err = r.transport.BuildServiceReadinessObserveCommand(claim.Command, claim.ObserveRequest, r.now())
		}
		if err != nil {
			return true, r.store.FailServiceReadiness(ctx, claim, "invalid_service_readiness_command")
		}
		if err = r.store.PersistServiceReadinessCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist service readiness command: %w", err)
		}
	}
	if ValidateSignedServiceReadinessCommand(signed) != nil {
		return true, r.store.FailServiceReadiness(ctx, claim, "invalid_service_readiness_command")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	var result ServiceReadinessResult
	if claim.Phase == ServiceReadinessPhaseIssue {
		result, err = r.transport.RequestServiceReadinessIssue(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.IssueRequest)
	} else {
		result, err = r.transport.RequestServiceReadinessObserve(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.ObserveRequest)
	}
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferServiceReadiness(ctx, claim, "service_readiness_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if serviceReadinessCommandExpired(err) {
			return true, r.store.ExpireServiceReadinessCommand(ctx, claim)
		}
		return true, r.store.DeferServiceReadiness(ctx, claim, "service_readiness_transport_failed", r.now().Add(r.cfg.RetryDelay))
	}
	if ValidateServiceReadinessResult(claim, result, r.now()) != nil {
		return true, r.store.DeferServiceReadiness(ctx, claim, "invalid_service_readiness_result", r.now().Add(r.cfg.RetryDelay))
	}
	if err = r.store.CommitServiceReadiness(ctx, claim, result); err != nil {
		return true, fmt.Errorf("commit service readiness: %w", err)
	}
	return true, nil
}

func (r *ServiceReadinessRunner) now() time.Time { return r.cfg.Now().UTC() }
