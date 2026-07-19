package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ServiceDestroyRunner struct {
	store     ServiceDestroyStore
	transport ServiceDestroyTransport
	cfg       Config
}

func NewServiceDestroyRunner(store ServiceDestroyStore, transport ServiceDestroyTransport, cfg Config) *ServiceDestroyRunner {
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
	return &ServiceDestroyRunner{store: store, transport: transport, cfg: cfg}
}

func (r *ServiceDestroyRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud service destroy store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud service destroy transport is unavailable")
	}
	if strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud service destroy configuration is invalid")
	}
	claim, found, err := r.store.ClaimServiceDestroy(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if ValidateServiceDestroyClaim(claim) != nil {
		return true, r.store.FailServiceDestroy(ctx, claim, "invalid_service_destroy_claim")
	}
	if err = r.store.MarkServiceDestroyStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark service destroy started: %w", err)
	}
	signed := SignedServiceDestroyCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildServiceDestroyCommand(claim.Command, claim.Request, claim.Approval)
		if err != nil {
			return true, r.store.FailServiceDestroy(ctx, claim, "invalid_service_destroy_claim")
		}
		if err = r.store.PersistServiceDestroyCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist service destroy command: %w", err)
		}
		claim.Command.SignedEnvelope, claim.Command.PayloadJSON, claim.Command.PayloadSHA256, claim.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		claim.Command.IssuedAt, claim.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
	}
	if ValidateSignedServiceDestroyCommand(signed) != nil {
		return true, r.store.FailServiceDestroy(ctx, claim, "invalid_service_destroy_claim")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, requestErr := r.transport.RequestServiceDestroy(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request, claim.Approval)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferServiceDestroy(ctx, claim, "service_destroy_attempt_timed_out", r.cfg.Now().Add(r.cfg.RetryDelay))
	}
	if requestErr != nil {
		if code, retry := retryCode(requestErr); retry {
			return true, r.store.DeferServiceDestroy(ctx, claim, code, r.cfg.Now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailServiceDestroy(ctx, claim, "service_destroy_transport_failed")
	}
	if ValidateServiceDestroyResult(claim, signed, result) != nil {
		return true, r.store.FailServiceDestroy(ctx, claim, "invalid_service_destroy_result")
	}
	if err = r.store.CompleteServiceDestroy(ctx, claim, result); err != nil {
		return true, fmt.Errorf("complete service destroy: %w", err)
	}
	return true, nil
}
