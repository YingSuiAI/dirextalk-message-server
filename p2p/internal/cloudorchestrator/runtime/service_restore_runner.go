package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ServiceRestoreRunner struct {
	store     ServiceRestoreStore
	transport ServiceRestoreTransport
	cfg       Config
}

func NewServiceRestoreRunner(store ServiceRestoreStore, transport ServiceRestoreTransport, cfg Config) *ServiceRestoreRunner {
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
	return &ServiceRestoreRunner{store: store, transport: transport, cfg: cfg}
}

func (runner *ServiceRestoreRunner) RunOnce(ctx context.Context) (bool, error) {
	if runner == nil || runner.store == nil {
		return false, errors.New("cloud service restore store is unavailable")
	}
	if runner.transport == nil {
		return false, errors.New("cloud service restore transport is unavailable")
	}
	if strings.TrimSpace(runner.cfg.WorkerID) == "" || runner.cfg.Lease <= 0 || runner.cfg.Lease > 5*time.Minute || runner.cfg.AttemptTimeout <= 0 || runner.cfg.AttemptTimeout >= runner.cfg.Lease {
		return false, errors.New("cloud service restore configuration is invalid")
	}
	claim, found, err := runner.store.ClaimServiceRestore(ctx, runner.cfg.WorkerID, runner.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if ValidateServiceRestoreClaim(claim) != nil {
		return true, runner.store.FailServiceRestore(ctx, claim, "invalid_service_restore_claim")
	}
	if err = runner.store.MarkServiceRestoreStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark service restore started: %w", err)
	}
	signed := SignedServiceRestoreCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		signed, err = runner.transport.BuildServiceRestoreCommand(claim.Command, claim.Request, claim.Approval)
		if err != nil {
			return true, runner.store.FailServiceRestore(ctx, claim, "invalid_service_restore_claim")
		}
		if err = runner.store.PersistServiceRestoreCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist service restore command: %w", err)
		}
		claim.Command.SignedEnvelope, claim.Command.PayloadJSON, claim.Command.PayloadSHA256, claim.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		claim.Command.IssuedAt, claim.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
	}
	attemptCtx, cancel := context.WithTimeout(ctx, runner.cfg.AttemptTimeout)
	result, requestErr := runner.transport.RequestServiceRestore(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request, claim.Approval)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, runner.store.DeferServiceRestore(ctx, claim, "service_restore_attempt_timed_out", runner.cfg.Now().Add(runner.cfg.RetryDelay))
	}
	if requestErr != nil {
		if code, retry := retryCode(requestErr); retry {
			return true, runner.store.DeferServiceRestore(ctx, claim, code, runner.cfg.Now().Add(runner.cfg.RetryDelay))
		}
		return true, runner.store.FailServiceRestore(ctx, claim, "service_restore_transport_failed")
	}
	if ValidateServiceRestoreResult(claim, signed, result) != nil {
		return true, runner.store.FailServiceRestore(ctx, claim, "invalid_service_restore_result")
	}
	if err = runner.store.CompleteServiceRestore(ctx, claim, result); err != nil {
		return true, fmt.Errorf("complete service restore: %w", err)
	}
	return true, nil
}
