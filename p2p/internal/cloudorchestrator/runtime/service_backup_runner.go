package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ServiceBackupRunner struct {
	store     ServiceBackupStore
	transport ServiceBackupTransport
	cfg       Config
}

func NewServiceBackupRunner(store ServiceBackupStore, transport ServiceBackupTransport, cfg Config) *ServiceBackupRunner {
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
	return &ServiceBackupRunner{store: store, transport: transport, cfg: cfg}
}
func (r *ServiceBackupRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud service backup store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud service backup transport is unavailable")
	}
	if strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud service backup configuration is invalid")
	}
	claim, found, e := r.store.ClaimServiceBackup(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if e != nil || !found {
		return found, e
	}
	if ValidateServiceBackupClaim(claim) != nil {
		return true, r.store.FailServiceBackup(ctx, claim, "invalid_service_backup_claim")
	}
	if e = r.store.MarkServiceBackupStarted(ctx, claim); e != nil {
		return true, fmt.Errorf("mark service backup started: %w", e)
	}
	signed := SignedServiceBackupCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		signed, e = r.transport.BuildServiceBackupCommand(claim.Command, claim.Request, claim.Approval)
		if e != nil {
			return true, r.store.FailServiceBackup(ctx, claim, "invalid_service_backup_claim")
		}
		if e = r.store.PersistServiceBackupCommand(ctx, claim, signed); e != nil {
			return true, fmt.Errorf("persist service backup command: %w", e)
		}
		claim.Command.SignedEnvelope, claim.Command.PayloadJSON, claim.Command.PayloadSHA256, claim.Command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
		claim.Command.IssuedAt, claim.Command.ExpiresAt = signed.IssuedAt, signed.ExpiresAt
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, requestErr := r.transport.RequestServiceBackup(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request, claim.Approval)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferServiceBackup(ctx, claim, "service_backup_attempt_timed_out", r.cfg.Now().Add(r.cfg.RetryDelay))
	}
	if requestErr != nil {
		if code, retry := retryCode(requestErr); retry {
			return true, r.store.DeferServiceBackup(ctx, claim, code, r.cfg.Now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailServiceBackup(ctx, claim, "service_backup_transport_failed")
	}
	if ValidateServiceBackupResult(claim, signed, result) != nil {
		return true, r.store.FailServiceBackup(ctx, claim, "invalid_service_backup_result")
	}
	if e = r.store.CompleteServiceBackup(ctx, claim, result); e != nil {
		return true, fmt.Errorf("complete service backup: %w", e)
	}
	return true, nil
}
