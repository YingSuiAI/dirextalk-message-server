package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

type ServiceRestorePlanRunner struct {
	store     ServiceRestorePlanStore
	transport ServiceRestorePlanTransport
	cfg       Config
}

func NewServiceRestorePlanRunner(s ServiceRestorePlanStore, t ServiceRestorePlanTransport, c Config) *ServiceRestorePlanRunner {
	if c.Lease <= 0 {
		c.Lease = 2 * time.Minute
	}
	if c.AttemptTimeout <= 0 {
		c.AttemptTimeout = c.Lease / 2
	}
	if c.RetryDelay <= 0 {
		c.RetryDelay = time.Minute
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	return &ServiceRestorePlanRunner{s, t, c}
}
func (r *ServiceRestorePlanRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud service restore plan store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud service restore plan transport is unavailable")
	}
	if strings.TrimSpace(r.cfg.WorkerID) == "" || r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud service restore plan configuration is invalid")
	}
	c, found, e := r.store.ClaimServiceRestorePlan(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if e != nil || !found {
		return found, e
	}
	if ValidateServiceRestorePlanClaim(c) != nil {
		return true, r.store.FailServiceRestorePlan(ctx, c, "invalid_service_restore_plan_claim")
	}
	if e = r.store.MarkServiceRestorePlanStarted(ctx, c); e != nil {
		return true, fmt.Errorf("mark service restore plan started: %w", e)
	}
	s := SignedServiceRestorePlanCommand{EnvelopeJSON: c.Command.SignedEnvelope, PayloadJSON: c.Command.PayloadJSON, PayloadSHA256: c.Command.PayloadSHA256, RequestSHA256: c.Command.RequestSHA256, IssuedAt: c.Command.IssuedAt, ExpiresAt: c.Command.ExpiresAt}
	if s.EnvelopeJSON == "" {
		s, e = r.transport.BuildServiceRestorePlanCommand(c.Command, c.Request)
		if e != nil {
			return true, r.store.FailServiceRestorePlan(ctx, c, "invalid_service_restore_plan_claim")
		}
		if e = r.store.PersistServiceRestorePlanCommand(ctx, c, s); e != nil {
			return true, fmt.Errorf("persist service restore plan command: %w", e)
		}
		c.Command.SignedEnvelope, c.Command.PayloadJSON, c.Command.PayloadSHA256, c.Command.RequestSHA256 = s.EnvelopeJSON, s.PayloadJSON, s.PayloadSHA256, s.RequestSHA256
		c.Command.IssuedAt, c.Command.ExpiresAt = s.IssuedAt, s.ExpiresAt
	}
	attempt, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, requestErr := r.transport.RequestServiceRestorePlan(attempt, c.BrokerEndpoint, c.Command, s, c.Request)
	attemptErr := attempt.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferServiceRestorePlan(ctx, c, "service_restore_plan_attempt_timed_out", r.cfg.Now().Add(r.cfg.RetryDelay))
	}
	if requestErr != nil {
		if quoteCommandExpired(requestErr) {
			return true, r.store.ExpireServiceRestorePlanCommand(ctx, c)
		}
		if code, retry := retryCode(requestErr); retry {
			return true, r.store.DeferServiceRestorePlan(ctx, c, code, r.cfg.Now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailServiceRestorePlan(ctx, c, "service_restore_plan_transport_failed")
	}
	if ValidateServiceRestorePlanResult(c, s, result) != nil {
		return true, r.store.FailServiceRestorePlan(ctx, c, "invalid_service_restore_plan_result")
	}
	if e = r.store.CompleteServiceRestorePlan(ctx, c, result); e != nil {
		return true, fmt.Errorf("complete service restore plan: %w", e)
	}
	return true, nil
}
