package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ExecutionProbeRunner is the only runtime path that connects a sealed,
// digest-only execution_probe task to the Connection Stack. It cannot execute
// a Recipe, run a shell, create EC2, access a Worker bearer, or turn a task
// transport result into service readiness.
type ExecutionProbeRunner struct {
	store     ExecutionProbeStore
	transport ExecutionProbeTransport
	cfg       Config
}

func NewExecutionProbeRunner(store ExecutionProbeStore, transport ExecutionProbeTransport, cfg Config) *ExecutionProbeRunner {
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
	return &ExecutionProbeRunner{store: store, transport: transport, cfg: cfg}
}

func (r *ExecutionProbeRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud execution probe store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud execution probe transport is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud execution probe timing configuration is invalid")
	}
	claim, found, err := r.store.ClaimExecutionProbe(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err := validateExecutionProbeClaim(claim); err != nil {
		return true, r.store.FailExecutionProbe(ctx, claim, executionProbeInvalidClaimCode(claim.Phase))
	}
	if err := r.store.MarkExecutionProbeStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark cloud execution probe started: %w", err)
	}
	signed := SignedExecutionProbeCommand{
		EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON,
		PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256,
		IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt,
	}
	if signed.EnvelopeJSON == "" {
		signed, err = r.build(claim)
		if err != nil {
			return true, r.store.FailExecutionProbe(ctx, claim, executionProbeInvalidClaimCode(claim.Phase))
		}
		if err := r.store.PersistExecutionProbeCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist cloud execution probe command: %w", err)
		}
		claim.Command.PayloadJSON = signed.PayloadJSON
		claim.Command.PayloadSHA256 = signed.PayloadSHA256
		claim.Command.RequestSHA256 = signed.RequestSHA256
		claim.Command.SignedEnvelope = signed.EnvelopeJSON
		claim.Command.IssuedAt = signed.IssuedAt
		claim.Command.ExpiresAt = signed.ExpiresAt
	}
	if err := validateSignedExecutionProbeCommand(claim.Command, signed); err != nil {
		return true, r.store.FailExecutionProbe(ctx, claim, executionProbeInvalidClaimCode(claim.Phase))
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	result, err := r.request(attemptCtx, claim, signed)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		// Preserve the lease and exact envelope for a later process to replay.
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferExecutionProbe(ctx, claim, "execution_probe_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if executionProbeCommandExpired(err) {
			return true, r.store.ExpireExecutionProbeCommand(ctx, claim)
		}
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferExecutionProbe(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.DeferExecutionProbe(ctx, claim, executionProbeRetryCode(claim.Phase), r.now().Add(r.cfg.RetryDelay))
	}
	if err := ValidateExecutionProbeResult(claim, signed, result, r.now()); err != nil {
		return true, r.store.DeferExecutionProbe(ctx, claim, executionProbeInvalidResultCode(claim.Phase), r.now().Add(r.cfg.RetryDelay))
	}
	if err := r.store.CommitExecutionProbe(ctx, claim, result); err != nil {
		return true, fmt.Errorf("commit cloud execution probe: %w", err)
	}
	return true, nil
}

func (r *ExecutionProbeRunner) build(claim ExecutionProbeClaim) (SignedExecutionProbeCommand, error) {
	switch claim.Phase {
	case ExecutionProbePhaseIssue:
		return r.transport.BuildExecutionProbeIssueCommand(claim.Command, claim.IssueRequest, r.now())
	case ExecutionProbePhaseObserve:
		return r.transport.BuildExecutionProbeObserveCommand(claim.Command, claim.ObserveRequest, r.now())
	default:
		return SignedExecutionProbeCommand{}, errors.New("execution probe phase is invalid")
	}
}

func (r *ExecutionProbeRunner) request(ctx context.Context, claim ExecutionProbeClaim, signed SignedExecutionProbeCommand) (ExecutionProbeTaskResult, error) {
	switch claim.Phase {
	case ExecutionProbePhaseIssue:
		return r.transport.RequestExecutionProbeIssue(ctx, claim.BrokerEndpoint, claim.Command, signed, claim.IssueRequest)
	case ExecutionProbePhaseObserve:
		return r.transport.RequestExecutionProbeObserve(ctx, claim.BrokerEndpoint, claim.Command, signed, claim.ObserveRequest)
	default:
		return ExecutionProbeTaskResult{}, errors.New("execution probe phase is invalid")
	}
}

func (r *ExecutionProbeRunner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
