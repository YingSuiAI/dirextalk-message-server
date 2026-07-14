package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	invalidWorkerBootstrapObservationClaimCode  = "invalid_worker_bootstrap_observation_claim"
	invalidWorkerBootstrapObservationResultCode = "invalid_worker_bootstrap_observation_result"
	workerBootstrapObservationTransportCode     = "worker_bootstrap_observation_transport_failed"
)

// WorkerBootstrapObservationRunner polls only the closed, signed
// deployment.observe command after an EC2 create receipt exists. It cannot
// create or destroy cloud resources and never receives Worker credentials.
type WorkerBootstrapObservationRunner struct {
	store     WorkerBootstrapObservationStore
	transport WorkerBootstrapObservationTransport
	cfg       Config
}

func NewWorkerBootstrapObservationRunner(store WorkerBootstrapObservationStore, transport WorkerBootstrapObservationTransport, cfg Config) *WorkerBootstrapObservationRunner {
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
	return &WorkerBootstrapObservationRunner{store: store, transport: transport, cfg: cfg}
}

func (r *WorkerBootstrapObservationRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud worker bootstrap observation store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud worker bootstrap observation transport is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud worker bootstrap observation timing configuration is invalid")
	}
	claim, found, err := r.store.ClaimWorkerBootstrapObservation(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err := validateWorkerBootstrapObservationClaim(claim); err != nil {
		return true, r.store.FailWorkerBootstrapObservation(ctx, claim, invalidWorkerBootstrapObservationClaimCode)
	}
	if err := r.store.MarkWorkerBootstrapObservationStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark cloud worker bootstrap observation started: %w", err)
	}
	signed := SignedWorkerBootstrapObservationCommand{
		EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON,
		PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256,
		IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt,
	}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildWorkerBootstrapObservationCommand(claim.Command, claim.Request, r.now())
		if err != nil {
			return true, r.store.FailWorkerBootstrapObservation(ctx, claim, invalidWorkerBootstrapObservationClaimCode)
		}
		if err := r.store.PersistWorkerBootstrapObservationCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist cloud worker bootstrap observation command: %w", err)
		}
		claim.Command.PayloadJSON = signed.PayloadJSON
		claim.Command.PayloadSHA256 = signed.PayloadSHA256
		claim.Command.RequestSHA256 = signed.RequestSHA256
		claim.Command.SignedEnvelope = signed.EnvelopeJSON
		claim.Command.IssuedAt = signed.IssuedAt
		claim.Command.ExpiresAt = signed.ExpiresAt
	}
	if err := validateSignedWorkerBootstrapObservationCommand(claim.Command, signed); err != nil {
		return true, r.store.FailWorkerBootstrapObservation(ctx, claim, invalidWorkerBootstrapObservationClaimCode)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	observation, err := r.transport.RequestWorkerBootstrapObservation(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferWorkerBootstrapObservation(ctx, claim, "worker_bootstrap_observation_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if workerBootstrapObservationCommandExpired(err) {
			return true, r.store.ExpireWorkerBootstrapObservationCommand(ctx, claim)
		}
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferWorkerBootstrapObservation(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.DeferWorkerBootstrapObservation(ctx, claim, workerBootstrapObservationTransportCode, r.now().Add(r.cfg.RetryDelay))
	}
	if err := ValidateWorkerBootstrapObservation(claim, observation, r.now()); err != nil {
		return true, r.store.DeferWorkerBootstrapObservation(ctx, claim, invalidWorkerBootstrapObservationResultCode, r.now().Add(r.cfg.RetryDelay))
	}
	if err := r.store.CommitWorkerBootstrapObservation(ctx, claim, observation); err != nil {
		return true, fmt.Errorf("commit cloud worker bootstrap observation: %w", err)
	}
	return true, nil
}

func (r *WorkerBootstrapObservationRunner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
