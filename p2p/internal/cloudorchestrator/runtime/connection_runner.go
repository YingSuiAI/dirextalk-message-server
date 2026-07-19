package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	invalidConnectionRegistrationClaimCode    = "invalid_connection_registration_claim"
	invalidConnectionRegistrationResultCode   = "invalid_connection_registration_result"
	connectionRegistrationTransportFailedCode = "connection_registration_transport_failed"
)

// ConnectionRegistrationRunner verifies a user-deployed Connection Stack with
// exactly one fixed signed Broker command. It cannot create EC2, query a cloud
// SDK, or activate a connection from client supplied metadata alone.
type ConnectionRegistrationRunner struct {
	store     ConnectionRegistrationStore
	transport ConnectionRegistrationTransport
	cfg       Config
}

func NewConnectionRegistrationRunner(store ConnectionRegistrationStore, transport ConnectionRegistrationTransport, cfg Config) *ConnectionRegistrationRunner {
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
	return &ConnectionRegistrationRunner{store: store, transport: transport, cfg: cfg}
}

func (r *ConnectionRegistrationRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil {
		return false, errors.New("cloud connection registration store is unavailable")
	}
	if r.transport == nil {
		return false, errors.New("cloud connection registration transport is unavailable")
	}
	workerID := strings.TrimSpace(r.cfg.WorkerID)
	if workerID == "" {
		return false, errors.New("cloud orchestrator worker id is required")
	}
	if r.cfg.Lease <= 0 || r.cfg.Lease > 5*time.Minute || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease {
		return false, errors.New("cloud connection registration timing configuration is invalid")
	}
	claim, found, err := r.store.ClaimConnectionRegistration(ctx, workerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err := validateConnectionRegistrationClaim(claim); err != nil {
		return true, r.store.FailConnectionRegistration(ctx, claim, invalidConnectionRegistrationClaimCode)
	}
	if err := r.store.MarkConnectionRegistrationStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark cloud connection registration started: %w", err)
	}
	signed := SignedConnectionRegistrationCommand{
		EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON,
		PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256,
		IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt,
	}
	if signed.EnvelopeJSON == "" {
		signed, err = r.transport.BuildConnectionRegistrationCommand(claim.Command, claim.Request)
		if err != nil {
			return true, r.store.FailConnectionRegistration(ctx, claim, invalidConnectionRegistrationClaimCode)
		}
		if err := r.store.PersistConnectionRegistrationCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist cloud connection registration command: %w", err)
		}
		claim.Command.PayloadJSON = signed.PayloadJSON
		claim.Command.PayloadSHA256 = signed.PayloadSHA256
		claim.Command.RequestSHA256 = signed.RequestSHA256
		claim.Command.SignedEnvelope = signed.EnvelopeJSON
		claim.Command.IssuedAt = signed.IssuedAt
		claim.Command.ExpiresAt = signed.ExpiresAt
	}
	if err := validateSignedConnectionRegistrationCommand(claim.Command, signed); err != nil {
		return true, r.store.FailConnectionRegistration(ctx, claim, invalidConnectionRegistrationClaimCode)
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	registration, err := r.transport.RequestConnectionRegistration(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferConnectionRegistration(ctx, claim, "connection_registration_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if connectionRegistrationCommandExpired(err) {
			return true, r.store.ExpireConnectionRegistrationCommand(ctx, claim)
		}
		if code, retry := retryCode(err); retry {
			return true, r.store.DeferConnectionRegistration(ctx, claim, code, r.now().Add(r.cfg.RetryDelay))
		}
		return true, r.store.FailConnectionRegistration(ctx, claim, connectionRegistrationTransportFailedCode)
	}
	if err := ValidateBrokerRegistration(claim, signed, registration); err != nil {
		return true, r.store.FailConnectionRegistration(ctx, claim, invalidConnectionRegistrationResultCode)
	}
	if err := r.store.CommitConnectionRegistration(ctx, claim, registration); err != nil {
		return true, fmt.Errorf("commit cloud connection registration: %w", err)
	}
	return true, nil
}

func (r *ConnectionRegistrationRunner) now() time.Time {
	if r != nil && r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
