package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

const ServiceSecretObserveAction = "service.secret.observe"

var (
	serviceSecretObserveID      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	serviceSecretObserveBinding = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	serviceSecretObserveTask    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	serviceSecretObserveRef     = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	serviceSecretObserveVersion = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
)

type ServiceSecretObserveRequest struct {
	SessionID      string `json:"session_id"`
	DeploymentID   string `json:"deployment_id"`
	TaskID         string `json:"task_id"`
	ExecutionID    string `json:"execution_id"`
	ManifestDigest string `json:"manifest_digest"`
	SecretRef      string `json:"secret_ref"`
	ContextDigest  string `json:"context_digest"`
}
type ServiceSecretObserveCommand struct {
	CommandID, ConnectionID, NodeKeyID                        string
	ExpectedGeneration, NodeCounter                           int64
	Attempt                                                   int
	Action                                                    string
	IssuedAt, ExpiresAt                                       time.Time
	PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope string
}
type SignedServiceSecretObserveCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}
type ServiceSecretObservation struct{ SessionID, Status, ProviderVersion, BindingDigest, UpdatedMarker string }
type ServiceSecretObserveClaim struct {
	LeaseToken, BrokerEndpoint, Region string
	ApprovalExpiresAt                  time.Time
	Request                            ServiceSecretObserveRequest
	Command                            ServiceSecretObserveCommand
}

type ServiceSecretObserveStore interface {
	ClaimPendingServiceSecretObserve(context.Context, string, time.Duration) (ServiceSecretObserveClaim, bool, error)
	PersistServiceSecretObserveCommand(context.Context, ServiceSecretObserveClaim, SignedServiceSecretObserveCommand) error
	CompleteServiceSecretObserve(context.Context, ServiceSecretObserveClaim, ServiceSecretObservation) error
	DeferServiceSecretObserve(context.Context, ServiceSecretObserveClaim, string, time.Time) error
	ExpireServiceSecretObserve(context.Context, ServiceSecretObserveClaim) error
	FailServiceSecretObserve(context.Context, ServiceSecretObserveClaim, string) error
}
type ServiceSecretObserveTransport interface {
	BuildServiceSecretObserveCommand(ServiceSecretObserveCommand, ServiceSecretObserveRequest, time.Time) (SignedServiceSecretObserveCommand, error)
	RequestServiceSecretObserve(context.Context, string, ServiceSecretObserveCommand, SignedServiceSecretObserveCommand, ServiceSecretObserveRequest) (ServiceSecretObservation, error)
}

type serviceSecretObserveUnavailableError struct{ cause error }

func (e serviceSecretObserveUnavailableError) Error() string {
	return "service_secret_observe_unavailable"
}
func (e serviceSecretObserveUnavailableError) Unwrap() error { return e.cause }
func ServiceSecretObserveUnavailable(cause error) error {
	return serviceSecretObserveUnavailableError{cause: cause}
}
func serviceSecretObserveUnavailable(err error) bool {
	var target serviceSecretObserveUnavailableError
	return errors.As(err, &target)
}

type serviceSecretObserveExpiredError struct{ cause error }

func (e serviceSecretObserveExpiredError) Error() string {
	return "service_secret_observe_command_expired"
}
func (e serviceSecretObserveExpiredError) Unwrap() error { return e.cause }
func ServiceSecretObserveCommandExpired(cause error) error {
	return serviceSecretObserveExpiredError{cause: cause}
}
func serviceSecretObserveCommandExpired(err error) bool {
	var target serviceSecretObserveExpiredError
	return errors.As(err, &target)
}

type ServiceSecretObserver struct {
	store     ServiceSecretObserveStore
	transport ServiceSecretObserveTransport
	cfg       Config
}

func NewServiceSecretObserver(store ServiceSecretObserveStore, transport ServiceSecretObserveTransport, cfg Config) *ServiceSecretObserver {
	return &ServiceSecretObserver{store: store, transport: transport, cfg: cfg}
}

func (o *ServiceSecretObserver) RunOnce(ctx context.Context) (bool, error) {
	if o == nil || o.store == nil || o.transport == nil {
		return false, errors.New("service secret observer unavailable")
	}
	if o.cfg.WorkerID == "" || o.cfg.Lease <= 0 || o.cfg.AttemptTimeout <= 0 || o.cfg.AttemptTimeout >= o.cfg.Lease || o.cfg.RetryDelay <= 0 {
		return false, errors.New("service secret observer configuration invalid")
	}
	claim, found, err := o.store.ClaimPendingServiceSecretObserve(ctx, o.cfg.WorkerID, o.cfg.Lease)
	if err != nil || !found {
		return false, err
	}
	if validateServiceSecretObserveClaim(claim) != nil {
		return true, o.store.FailServiceSecretObserve(ctx, claim, "invalid_service_secret_observe_claim")
	}
	signed := SignedServiceSecretObserveCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		signed, err = o.transport.BuildServiceSecretObserveCommand(claim.Command, claim.Request, o.now())
		if err != nil {
			return true, o.store.FailServiceSecretObserve(ctx, claim, "invalid_service_secret_observe_command")
		}
		if validateSignedServiceSecretObserve(claim.Command, claim.Request, signed) != nil {
			return true, o.store.FailServiceSecretObserve(ctx, claim, "invalid_service_secret_observe_command")
		}
		if err = o.store.PersistServiceSecretObserveCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist service secret observe command: %w", err)
		}
	}
	if validateSignedServiceSecretObserve(claim.Command, claim.Request, signed) != nil {
		return true, o.store.FailServiceSecretObserve(ctx, claim, "invalid_service_secret_observe_command")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, o.cfg.AttemptTimeout)
	result, err := o.transport.RequestServiceSecretObserve(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.Request)
	attemptErr := attemptCtx.Err()
	cancel()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, o.store.DeferServiceSecretObserve(ctx, claim, "service_secret_observe_attempt_timed_out", o.now().Add(o.cfg.RetryDelay))
	}
	if err != nil {
		if serviceSecretObserveCommandExpired(err) {
			return true, o.deferOrExpireServiceSecretObserve(ctx, claim, "service_secret_observe_command_expired")
		}
		if serviceSecretObserveUnavailable(err) {
			return true, o.deferOrExpireServiceSecretObserve(ctx, claim, "service_secret_observe_unavailable")
		}
		return true, o.store.DeferServiceSecretObserve(ctx, claim, "service_secret_observe_transport_failed", o.now().Add(o.cfg.RetryDelay))
	}
	if validateServiceSecretObservation(claim, result) != nil {
		return true, o.store.DeferServiceSecretObserve(ctx, claim, "invalid_service_secret_observation", o.now().Add(o.cfg.RetryDelay))
	}
	switch result.Status {
	case "completed":
		return true, o.store.CompleteServiceSecretObserve(ctx, claim, result)
	case "expired":
		return true, o.store.ExpireServiceSecretObserve(ctx, claim)
	case "pending_upload", "processing", "uploaded":
		return true, o.deferOrExpireServiceSecretObserve(ctx, claim, "service_secret_observe_pending")
	default:
		return true, o.store.DeferServiceSecretObserve(ctx, claim, "invalid_service_secret_observation", o.now().Add(o.cfg.RetryDelay))
	}
}

func (o *ServiceSecretObserver) deferOrExpireServiceSecretObserve(ctx context.Context, claim ServiceSecretObserveClaim, code string) error {
	if !o.now().Before(claim.ApprovalExpiresAt) {
		return o.store.ExpireServiceSecretObserve(ctx, claim)
	}
	return o.store.DeferServiceSecretObserve(ctx, claim, code, o.now().Add(o.cfg.RetryDelay))
}
func (o *ServiceSecretObserver) now() time.Time {
	if o.cfg.Now != nil {
		return o.cfg.Now().UTC()
	}
	return time.Now().UTC()
}

func validateServiceSecretObserveClaim(c ServiceSecretObserveClaim) error {
	if c.LeaseToken == "" || c.ApprovalExpiresAt.IsZero() || !serviceSecretObserveID.MatchString(c.Command.CommandID) || !serviceSecretObserveID.MatchString(c.Command.ConnectionID) || c.Command.NodeKeyID == "" || c.Command.ExpectedGeneration < 1 || c.Command.NodeCounter < 1 || c.Command.Attempt < 1 || c.Command.Action != ServiceSecretObserveAction || !serviceSecretObserveID.MatchString(c.Request.SessionID) || !serviceSecretObserveID.MatchString(c.Request.DeploymentID) || !serviceSecretObserveTask.MatchString(c.Request.TaskID) || !serviceSecretObserveBinding.MatchString(c.Request.ExecutionID) || !deploymentDigestPattern.MatchString(c.Request.ManifestDigest) || !deploymentDigestPattern.MatchString(c.Request.ContextDigest) || !serviceSecretObserveRef.MatchString(c.Request.SecretRef) {
		return errors.New("invalid service secret observe claim")
	}
	if cloudmodule.ValidateConnectionRegistrationEndpoint(c.BrokerEndpoint, c.Region) != nil {
		return errors.New("invalid service secret observe endpoint")
	}
	return nil
}
func validateSignedServiceSecretObserve(c ServiceSecretObserveCommand, request ServiceSecretObserveRequest, s SignedServiceSecretObserveCommand) error {
	if strings.TrimSpace(s.EnvelopeJSON) != s.EnvelopeJSON || s.EnvelopeJSON == "" || strings.TrimSpace(s.PayloadJSON) != s.PayloadJSON || s.PayloadJSON == "" || !lowerHexSHA256(s.PayloadSHA256) || !lowerHexSHA256(s.RequestSHA256) || len(s.PayloadJSON) > 8*1024 || len(s.EnvelopeJSON) > 256*1024 || s.IssuedAt.IsZero() || !s.ExpiresAt.After(s.IssuedAt) || s.ExpiresAt.Sub(s.IssuedAt) > 5*time.Minute {
		return errors.New("invalid signed service secret observe command")
	}
	raw, _ := json.Marshal(request)
	if string(raw) != s.PayloadJSON || c.PayloadJSON != "" && c.PayloadJSON != s.PayloadJSON || c.PayloadSHA256 != "" && c.PayloadSHA256 != s.PayloadSHA256 || c.RequestSHA256 != "" && c.RequestSHA256 != s.RequestSHA256 || c.SignedEnvelope != "" && c.SignedEnvelope != s.EnvelopeJSON || !c.IssuedAt.IsZero() && !c.IssuedAt.Equal(s.IssuedAt) || !c.ExpiresAt.IsZero() && !c.ExpiresAt.Equal(s.ExpiresAt) {
		return errors.New("service secret observe persisted command mismatch")
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != s.PayloadSHA256 {
		return errors.New("service secret observe payload mismatch")
	}
	return nil
}
func validateServiceSecretObservation(c ServiceSecretObserveClaim, r ServiceSecretObservation) error {
	if r.SessionID != c.Request.SessionID || r.BindingDigest != c.Request.ContextDigest || !deploymentDigestPattern.MatchString(r.BindingDigest) || !lowerHexSHA256(r.UpdatedMarker) {
		return errors.New("service secret observation binding mismatch")
	}
	switch r.Status {
	case "pending_upload", "processing", "expired":
		if r.ProviderVersion != "" {
			return errors.New("invalid provider version")
		}
	case "uploaded", "completed":
		if !serviceSecretObserveVersion.MatchString(r.ProviderVersion) {
			return errors.New("invalid provider version")
		}
	default:
		return errors.New("invalid status")
	}
	return nil
}
