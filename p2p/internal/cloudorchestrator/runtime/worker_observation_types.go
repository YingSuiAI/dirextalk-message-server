package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

const (
	// WorkerBootstrapObservationSchema is the closed, de-secretsed response
	// from a signed deployment.observe command. It never contains a Worker
	// bearer, bootstrap session identifier, IID document, raw Worker event, or
	// service endpoint.
	WorkerBootstrapObservationSchema = "dirextalk.aws.deployment-observation/v1"

	workerBootstrapObservationMaximumAge = 5 * time.Minute
	workerBootstrapObservationClockSkew  = time.Minute
)

// WorkerBootstrapObservationClaim is a private, lease-fenced read request.
// It can only observe the Worker created for one existing deployment. It has
// no cloud mutation capability and does not contain a Worker bearer or any
// bootstrap session identifier.
type WorkerBootstrapObservationClaim struct {
	DeploymentID       string
	PlanID             string
	ConnectionID       string
	Region             string
	InstanceID         string
	BrokerEndpoint     string
	NodeKeyID          string
	ExpectedGeneration int64
	JobID              string
	LeaseToken         string
	Attempt            int
	Request            WorkerBootstrapObservationRequest
	Command            WorkerBootstrapObservationCommand
}

// WorkerBootstrapObservationRequest is deliberately closed to the one
// deployment selected by the Store. The Connection Stack binds this request to
// its private create receipt and active Worker session before returning a
// de-secretsed observation.
type WorkerBootstrapObservationRequest struct {
	DeploymentID string `json:"deployment_id"`
}

func (request WorkerBootstrapObservationRequest) Validate() error {
	if !validResearchIdentifier("deployment_id", request.DeploymentID) {
		return errors.New("worker bootstrap observation request is invalid")
	}
	return nil
}

// Digest is separate from the signed-envelope hash. It prevents a persisted
// observe command from being replayed for another deployment.
func (request WorkerBootstrapObservationRequest) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	hash := sha256.New()
	for _, value := range []string{"dirextalk.cloud.worker-bootstrap-observation-request/v1", request.DeploymentID} {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// WorkerBootstrapObservationCommand is an exact persisted command identity.
// An indeterminate read replays this byte-for-byte envelope; only the Stack's
// explicit expired-command response may allocate a new node counter.
type WorkerBootstrapObservationCommand struct {
	CommandID          string
	DeploymentID       string
	ConnectionID       string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	Attempt            int
	IssuedAt           time.Time
	ExpiresAt          time.Time
	RequestDigest      string
	PayloadJSON        string
	PayloadSHA256      string
	RequestSHA256      string
	SignedEnvelope     string
	State              string
}

type SignedWorkerBootstrapObservationCommand struct {
	EnvelopeJSON  string
	PayloadJSON   string
	PayloadSHA256 string
	RequestSHA256 string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// WorkerBootstrapObservation is the sole accepted deployment.observe result.
// Stack-side code has already checked the hidden bootstrap-session identity,
// create request hash, connection, and instance ID. The Orchestrator persists
// only this minimum independent evidence, never the hidden session identity.
// ResourceStatus deliberately remains the Stack create receipt's
// "provisioning" value; it is not the ProductCore Deployment resource axis.
type WorkerBootstrapObservation struct {
	Schema             string
	DeploymentID       string
	ResourceStatus     string
	InstanceID         string
	WorkerSessionState string
	LeaseEpoch         int64
	LeaseExpiresAt     time.Time
	LastSequence       int64
	LastEventAt        time.Time
	ObservedAt         time.Time
}

// WorkerBootstrapObservationStore owns the durable polling lease, signed
// command journal, private evidence, and atomic Job transition. The public
// Message Server does not implement this interface.
type WorkerBootstrapObservationStore interface {
	ClaimWorkerBootstrapObservation(context.Context, string, time.Duration) (WorkerBootstrapObservationClaim, bool, error)
	PersistWorkerBootstrapObservationCommand(context.Context, WorkerBootstrapObservationClaim, SignedWorkerBootstrapObservationCommand) error
	MarkWorkerBootstrapObservationStarted(context.Context, WorkerBootstrapObservationClaim) error
	CommitWorkerBootstrapObservation(context.Context, WorkerBootstrapObservationClaim, WorkerBootstrapObservation) error
	DeferWorkerBootstrapObservation(context.Context, WorkerBootstrapObservationClaim, string, time.Time) error
	ExpireWorkerBootstrapObservationCommand(context.Context, WorkerBootstrapObservationClaim) error
	FailWorkerBootstrapObservation(context.Context, WorkerBootstrapObservationClaim, string) error
}

// WorkerBootstrapObservationTransport can only form and send the closed
// deployment.observe command. It has neither an AWS SDK nor a Worker/session
// credential API.
type WorkerBootstrapObservationTransport interface {
	BuildWorkerBootstrapObservationCommand(WorkerBootstrapObservationCommand, WorkerBootstrapObservationRequest, time.Time) (SignedWorkerBootstrapObservationCommand, error)
	RequestWorkerBootstrapObservation(context.Context, string, WorkerBootstrapObservationCommand, SignedWorkerBootstrapObservationCommand, WorkerBootstrapObservationRequest) (WorkerBootstrapObservation, error)
}

type workerBootstrapObservationExpiredError struct{ cause error }

func (e workerBootstrapObservationExpiredError) Error() string {
	if e.cause == nil {
		return "worker_bootstrap_observation_command_expired"
	}
	return "worker_bootstrap_observation_command_expired: " + e.cause.Error()
}

func (e workerBootstrapObservationExpiredError) Unwrap() error { return e.cause }

// WorkerBootstrapObservationCommandExpired marks the one explicit Stack
// response that permits a new observation counter and envelope.
func WorkerBootstrapObservationCommandExpired(cause error) error {
	return workerBootstrapObservationExpiredError{cause: cause}
}

func workerBootstrapObservationCommandExpired(err error) bool {
	var expired workerBootstrapObservationExpiredError
	return errors.As(err, &expired)
}

func WorkerBootstrapObservationRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "worker_bootstrap_observation_retryable"), cause: cause}
}

func validateWorkerBootstrapObservationClaim(claim WorkerBootstrapObservationClaim) error {
	if !validResearchIdentifier("deployment_id", claim.DeploymentID) || !validResearchIdentifier("plan_id", claim.PlanID) ||
		!validResearchIdentifier("cloud_connection_id", claim.ConnectionID) || !cloudRegion(claim.Region) ||
		!ec2InstanceIDPattern.MatchString(claim.InstanceID) || claim.BrokerEndpoint == "" || !cloudKeyIdentifier(claim.NodeKeyID) ||
		!safeDeploymentGeneration(claim.ExpectedGeneration) || !validResearchIdentifier("job_id", claim.JobID) || claim.LeaseToken == "" {
		return errors.New("worker bootstrap observation claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return errors.New("worker bootstrap observation endpoint is invalid")
	}
	if err := claim.Request.Validate(); err != nil || claim.Request.DeploymentID != claim.DeploymentID {
		return errors.New("worker bootstrap observation request does not bind the claim")
	}
	digest, err := claim.Request.Digest()
	if err != nil || claim.Command.CommandID == "" || claim.Command.DeploymentID != claim.DeploymentID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID ||
		claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 ||
		claim.Command.RequestDigest != digest {
		return errors.New("worker bootstrap observation command does not bind the claim")
	}
	return nil
}

func validateSignedWorkerBootstrapObservationCommand(command WorkerBootstrapObservationCommand, signed SignedWorkerBootstrapObservationCommand) error {
	if command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" ||
		!lowerHexSHA256(signed.PayloadSHA256) || !lowerHexSHA256(signed.RequestSHA256) || len(signed.EnvelopeJSON) > 256*1024 ||
		len(signed.PayloadJSON) > 8*1024 || signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() ||
		!signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed worker bootstrap observation command is invalid")
	}
	return nil
}

// ValidateWorkerBootstrapObservation checks the de-secretsed Stack result at
// the process boundary. Store implementations must repeat this immediately
// before their fencing transaction; a prior runtime validation is not an
// authorization decision.
func ValidateWorkerBootstrapObservation(claim WorkerBootstrapObservationClaim, observation WorkerBootstrapObservation, now time.Time) error {
	if err := validateWorkerBootstrapObservationClaim(claim); err != nil {
		return err
	}
	if observation.Schema != WorkerBootstrapObservationSchema || observation.DeploymentID != claim.DeploymentID ||
		observation.ResourceStatus != "provisioning" || observation.InstanceID != claim.InstanceID ||
		observation.WorkerSessionState != "active" || !safeDeploymentGeneration(observation.LeaseEpoch) ||
		observation.LastSequence < 0 || observation.ObservedAt.IsZero() || observation.LeaseExpiresAt.IsZero() {
		return errors.New("worker bootstrap observation does not bind the claim")
	}
	now = now.UTC()
	observedAt := observation.ObservedAt.UTC()
	if observedAt.After(now.Add(workerBootstrapObservationClockSkew)) || now.Sub(observedAt) > workerBootstrapObservationMaximumAge ||
		!observation.LeaseExpiresAt.UTC().After(now) || !observation.LeaseExpiresAt.UTC().After(observedAt) ||
		(!observation.LastEventAt.IsZero() && observation.LastEventAt.UTC().After(observedAt)) {
		return fmt.Errorf("worker bootstrap observation is stale")
	}
	return nil
}
