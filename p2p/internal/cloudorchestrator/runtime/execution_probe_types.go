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
	// ExecutionProbeIssueRequested is the private outbox kind created after a
	// Connection Stack independently verifies one dedicated Worker lease.
	ExecutionProbeIssueRequested = cloudmodule.OutboxKindExecutionProbeIssueRequested

	ExecutionProbePhaseIssue   = "issue"
	ExecutionProbePhaseObserve = "observe"

	ExecutionProbeTaskKind        = "execution_probe"
	ExecutionProbeIssueSchema     = "dirextalk.worker-task-issue/v1"
	ExecutionProbeIssueAction     = "worker.task.issue"
	ExecutionProbeObserveAction   = "worker.task.observe"
	ExecutionProbeReceived        = "execution_manifest_received"
	ExecutionProbeTransportPassed = "task_transport_verified"

	executionProbeClockSkew = time.Minute
)

// ExecutionProbeClaim is a lease-fenced, digest-only instruction for the
// first Worker task transport. It deliberately contains no recipe command,
// source URL, secret reference, bearer, endpoint from the Worker, or cloud
// control input. Phase is either one durable issue outbox or a read-only task
// observation.
type ExecutionProbeClaim struct {
	Phase                   string
	OutboxID                string
	Kind                    string
	AggregateType           string
	AggregateID             string
	DeploymentID            string
	PlanID                  string
	ConnectionID            string
	Region                  string
	InstanceID              string
	TaskID                  string
	TaskAttempt             int64
	ExecutionManifestDigest string
	InputDigest             string
	BrokerEndpoint          string
	NodeKeyID               string
	ExpectedGeneration      int64
	JobID                   string
	LeaseToken              string
	Command                 ExecutionProbeCommand
	IssueRequest            ExecutionProbeIssueRequest
	ObserveRequest          ExecutionProbeObserveRequest
}

// ExecutionProbeIssueRequest is the exact digest-only intent accepted by the
// Connection Stack. Both artifact references must have been sealed by the
// Orchestrator before this request exists.
type ExecutionProbeIssueRequest struct {
	Schema                  string `json:"schema"`
	DeploymentID            string `json:"deployment_id"`
	TaskID                  string `json:"task_id"`
	TaskKind                string `json:"task_kind"`
	ExecutionManifestDigest string `json:"execution_manifest_digest"`
	InputDigest             string `json:"input_digest"`
}

func (request ExecutionProbeIssueRequest) Validate() error {
	if request.Schema != ExecutionProbeIssueSchema || !validResearchIdentifier("deployment_id", request.DeploymentID) ||
		!validResearchIdentifier("task_id", request.TaskID) || request.TaskKind != ExecutionProbeTaskKind ||
		!deploymentDigestPattern.MatchString(request.ExecutionManifestDigest) || !deploymentDigestPattern.MatchString(request.InputDigest) {
		return errors.New("execution probe issue request is invalid")
	}
	return nil
}

// Digest is a local command-journal identity. It is not a plan, recipe, input
// artifact, or Stack request SHA-256 value.
func (request ExecutionProbeIssueRequest) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	return executionProbeRequestDigest(
		"dirextalk.cloud.execution-probe-issue-request/v1", request.Schema, request.DeploymentID,
		request.TaskID, request.TaskKind, request.ExecutionManifestDigest, request.InputDigest,
	), nil
}

// ExecutionProbeObserveRequest identifies one already-issued task. It carries
// no execution capability or task material.
type ExecutionProbeObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}

func (request ExecutionProbeObserveRequest) Validate() error {
	if !validResearchIdentifier("deployment_id", request.DeploymentID) || !validResearchIdentifier("task_id", request.TaskID) {
		return errors.New("execution probe observe request is invalid")
	}
	return nil
}

func (request ExecutionProbeObserveRequest) Digest() (string, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	return executionProbeRequestDigest("dirextalk.cloud.execution-probe-observe-request/v1", request.DeploymentID, request.TaskID), nil
}

func executionProbeRequestDigest(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(value))
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// ExecutionProbeCommand preserves an exact signed Stack envelope across
// retries. A new command counter can be allocated only after the Stack says
// the persisted command expired.
type ExecutionProbeCommand struct {
	CommandID          string
	DeploymentID       string
	TaskID             string
	ConnectionID       string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	Attempt            int
	Action             string
	IssuedAt           time.Time
	ExpiresAt          time.Time
	RequestDigest      string
	PayloadJSON        string
	PayloadSHA256      string
	RequestSHA256      string
	SignedEnvelope     string
	State              string
}

type SignedExecutionProbeCommand struct {
	EnvelopeJSON  string
	PayloadJSON   string
	PayloadSHA256 string
	RequestSHA256 string
	IssuedAt      time.Time
	ExpiresAt     time.Time
}

// ExecutionProbeTaskResult is the de-secreted state summary returned by the
// Connection Stack. It excludes task documents, logs, endpoints, Worker
// identity, raw events, secret values and arbitrary output.
type ExecutionProbeTaskResult struct {
	TaskID         string
	DeploymentID   string
	Status         string
	Attempt        int64
	LastSequence   int64
	Checkpoint     *string
	ErrorCode      *string
	EvidenceDigest *string
	UpdatedAt      time.Time
}

// ExecutionProbeStore owns both the issue-outbox lease and a separate
// observation lease. Its implementation must repeat validation inside each
// PostgreSQL fencing transaction.
type ExecutionProbeStore interface {
	ClaimExecutionProbe(context.Context, string, time.Duration) (ExecutionProbeClaim, bool, error)
	PersistExecutionProbeCommand(context.Context, ExecutionProbeClaim, SignedExecutionProbeCommand) error
	MarkExecutionProbeStarted(context.Context, ExecutionProbeClaim) error
	CommitExecutionProbe(context.Context, ExecutionProbeClaim, ExecutionProbeTaskResult) error
	DeferExecutionProbe(context.Context, ExecutionProbeClaim, string, time.Time) error
	ExpireExecutionProbeCommand(context.Context, ExecutionProbeClaim) error
	FailExecutionProbe(context.Context, ExecutionProbeClaim, string) error
}

// ExecutionProbeTransport is limited to the two existing Connection Stack task
// commands. It has neither AWS credentials nor a Worker bearer/session API.
type ExecutionProbeTransport interface {
	BuildExecutionProbeIssueCommand(ExecutionProbeCommand, ExecutionProbeIssueRequest, time.Time) (SignedExecutionProbeCommand, error)
	RequestExecutionProbeIssue(context.Context, string, ExecutionProbeCommand, SignedExecutionProbeCommand, ExecutionProbeIssueRequest) (ExecutionProbeTaskResult, error)
	BuildExecutionProbeObserveCommand(ExecutionProbeCommand, ExecutionProbeObserveRequest, time.Time) (SignedExecutionProbeCommand, error)
	RequestExecutionProbeObserve(context.Context, string, ExecutionProbeCommand, SignedExecutionProbeCommand, ExecutionProbeObserveRequest) (ExecutionProbeTaskResult, error)
}

type executionProbeCommandExpiredError struct{ cause error }

func (e executionProbeCommandExpiredError) Error() string {
	if e.cause == nil {
		return "execution_probe_command_expired"
	}
	return "execution_probe_command_expired: " + e.cause.Error()
}

func (e executionProbeCommandExpiredError) Unwrap() error { return e.cause }

func ExecutionProbeCommandExpired(cause error) error {
	return executionProbeCommandExpiredError{cause: cause}
}

func executionProbeCommandExpired(err error) bool {
	var expired executionProbeCommandExpiredError
	return errors.As(err, &expired)
}

func ExecutionProbeRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "execution_probe_retryable"), cause: cause}
}

func validateExecutionProbeClaim(claim ExecutionProbeClaim) error {
	if !validResearchIdentifier("deployment_id", claim.DeploymentID) || !validResearchIdentifier("plan_id", claim.PlanID) ||
		!validResearchIdentifier("cloud_connection_id", claim.ConnectionID) || !cloudRegion(claim.Region) ||
		!ec2InstanceIDPattern.MatchString(claim.InstanceID) || !validResearchIdentifier("task_id", claim.TaskID) ||
		!safeExecutionProbeValue(claim.TaskAttempt) || !deploymentDigestPattern.MatchString(claim.ExecutionManifestDigest) ||
		!deploymentDigestPattern.MatchString(claim.InputDigest) || claim.BrokerEndpoint == "" || !cloudKeyIdentifier(claim.NodeKeyID) ||
		!safeDeploymentGeneration(claim.ExpectedGeneration) || !validResearchIdentifier("job_id", claim.JobID) || claim.LeaseToken == "" {
		return errors.New("execution probe claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return errors.New("execution probe endpoint is invalid")
	}
	if claim.Command.CommandID == "" || claim.Command.DeploymentID != claim.DeploymentID || claim.Command.TaskID != claim.TaskID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID ||
		claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 {
		return errors.New("execution probe command does not bind the claim")
	}
	switch claim.Phase {
	case ExecutionProbePhaseIssue:
		if claim.OutboxID == "" || claim.Kind != ExecutionProbeIssueRequested || claim.AggregateType != "execution_probe_task" || claim.AggregateID != claim.TaskID ||
			claim.ObserveRequest != (ExecutionProbeObserveRequest{}) || claim.Command.Action != ExecutionProbeIssueAction {
			return errors.New("execution probe issue claim is invalid")
		}
		if err := claim.IssueRequest.Validate(); err != nil || claim.IssueRequest.DeploymentID != claim.DeploymentID || claim.IssueRequest.TaskID != claim.TaskID ||
			claim.IssueRequest.ExecutionManifestDigest != claim.ExecutionManifestDigest || claim.IssueRequest.InputDigest != claim.InputDigest {
			return errors.New("execution probe issue request does not bind the claim")
		}
		digest, err := claim.IssueRequest.Digest()
		if err != nil || claim.Command.RequestDigest != digest {
			return errors.New("execution probe issue command does not bind the request")
		}
	case ExecutionProbePhaseObserve:
		if claim.OutboxID != "" || claim.Kind != "" || claim.AggregateType != "" || claim.AggregateID != "" ||
			claim.IssueRequest != (ExecutionProbeIssueRequest{}) || claim.Command.Action != ExecutionProbeObserveAction {
			return errors.New("execution probe observe claim is invalid")
		}
		if err := claim.ObserveRequest.Validate(); err != nil || claim.ObserveRequest.DeploymentID != claim.DeploymentID || claim.ObserveRequest.TaskID != claim.TaskID {
			return errors.New("execution probe observe request does not bind the claim")
		}
		digest, err := claim.ObserveRequest.Digest()
		if err != nil || claim.Command.RequestDigest != digest {
			return errors.New("execution probe observe command does not bind the request")
		}
	default:
		return errors.New("execution probe phase is invalid")
	}
	return nil
}

func validateSignedExecutionProbeCommand(command ExecutionProbeCommand, signed SignedExecutionProbeCommand) error {
	if command.CommandID == "" || strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" ||
		strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON || signed.PayloadJSON == "" || !lowerHexSHA256(signed.PayloadSHA256) ||
		!lowerHexSHA256(signed.RequestSHA256) || len(signed.EnvelopeJSON) > 256*1024 || len(signed.PayloadJSON) > 8*1024 ||
		signed.IssuedAt.IsZero() || signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed execution probe command is invalid")
	}
	return nil
}

// ValidateExecutionProbeResult validates the closed Stack task summary before
// it reaches the Store. The Store must additionally enforce the persisted
// sequence and status transition inside its lease-fenced transaction.
func ValidateExecutionProbeResult(claim ExecutionProbeClaim, signed SignedExecutionProbeCommand, result ExecutionProbeTaskResult, now time.Time) error {
	if err := validateExecutionProbeClaim(claim); err != nil {
		return err
	}
	if err := validateSignedExecutionProbeCommand(claim.Command, signed); err != nil {
		return err
	}
	if result.DeploymentID != claim.DeploymentID || result.TaskID != claim.TaskID || !safeExecutionProbeValue(result.Attempt) ||
		result.Attempt != claim.TaskAttempt || !safeExecutionProbeNonnegative(result.LastSequence) || result.UpdatedAt.IsZero() ||
		result.UpdatedAt.UTC().After(now.UTC().Add(executionProbeClockSkew)) || !validExecutionProbeCode(result.Checkpoint) ||
		!validExecutionProbeCode(result.ErrorCode) || !validExecutionProbeDigest(result.EvidenceDigest) {
		return errors.New("execution probe result does not bind the claim")
	}
	switch result.Status {
	case "queued":
		if result.Attempt != 1 || result.LastSequence != 0 || result.Checkpoint != nil || result.ErrorCode != nil || result.EvidenceDigest != nil {
			return errors.New("execution probe queued result is invalid")
		}
	case "running":
		if result.LastSequence < 1 || result.Checkpoint == nil || *result.Checkpoint != ExecutionProbeReceived || result.ErrorCode != nil ||
			result.EvidenceDigest == nil || *result.EvidenceDigest != claim.ExecutionManifestDigest {
			return errors.New("execution probe running result is invalid")
		}
	case "succeeded":
		if result.LastSequence < 1 || result.Checkpoint == nil || *result.Checkpoint != ExecutionProbeTransportPassed || result.ErrorCode != nil ||
			result.EvidenceDigest == nil || *result.EvidenceDigest != claim.ExecutionManifestDigest {
			return errors.New("execution probe succeeded result is invalid")
		}
	case "failed", "interrupted":
		if result.LastSequence < 1 || result.Checkpoint != nil || result.ErrorCode == nil || result.EvidenceDigest != nil {
			return errors.New("execution probe terminal result is invalid")
		}
	default:
		return errors.New("execution probe status is invalid")
	}
	return nil
}

func safeExecutionProbeValue(value int64) bool {
	return value > 0 && value <= 9_007_199_254_740_991
}

func safeExecutionProbeNonnegative(value int64) bool {
	return value >= 0 && value <= 9_007_199_254_740_991
}

func validExecutionProbeCode(value *string) bool {
	if value == nil {
		return true
	}
	return *value != "" && normalizedErrorCode(*value, "") == *value
}

func validExecutionProbeDigest(value *string) bool {
	return value == nil || deploymentDigestPattern.MatchString(*value)
}

func executionProbeRetryCode(phase string) string {
	switch phase {
	case ExecutionProbePhaseIssue:
		return "execution_probe_issue_transport_failed"
	case ExecutionProbePhaseObserve:
		return "execution_probe_observe_transport_failed"
	default:
		return "execution_probe_transport_failed"
	}
}

func executionProbeInvalidClaimCode(phase string) string {
	return fmt.Sprintf("invalid_execution_probe_%s_claim", phase)
}

func executionProbeInvalidResultCode(phase string) string {
	return fmt.Sprintf("invalid_execution_probe_%s_result", phase)
}
