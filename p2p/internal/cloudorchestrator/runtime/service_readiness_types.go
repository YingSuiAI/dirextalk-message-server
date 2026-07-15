package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

const (
	ServiceReadinessPhaseIssue   = "issue"
	ServiceReadinessPhaseObserve = "observe"

	ServiceReadinessIssueAction   = "worker.service_readiness.issue"
	ServiceReadinessObserveAction = "worker.service_readiness.observe"
	ServiceReadinessIssueSchema   = "dirextalk.service-readiness-task-issue/v1"
	ServiceReadinessProbeKind     = "stack_witnessed_fixed_worker_probe_v1"

	ServiceReadinessChallengeIssued = "challenge_issued"
	ServiceReadinessVerified        = "readiness_verified"
)

// ServiceReadinessIssueRequest is a closed request for a fresh challenge
// generated and persisted by the Connection Stack. It cannot select a URL,
// port, path, command, executable, credential, or AWS API. The semantic digest
// identifies a compiled probe known to the Worker image; it is not Worker
// supplied output.
type ServiceReadinessIssueRequest struct {
	Schema                        string `json:"schema"`
	ExecutionID                   string `json:"execution_id"`
	DeploymentID                  string `json:"deployment_id"`
	ServiceID                     string `json:"service_id"`
	TaskID                        string `json:"task_id"`
	ProbeKind                     string `json:"probe_kind"`
	RecipeExecutionManifestDigest string `json:"recipe_execution_manifest_digest"`
	InstallEvidenceDigest         string `json:"install_evidence_digest"`
	SemanticExpectationDigest     string `json:"semantic_expectation_digest"`
}

func (r ServiceReadinessIssueRequest) Validate() error {
	if r.Schema != ServiceReadinessIssueSchema || !validResearchIdentifier("execution_id", r.ExecutionID) ||
		!validResearchIdentifier("deployment_id", r.DeploymentID) || !validResearchIdentifier("service_id", r.ServiceID) ||
		!validResearchIdentifier("task_id", r.TaskID) || r.ProbeKind != ServiceReadinessProbeKind ||
		!deploymentDigestPattern.MatchString(r.RecipeExecutionManifestDigest) ||
		!deploymentDigestPattern.MatchString(r.InstallEvidenceDigest) ||
		!deploymentDigestPattern.MatchString(r.SemanticExpectationDigest) {
		return errors.New("service readiness issue request is invalid")
	}
	return nil
}

func (r ServiceReadinessIssueRequest) Digest() (string, error) { return serviceReadinessDigest(r) }

// ServiceReadinessObserveRequest is read-only and identifies only a task that
// the Stack already accepted. It carries no probe target or execution input.
type ServiceReadinessObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	ServiceID    string `json:"service_id"`
	TaskID       string `json:"task_id"`
}

func (r ServiceReadinessObserveRequest) Validate() error {
	if !validResearchIdentifier("deployment_id", r.DeploymentID) || !validResearchIdentifier("service_id", r.ServiceID) ||
		!validResearchIdentifier("task_id", r.TaskID) {
		return errors.New("service readiness observe request is invalid")
	}
	return nil
}

func (r ServiceReadinessObserveRequest) Digest() (string, error) { return serviceReadinessDigest(r) }

func serviceReadinessDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("dirextalk.cloud.service-readiness-request/v1\x00"), raw...))
	return hex.EncodeToString(sum[:]), nil
}

type ServiceReadinessCommand struct {
	CommandID, ExecutionID, DeploymentID, ServiceID, TaskID, ConnectionID, NodeKeyID string
	ExpectedGeneration, NodeCounter                                                  int64
	Attempt                                                                          int
	Action, RequestDigest                                                            string
	IssuedAt, ExpiresAt                                                              time.Time
	PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope, State                 string
}

type SignedServiceReadinessCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}

// ServiceReadinessResult is a de-secreted Stack projection. A successful
// result requires a fresh challenge digest plus the Stack's own observation
// digest. A local Worker log, checkpoint, or semantic digest alone can never
// satisfy this contract. This is control-plane-witnessed freshness suitable
// for an experimental Service, not proof against a hostile VM root; managed
// maturity still requires an external probe outside the Worker trust boundary.
type ServiceReadinessResult struct {
	ExecutionID, DeploymentID, ServiceID, TaskID, Status, Checkpoint string
	Attempt, LastSequence                                            int64
	ChallengeDigest, SemanticEvidenceDigest, StackObservationDigest  *string
	ErrorCode                                                        *string
	UpdatedAt                                                        time.Time
}

type ServiceReadinessClaim struct {
	Phase, OutboxID, Kind, AggregateType, AggregateID, LeaseToken                  string
	ExecutionID, DeploymentID, ServiceID, ConnectionID, Region, InstanceID, TaskID string
	Purpose, RestoreID, JobID                                                      string
	BrokerEndpoint, NodeKeyID, SemanticExpectationDigest                           string
	ExpectedGeneration, TaskAttempt                                                int64
	Command                                                                        ServiceReadinessCommand
	IssueRequest                                                                   ServiceReadinessIssueRequest
	ObserveRequest                                                                 ServiceReadinessObserveRequest
}

type ServiceReadinessStore interface {
	ClaimServiceReadiness(context.Context, string, time.Duration) (ServiceReadinessClaim, bool, error)
	MarkServiceReadinessStarted(context.Context, ServiceReadinessClaim) error
	PersistServiceReadinessCommand(context.Context, ServiceReadinessClaim, SignedServiceReadinessCommand) error
	CommitServiceReadiness(context.Context, ServiceReadinessClaim, ServiceReadinessResult) error
	DeferServiceReadiness(context.Context, ServiceReadinessClaim, string, time.Time) error
	ExpireServiceReadinessCommand(context.Context, ServiceReadinessClaim) error
	FailServiceReadiness(context.Context, ServiceReadinessClaim, string) error
}

type ServiceReadinessTransport interface {
	BuildServiceReadinessIssueCommand(ServiceReadinessCommand, ServiceReadinessIssueRequest, time.Time) (SignedServiceReadinessCommand, error)
	RequestServiceReadinessIssue(context.Context, string, ServiceReadinessCommand, SignedServiceReadinessCommand, ServiceReadinessIssueRequest) (ServiceReadinessResult, error)
	BuildServiceReadinessObserveCommand(ServiceReadinessCommand, ServiceReadinessObserveRequest, time.Time) (SignedServiceReadinessCommand, error)
	RequestServiceReadinessObserve(context.Context, string, ServiceReadinessCommand, SignedServiceReadinessCommand, ServiceReadinessObserveRequest) (ServiceReadinessResult, error)
}

type serviceReadinessCommandExpiredError struct{ cause error }

func (e serviceReadinessCommandExpiredError) Error() string {
	return "service_readiness_command_expired"
}
func (e serviceReadinessCommandExpiredError) Unwrap() error { return e.cause }
func ServiceReadinessCommandExpired(cause error) error {
	return serviceReadinessCommandExpiredError{cause: cause}
}

func serviceReadinessCommandExpired(err error) bool {
	var expired serviceReadinessCommandExpiredError
	return errors.As(err, &expired)
}

func ValidateServiceReadinessClaim(c ServiceReadinessClaim) error {
	if c.LeaseToken == "" || c.BrokerEndpoint == "" || c.ExpectedGeneration < 1 || c.TaskAttempt < 1 ||
		!validResearchIdentifier("execution_id", c.ExecutionID) || !validResearchIdentifier("deployment_id", c.DeploymentID) ||
		!validResearchIdentifier("service_id", c.ServiceID) || !validResearchIdentifier("task_id", c.TaskID) ||
		!validResearchIdentifier("cloud_connection_id", c.ConnectionID) || !cloudRegion(c.Region) ||
		!ec2InstanceIDPattern.MatchString(c.InstanceID) || !cloudKeyIdentifier(c.NodeKeyID) ||
		!deploymentDigestPattern.MatchString(c.SemanticExpectationDigest) ||
		cloudmodule.ValidateConnectionRegistrationEndpoint(c.BrokerEndpoint, c.Region) != nil {
		return errors.New("service readiness claim is invalid")
	}
	if c.JobID == "" || (c.Purpose != "install" && c.Purpose != "restore") || (c.Purpose == "install" && c.RestoreID != "") || (c.Purpose == "restore" && !validResearchIdentifier("restore_id", c.RestoreID)) {
		return errors.New("service readiness purpose is invalid")
	}
	if c.Command.CommandID == "" || c.Command.ExecutionID != c.ExecutionID || c.Command.DeploymentID != c.DeploymentID ||
		c.Command.ServiceID != c.ServiceID || c.Command.TaskID != c.TaskID || c.Command.ConnectionID != c.ConnectionID ||
		c.Command.NodeKeyID != c.NodeKeyID || c.Command.ExpectedGeneration != c.ExpectedGeneration || c.Command.NodeCounter < 1 || c.Command.Attempt < 1 {
		return errors.New("service readiness command does not bind claim")
	}
	var digest string
	var err error
	if c.Phase == ServiceReadinessPhaseIssue {
		if c.OutboxID == "" || c.Kind != "cloud.service_readiness.requested" || c.AggregateType != "service_readiness_task" ||
			c.AggregateID != c.TaskID || c.Command.Action != ServiceReadinessIssueAction || c.IssueRequest.Validate() != nil ||
			c.IssueRequest.ExecutionID != c.ExecutionID || c.IssueRequest.DeploymentID != c.DeploymentID ||
			c.IssueRequest.ServiceID != c.ServiceID || c.IssueRequest.TaskID != c.TaskID ||
			c.IssueRequest.SemanticExpectationDigest != c.SemanticExpectationDigest || c.ObserveRequest != (ServiceReadinessObserveRequest{}) {
			return errors.New("service readiness issue claim is invalid")
		}
		digest, err = c.IssueRequest.Digest()
	} else if c.Phase == ServiceReadinessPhaseObserve {
		if c.OutboxID != "" || c.Kind != "" || c.AggregateType != "" || c.AggregateID != "" ||
			c.IssueRequest != (ServiceReadinessIssueRequest{}) || c.Command.Action != ServiceReadinessObserveAction || c.ObserveRequest.Validate() != nil ||
			c.ObserveRequest.DeploymentID != c.DeploymentID || c.ObserveRequest.ServiceID != c.ServiceID || c.ObserveRequest.TaskID != c.TaskID {
			return errors.New("service readiness observe claim is invalid")
		}
		digest, err = c.ObserveRequest.Digest()
	} else {
		return errors.New("service readiness phase is invalid")
	}
	if err != nil || c.Command.RequestDigest != digest {
		return errors.New("service readiness command does not bind request")
	}
	return nil
}

func ValidateSignedServiceReadinessCommand(s SignedServiceReadinessCommand) error {
	if strings.TrimSpace(s.EnvelopeJSON) != s.EnvelopeJSON || s.EnvelopeJSON == "" || strings.TrimSpace(s.PayloadJSON) != s.PayloadJSON || s.PayloadJSON == "" ||
		!lowerHexSHA256(s.PayloadSHA256) || !lowerHexSHA256(s.RequestSHA256) || len(s.PayloadJSON) > 16*1024 || len(s.EnvelopeJSON) > 256*1024 ||
		s.IssuedAt.IsZero() || !s.ExpiresAt.After(s.IssuedAt) || s.ExpiresAt.Sub(s.IssuedAt) > 5*time.Minute {
		return errors.New("signed service readiness command is invalid")
	}
	return nil
}

func ValidateServiceReadinessResult(c ServiceReadinessClaim, result ServiceReadinessResult, now time.Time) error {
	if ValidateServiceReadinessClaim(c) != nil || result.ExecutionID != c.ExecutionID || result.DeploymentID != c.DeploymentID ||
		result.ServiceID != c.ServiceID || result.TaskID != c.TaskID || result.Attempt < c.TaskAttempt ||
		(c.Phase == ServiceReadinessPhaseIssue && result.Attempt != c.TaskAttempt) || result.LastSequence < 0 ||
		result.UpdatedAt.IsZero() || result.UpdatedAt.After(now.Add(time.Minute)) {
		return errors.New("service readiness result does not bind claim")
	}
	validDigest := func(value *string) bool { return value != nil && deploymentDigestPattern.MatchString(*value) }
	switch result.Status {
	case "queued":
		if result.LastSequence == 0 && result.Checkpoint == "" && result.ChallengeDigest == nil && result.SemanticEvidenceDigest == nil && result.StackObservationDigest == nil && result.ErrorCode == nil {
			return nil
		}
	case "running":
		if result.LastSequence >= 0 && result.Checkpoint == ServiceReadinessChallengeIssued && validDigest(result.ChallengeDigest) &&
			result.SemanticEvidenceDigest == nil && result.StackObservationDigest == nil && result.ErrorCode == nil {
			return nil
		}
	case "succeeded":
		if result.LastSequence > 0 && result.Checkpoint == ServiceReadinessVerified && validDigest(result.ChallengeDigest) &&
			validDigest(result.SemanticEvidenceDigest) && *result.SemanticEvidenceDigest == c.SemanticExpectationDigest &&
			validDigest(result.StackObservationDigest) && *result.StackObservationDigest != *result.ChallengeDigest &&
			*result.StackObservationDigest != *result.SemanticEvidenceDigest && result.ErrorCode == nil {
			return nil
		}
	case "failed", "interrupted":
		if result.LastSequence > 0 && result.Checkpoint == "" && result.ErrorCode != nil && normalizedErrorCode(*result.ErrorCode, "") == *result.ErrorCode &&
			result.ChallengeDigest == nil && result.SemanticEvidenceDigest == nil && result.StackObservationDigest == nil {
			return nil
		}
	}
	return errors.New("service readiness result is invalid")
}
