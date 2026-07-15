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
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	RecipeInstallRequested     = cloudmodule.OutboxKindRecipeExecutionInstallRequested
	PairingResumeRequested     = cloudmodule.OutboxKindDeploymentPairingResumeRequested
	ServiceOperationRequested  = cloudmodule.OutboxKindServiceOperationRequested
	RecipeInstallPhaseIssue    = "issue"
	RecipeInstallPhaseObserve  = "observe"
	RecipeInstallIssueAction   = "worker.recipe_task.issue"
	RecipeInstallObserveAction = "worker.recipe_task.observe"
	RecipeInstallIssueSchema   = "dirextalk.recipe-execution-task-issue/v1"
)

type RecipeInstallIssueRequest struct {
	Schema                        string                                   `json:"schema"`
	ExecutionID                   string                                   `json:"execution_id"`
	DeploymentID                  string                                   `json:"deployment_id"`
	TaskID                        string                                   `json:"task_id"`
	TaskKind                      string                                   `json:"task_kind"`
	RecipeExecutionManifestDigest string                                   `json:"recipe_execution_manifest_digest"`
	InputDigest                   string                                   `json:"input_digest"`
	CheckpointSequence            []string                                 `json:"checkpoint_sequence"`
	Manifest                      cloudcontracts.RecipeExecutionManifestV1 `json:"manifest"`
}

type RecipeInstallObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
	TaskID       string `json:"task_id"`
}

type RecipeInstallCommand struct {
	CommandID, ExecutionID, DeploymentID, TaskID, ConnectionID, NodeKeyID string
	ExpectedGeneration, NodeCounter                                       int64
	Attempt                                                               int
	Action, RequestDigest                                                 string
	IssuedAt, ExpiresAt                                                   time.Time
	PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope, State      string
}

type SignedRecipeInstallCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}

type RecipeInstallResult struct {
	ExecutionID, DeploymentID, TaskID, Status, LastCheckpoint string
	Attempt, LastSequence                                     int64
	ErrorCode                                                 *string
	UpdatedAt                                                 time.Time
}

type RecipeInstallClaim struct {
	Phase, OutboxID, Kind, AggregateType, AggregateID, LeaseToken               string
	ExecutionID, DeploymentID, PlanID, ConnectionID, Region, InstanceID, TaskID string
	ManifestDigest, InputDigest, BrokerEndpoint, NodeKeyID, JobID               string
	ExpectedGeneration, TaskAttempt                                             int64
	Manifest                                                                    cloudcontracts.RecipeExecutionManifestV1
	Command                                                                     RecipeInstallCommand
	IssueRequest                                                                RecipeInstallIssueRequest
	ObserveRequest                                                              RecipeInstallObserveRequest
}

type RecipeInstallStore interface {
	ClaimRecipeInstall(context.Context, string, time.Duration) (RecipeInstallClaim, bool, error)
	MarkRecipeInstallStarted(context.Context, RecipeInstallClaim) error
	PersistRecipeInstallCommand(context.Context, RecipeInstallClaim, SignedRecipeInstallCommand) error
	CommitRecipeInstall(context.Context, RecipeInstallClaim, RecipeInstallResult) error
	DeferRecipeInstall(context.Context, RecipeInstallClaim, string, time.Time) error
	ExpireRecipeInstallCommand(context.Context, RecipeInstallClaim) error
	FailRecipeInstall(context.Context, RecipeInstallClaim, string) error
}

type RecipeInstallTransport interface {
	BuildRecipeInstallIssueCommand(RecipeInstallCommand, RecipeInstallIssueRequest, time.Time) (SignedRecipeInstallCommand, error)
	RequestRecipeInstallIssue(context.Context, string, RecipeInstallCommand, SignedRecipeInstallCommand, RecipeInstallIssueRequest) (RecipeInstallResult, error)
	BuildRecipeInstallObserveCommand(RecipeInstallCommand, RecipeInstallObserveRequest, time.Time) (SignedRecipeInstallCommand, error)
	RequestRecipeInstallObserve(context.Context, string, RecipeInstallCommand, SignedRecipeInstallCommand, RecipeInstallObserveRequest) (RecipeInstallResult, error)
}

type recipeInstallCommandExpiredError struct{ cause error }

func (e recipeInstallCommandExpiredError) Error() string { return "recipe_install_command_expired" }
func (e recipeInstallCommandExpiredError) Unwrap() error { return e.cause }

func RecipeInstallCommandExpired(cause error) error {
	return recipeInstallCommandExpiredError{cause: cause}
}

func recipeInstallCommandExpired(err error) bool {
	var expired recipeInstallCommandExpiredError
	return errors.As(err, &expired)
}

func (r RecipeInstallIssueRequest) Validate() error {
	if r.Schema != RecipeInstallIssueSchema || !validResearchIdentifier("execution_id", r.ExecutionID) ||
		!validResearchIdentifier("deployment_id", r.DeploymentID) || !validResearchIdentifier("task_id", r.TaskID) ||
		r.TaskKind != "recipe_execution" || !deploymentDigestPattern.MatchString(r.RecipeExecutionManifestDigest) ||
		!deploymentDigestPattern.MatchString(r.InputDigest) || len(r.CheckpointSequence) == 0 || len(r.CheckpointSequence) > 64 {
		return errors.New("recipe install issue request is invalid")
	}
	if r.Manifest.Validate() != nil || r.Manifest.ExecutionID != r.ExecutionID || r.Manifest.DeploymentID != r.DeploymentID ||
		r.Manifest.VerifyDigest(r.RecipeExecutionManifestDigest) != nil {
		return errors.New("recipe install manifest binding is invalid")
	}
	if len(r.Manifest.CheckpointSequence) != len(r.CheckpointSequence) {
		return errors.New("recipe install checkpoint binding is invalid")
	}
	for index := range r.CheckpointSequence {
		if r.Manifest.CheckpointSequence[index] != r.CheckpointSequence[index] {
			return errors.New("recipe install checkpoint binding is invalid")
		}
	}
	seen := map[string]struct{}{}
	for _, checkpoint := range r.CheckpointSequence {
		if normalizedErrorCode(checkpoint, "") != checkpoint {
			return errors.New("recipe install checkpoint is invalid")
		}
		if _, exists := seen[checkpoint]; exists {
			return errors.New("recipe install checkpoint is duplicated")
		}
		seen[checkpoint] = struct{}{}
	}
	return nil
}

func (r RecipeInstallIssueRequest) Digest() (string, error)   { return recipeInstallDigest(r) }
func (r RecipeInstallObserveRequest) Digest() (string, error) { return recipeInstallDigest(r) }
func recipeInstallDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("dirextalk.cloud.recipe-install-request/v1\x00"), raw...))
	return hex.EncodeToString(sum[:]), nil
}

func ValidateRecipeInstallClaim(c RecipeInstallClaim) error {
	if c.Manifest.Validate() != nil || c.Manifest.ExecutionID != c.ExecutionID || c.Manifest.DeploymentID != c.DeploymentID ||
		c.Manifest.PlanID != c.PlanID || c.Manifest.VerifyDigest(c.ManifestDigest) != nil ||
		!deploymentDigestPattern.MatchString(c.InputDigest) || c.TaskAttempt < 1 || c.ExpectedGeneration < 1 ||
		c.LeaseToken == "" || c.BrokerEndpoint == "" || c.Command.ConnectionID != c.ConnectionID || c.Command.ExecutionID != c.ExecutionID ||
		c.Command.DeploymentID != c.DeploymentID || c.Command.TaskID != c.TaskID || c.Command.NodeCounter < 1 {
		return errors.New("recipe install claim is invalid")
	}
	if cloudmodule.ValidateConnectionRegistrationEndpoint(c.BrokerEndpoint, c.Region) != nil {
		return errors.New("recipe install endpoint is invalid")
	}
	if c.Phase == RecipeInstallPhaseIssue {
		installIntent := c.Kind == RecipeInstallRequested && c.AggregateType == "recipe_execution"
		operationIntent := c.Kind == ServiceOperationRequested && c.AggregateType == "service_operation"
		if (!installIntent && !operationIntent) || c.AggregateID != c.ExecutionID || c.OutboxID == "" || c.IssueRequest.Validate() != nil || c.Command.Action != RecipeInstallIssueAction {
			return errors.New("recipe install issue claim is invalid")
		}
	} else if c.Phase == RecipeInstallPhaseObserve {
		if c.OutboxID != "" || c.Command.Action != RecipeInstallObserveAction || c.ObserveRequest.DeploymentID != c.DeploymentID || c.ObserveRequest.TaskID != c.TaskID {
			return errors.New("recipe install observe claim is invalid")
		}
	} else {
		return errors.New("recipe install phase is invalid")
	}
	return nil
}

func ValidateSignedRecipeInstallCommand(c RecipeInstallCommand, s SignedRecipeInstallCommand) error {
	if strings.TrimSpace(s.EnvelopeJSON) != s.EnvelopeJSON || s.EnvelopeJSON == "" || strings.TrimSpace(s.PayloadJSON) != s.PayloadJSON || s.PayloadJSON == "" ||
		!lowerHexSHA256(s.PayloadSHA256) || !lowerHexSHA256(s.RequestSHA256) || len(s.PayloadJSON) > 64*1024 || len(s.EnvelopeJSON) > 256*1024 ||
		s.IssuedAt.IsZero() || !s.ExpiresAt.After(s.IssuedAt) || s.ExpiresAt.Sub(s.IssuedAt) > 5*time.Minute {
		return errors.New("signed recipe install command is invalid")
	}
	return nil
}

func ValidateRecipeInstallResult(c RecipeInstallClaim, result RecipeInstallResult, now time.Time) error {
	if ValidateRecipeInstallClaim(c) != nil || result.ExecutionID != c.ExecutionID || result.DeploymentID != c.DeploymentID || result.TaskID != c.TaskID ||
		result.Attempt != c.TaskAttempt || result.LastSequence < 0 || result.UpdatedAt.IsZero() || result.UpdatedAt.After(now.Add(time.Minute)) {
		return errors.New("recipe install result does not bind claim")
	}
	switch result.Status {
	case "queued":
		return nil
	case "running":
		for _, cp := range c.Manifest.CheckpointSequence {
			if result.LastCheckpoint == cp {
				return nil
			}
		}
	case "succeeded":
		if result.LastCheckpoint == c.Manifest.CheckpointSequence[len(c.Manifest.CheckpointSequence)-1] && result.ErrorCode == nil {
			return nil
		}
	case "failed", "interrupted":
		if result.ErrorCode != nil {
			if result.LastCheckpoint == "" {
				return nil
			}
			for index, checkpoint := range c.Manifest.CheckpointSequence {
				if result.LastCheckpoint == checkpoint && index < len(c.Manifest.CheckpointSequence)-1 {
					return nil
				}
			}
		}
	}
	return errors.New("recipe install result is invalid")
}
