package cloud

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const agentCloudDeploymentDestroyApprovalSchema = "dirextalk.agent.cloud-deployment-destroy-approval/v1"
const agentCloudDeploymentDestroyScopeSchema = "dirextalk.agent.cloud-deployment-destroy-scope/v1"

var (
	agentCloudEC2ProviderIDPattern = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	agentCloudEBSProviderIDPattern = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	agentCloudENIProviderIDPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
	agentCloudSGProviderIDPattern  = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	agentCloudDestroyIDPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

type agentCloudDeploymentDestroyApprovalV1 struct {
	SchemaVersion string                           `json:"schema_version"`
	OperationID   string                           `json:"operation_id"`
	ChallengeID   string                           `json:"challenge_id"`
	ApprovalID    string                           `json:"approval_id"`
	SignerKeyID   string                           `json:"signer_key_id"`
	Scope         AgentCloudDeploymentDestroyScope `json:"scope"`
	ExpiresAt     time.Time                        `json:"expires_at"`
	Revision      int64                            `json:"revision"`
	Signature     string                           `json:"signature,omitempty"`
}

func (m *Module) prepareAgentDeploymentDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id", "expected_revision", "signer_key_id", "idempotency_key"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	deploymentID := values.String("deployment_id")
	expectedRevision := values.Int64("expected_revision")
	signerKeyID := values.String("signer_key_id")
	idempotencyKey := values.String("idempotency_key")
	if !canonicalUUID(deploymentID) || expectedRevision <= 0 || !cloudKeyIDPattern.MatchString(signerKeyID) ||
		!canonicalUUID(idempotencyKey) || ContainsSensitiveGoalMaterial(signerKeyID) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction is invalid")
	}
	deployment, found, err := m.getAgentDestroyDeployment(ctx, deploymentID)
	if err != nil || !found {
		return nil, agentDeploymentDestroyError(err, found)
	}
	if !validAgentDestroyDeployment(deployment) || deployment.Revision != expectedRevision || !agentDestroyableDeploymentProjection(deployment.Resource) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyConflictCode, "cloud deployment destruction conflicts with current Agent state")
	}
	challenge, err := m.cfg.AgentCloudControlClient.CreateAgentCloudDeploymentDestroyChallenge(ctx, AgentCloudDeploymentDestroyChallengeRequest{
		IdempotencyKey: idempotencyKey, DeploymentID: deploymentID, ExpectedRevision: expectedRevision,
		SignerKeyID: signerKeyID, ExpectedDeployment: deployment,
	})
	if err != nil {
		return nil, agentDeploymentDestroyError(err, true)
	}
	now := m.now().UTC()
	if !validAgentDeploymentDestroyChallenge(challenge, deployment, signerKeyID, now) {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudDeploymentDestroyInvalidCode, "cloud Agent returned an invalid deployment destruction confirmation")
	}
	approval := agentCloudDeploymentDestroyApprovalV1{
		SchemaVersion: agentCloudDeploymentDestroyApprovalSchema,
		OperationID:   challenge.OperationID, ChallengeID: challenge.ChallengeID, ApprovalID: challenge.ApprovalID,
		SignerKeyID: challenge.SignerKeyID, Scope: challenge.Scope, ExpiresAt: challenge.ExpiresAt, Revision: challenge.Revision,
	}
	digest := sha256.Sum256(challenge.SigningPayloadCBOR)
	return map[string]any{"confirmation": map[string]any{
		"deployment": deployment, "approval": approval,
		"signing_payload_cbor":   base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR),
		"signing_payload_digest": "sha256:" + hex.EncodeToString(digest[:]),
	}}, nil
}

func (m *Module) approveAgentDeploymentDestroy(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "deployment_id", "expected_revision", "idempotency_key", "approval"); err != nil {
		return nil, err
	}
	values := actionbase.Params(params)
	deploymentID := values.String("deployment_id")
	expectedRevision := values.Int64("expected_revision")
	idempotencyKey := values.String("idempotency_key")
	approval, signature, err := decodeAgentDeploymentDestroyApproval(params["approval"])
	if !canonicalUUID(deploymentID) || expectedRevision <= 0 || !canonicalUUID(idempotencyKey) || err != nil ||
		approval.Scope.DeploymentID != deploymentID || approval.Scope.DeploymentRevision != expectedRevision {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction approval is invalid")
	}
	if !m.now().UTC().Before(approval.ExpiresAt) {
		// A durable accepted operation may be replayed after its one-time
		// challenge expires. Read it before rejecting an otherwise exact retry.
		if recovered, ok := m.recoverAgentDeploymentDestroy(ctx, approval); ok {
			return recovered, nil
		}
		return nil, actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyExpiredCode, "cloud deployment destruction approval has expired")
	}
	deployment, found, getErr := m.getAgentDestroyDeployment(ctx, deploymentID)
	if getErr != nil || !found {
		return nil, agentDeploymentDestroyError(getErr, found)
	}
	if !validAgentDestroyDeployment(deployment) || !agentDestroyScopeMatchesDeployment(approval.Scope, deployment, expectedRevision) {
		if recovered, ok := m.recoverAgentDeploymentDestroy(ctx, approval); ok {
			return recovered, nil
		}
		return nil, actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyConflictCode, "cloud deployment destruction conflicts with current Agent state")
	}
	result, callErr := m.cfg.AgentCloudControlClient.ApproveAgentCloudDeploymentDestroy(ctx, AgentCloudDeploymentDestroyApproveRequest{
		IdempotencyKey: idempotencyKey, DeploymentID: deploymentID, ExpectedOperationID: approval.OperationID,
		ExpectedRevision: expectedRevision, ExpectedDeployment: deployment, Approval: signature,
	})
	if callErr == nil && validAgentDeploymentDestroyResult(result, approval) {
		return agentDeploymentDestroyResultView(result), nil
	}
	if callErr == nil {
		callErr = ErrAgentCloudControlInvalidResponse
	}
	if recovered, ok := m.recoverAgentDeploymentDestroy(ctx, approval); ok {
		return recovered, nil
	}
	return nil, agentDeploymentDestroyError(callErr, true)
}

func (m *Module) recoverAgentDeploymentDestroy(ctx context.Context, approval agentCloudDeploymentDestroyApprovalV1) (map[string]any, bool) {
	operation, found, err := m.cfg.AgentCloudControlClient.GetAgentCloudDestroyOperation(ctx, AgentCloudDestroyOperationRequest{OperationID: approval.OperationID})
	if err != nil || !found || operation.Status == "awaiting_approval" || !validAgentDestroyOperation(operation, approval) {
		return nil, false
	}
	deployment, deploymentFound, err := m.getAgentDestroyDeployment(ctx, approval.Scope.DeploymentID)
	if err != nil || !deploymentFound {
		return nil, false
	}
	result := AgentCloudDeploymentDestroyResult{Operation: operation, Deployment: deployment}
	if !validAgentDeploymentDestroyResult(result, approval) {
		return nil, false
	}
	return agentDeploymentDestroyResultView(result), true
}

func (m *Module) getAgentDestroyDeployment(ctx context.Context, deploymentID string) (Deployment, bool, error) {
	reader := m.deploymentReader()
	if reader == nil || m.cfg.AgentCloudControlClient == nil {
		return Deployment{}, false, ErrAgentCloudControlUnavailable
	}
	return reader.GetCloudDeployment(ctx, deploymentID)
}

func decodeAgentDeploymentDestroyApproval(value any) (agentCloudDeploymentDestroyApprovalV1, AgentCloudApprovalSignature, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return agentCloudDeploymentDestroyApprovalV1{}, AgentCloudApprovalSignature{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var approval agentCloudDeploymentDestroyApprovalV1
	if err = decoder.Decode(&approval); err != nil {
		return agentCloudDeploymentDestroyApprovalV1{}, AgentCloudApprovalSignature{}, err
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return agentCloudDeploymentDestroyApprovalV1{}, AgentCloudApprovalSignature{}, errors.New("cloud deployment destruction approval contains trailing JSON")
	}
	if approval.SchemaVersion != agentCloudDeploymentDestroyApprovalSchema || !canonicalUUID(approval.OperationID) ||
		!canonicalUUID(approval.ChallengeID) || !canonicalUUID(approval.ApprovalID) ||
		!cloudKeyIDPattern.MatchString(approval.SignerKeyID) || approval.Revision <= 0 || approval.ExpiresAt.Location() != time.UTC ||
		!validAgentDestroyScope(approval.Scope) {
		return agentCloudDeploymentDestroyApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize || base64.RawURLEncoding.EncodeToString(signature) != approval.Signature {
		return agentCloudDeploymentDestroyApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	return approval, AgentCloudApprovalSignature{
		ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
		ExpiresAt: approval.ExpiresAt, Signature: signature,
	}, nil
}

func validAgentDeploymentDestroyChallenge(value AgentCloudDeploymentDestroyChallenge, deployment Deployment, signerKeyID string, now time.Time) bool {
	return canonicalUUID(value.OperationID) && canonicalUUID(value.ChallengeID) && canonicalUUID(value.ApprovalID) &&
		value.SignerKeyID == signerKeyID && value.Revision > 0 && value.ExpiresAt.Location() == time.UTC && value.ExpiresAt.After(now) &&
		value.ExpiresAt.Sub(now) <= 6*time.Minute && len(value.SigningPayloadCBOR) > 0 && len(value.SigningPayloadCBOR) <= 256*1024 &&
		validAgentDestroyScope(value.Scope) && agentDestroyScopeMatchesDeployment(value.Scope, deployment, deployment.Revision)
}

func validAgentDestroyScope(scope AgentCloudDeploymentDestroyScope) bool {
	if scope.SchemaVersion != agentCloudDeploymentDestroyScopeSchema || !canonicalUUID(scope.AgentInstanceID) ||
		!agentCloudDestroyIDPattern.MatchString(scope.OwnerID) || !canonicalUUID(scope.DeploymentID) || scope.DeploymentRevision <= 0 ||
		!canonicalUUID(scope.TaskID) || !canonicalUUID(scope.PlanID) || !namedSHA256Pattern.MatchString(scope.PlanHash) ||
		!canonicalUUID(scope.ConnectionID) || len(scope.Resources) == 0 || len(scope.Resources) > 128 {
		return false
	}
	seen := make(map[string]AgentCloudDestroyResourceScope, len(scope.Resources))
	lastID := ""
	remaining := 0
	for _, resource := range scope.Resources {
		if !validAgentDestroyResource(resource) || resource.ResourceID <= lastID {
			return false
		}
		lastID = resource.ResourceID
		seen[resource.ResourceID] = resource
		if resource.Status != "verified_destroyed" {
			remaining++
		}
	}
	if remaining == 0 {
		return false
	}
	for _, resource := range scope.Resources {
		if !sort.StringsAreSorted(resource.DependsOnResourceIDs) {
			return false
		}
		lastDependency := ""
		for _, dependency := range resource.DependsOnResourceIDs {
			if dependency == resource.ResourceID || dependency == lastDependency {
				return false
			}
			if _, ok := seen[dependency]; !ok {
				return false
			}
			lastDependency = dependency
		}
	}
	return true
}

func validAgentDestroyResource(resource AgentCloudDestroyResourceScope) bool {
	if !canonicalUUID(resource.ResourceID) || resource.Revision <= 0 || !cloudRegionPattern.MatchString(resource.Region) ||
		resource.RetentionPolicy != "ephemeral_auto_destroy" || !agentDestroyableResourceScopeStatus(resource.Status) || resource.DestroyDeadline.Location() != time.UTC ||
		resource.DestroyDeadline.Unix() <= 0 || !resource.AutoDestroyApproved || !namedSHA256Pattern.MatchString(resource.SpecDigest) ||
		!namedSHA256Pattern.MatchString(resource.ApprovedPlanHash) || !canonicalUUID(resource.OriginalApprovalID) ||
		!resource.ReadBack.Observed || resource.ReadBack.Exists != (resource.Status != "verified_destroyed") || resource.ReadBack.ProviderID != resource.ProviderID ||
		resource.ReadBack.ObservedAt.Location() != time.UTC || resource.ReadBack.ObservedAt.Unix() <= 0 ||
		(resource.ReadBack.TagDigest != "" && !namedSHA256Pattern.MatchString(resource.ReadBack.TagDigest)) {
		return false
	}
	switch resource.Type {
	case "ec2":
		return agentCloudEC2ProviderIDPattern.MatchString(resource.ProviderID)
	case "ebs":
		return agentCloudEBSProviderIDPattern.MatchString(resource.ProviderID)
	case "eni":
		return agentCloudENIProviderIDPattern.MatchString(resource.ProviderID)
	case "security_group":
		return agentCloudSGProviderIDPattern.MatchString(resource.ProviderID)
	default:
		return false
	}
}

func validAgentDestroyDeployment(value Deployment) bool {
	return canonicalUUID(value.DeploymentID) && canonicalUUID(value.PlanID) && canonicalUUID(value.ConnectionID) && value.Revision > 0 &&
		value.CreatedAt > 0 && value.UpdatedAt >= value.CreatedAt && validAgentDestroyExecution(value.Execution) &&
		validAgentDestroyOutcome(value.Outcome) && validAgentDestroyResourceStatus(value.Resource)
}

func agentDestroyScopeMatchesDeployment(scope AgentCloudDeploymentDestroyScope, deployment Deployment, expectedRevision int64) bool {
	return scope.DeploymentID == deployment.DeploymentID && scope.DeploymentRevision == expectedRevision &&
		scope.PlanID == deployment.PlanID && scope.ConnectionID == deployment.ConnectionID &&
		deployment.Revision == expectedRevision && agentDestroyableDeploymentProjection(deployment.Resource) &&
		agentDestroyScopeProjection(scope.Resources) == deployment.Resource
}

func validAgentDestroyOperation(operation AgentCloudDestroyOperation, approval agentCloudDeploymentDestroyApprovalV1) bool {
	if !canonicalUUID(operation.OperationID) || operation.OperationID != approval.OperationID || !agentCloudDestroyIDPattern.MatchString(operation.OwnerID) ||
		operation.DeploymentID != approval.Scope.DeploymentID || operation.ApprovalID != approval.ApprovalID ||
		!namedSHA256Pattern.MatchString(operation.ScopeDigest) || operation.Revision <= 0 || operation.CreatedAt.Location() != time.UTC ||
		operation.UpdatedAt.Location() != time.UTC || operation.CreatedAt.Unix() <= 0 || operation.UpdatedAt.Before(operation.CreatedAt) {
		return false
	}
	switch operation.Status {
	case "awaiting_approval", "approved":
		return operation.ErrorCode == "" && operation.BlockedReason == "" && operation.AutomaticAttempts == 0 && operation.NextAttemptAt == nil && !operation.RequiresNewApproval
	case "destroying":
		return operation.AutomaticAttempts > 0 && operation.AutomaticAttempts <= 3 && operation.BlockedReason == "" && !operation.RequiresNewApproval &&
			((operation.NextAttemptAt == nil && operation.ErrorCode == "") || (operation.NextAttemptAt != nil && operation.NextAttemptAt.After(operation.UpdatedAt) && cloudKeyIDPattern.MatchString(operation.ErrorCode)))
	case "verified_destroyed":
		return operation.AutomaticAttempts > 0 && operation.AutomaticAttempts <= 3 && operation.ErrorCode == "" && operation.BlockedReason == "" && operation.NextAttemptAt == nil && !operation.RequiresNewApproval
	case "destroy_blocked":
		return cloudKeyIDPattern.MatchString(operation.ErrorCode) && validAgentDestroyText(operation.BlockedReason, 512) &&
			!ContainsSensitiveGoalMaterial(operation.BlockedReason) && operation.AutomaticAttempts > 0 && operation.AutomaticAttempts <= 3 && operation.NextAttemptAt == nil && operation.RequiresNewApproval
	default:
		return false
	}
}

func validAgentDestroyText(value string, maximum int) bool {
	return value != "" && value == strings.TrimSpace(value) && len(value) <= maximum
}

func validAgentDeploymentDestroyResult(result AgentCloudDeploymentDestroyResult, approval agentCloudDeploymentDestroyApprovalV1) bool {
	if !validAgentDestroyOperation(result.Operation, approval) || result.Operation.Status == "awaiting_approval" ||
		!validAgentDestroyDeployment(result.Deployment) || result.Deployment.DeploymentID != result.Operation.DeploymentID ||
		result.Deployment.PlanID != approval.Scope.PlanID || result.Deployment.ConnectionID != approval.Scope.ConnectionID ||
		result.Deployment.Revision < approval.Scope.DeploymentRevision {
		return false
	}
	if result.Operation.Status == "verified_destroyed" {
		return result.Deployment.Resource == "verified_destroyed"
	}
	return result.Deployment.Resource != "verified_destroyed"
}

func agentDeploymentDestroyResultView(result AgentCloudDeploymentDestroyResult) map[string]any {
	job := Job{
		JobID: result.Operation.OperationID, PlanID: result.Deployment.PlanID, DeploymentID: result.Deployment.DeploymentID,
		Kind: "destroy", Revision: result.Operation.Revision, CreatedAt: result.Operation.CreatedAt.UnixMilli(), UpdatedAt: result.Operation.UpdatedAt.UnixMilli(),
	}
	switch result.Operation.Status {
	case "approved":
		job.Execution, job.Outcome, job.Checkpoint = "queued", "pending", "approved"
	case "destroying":
		job.Execution, job.Outcome, job.Checkpoint = "running", "pending", "destroying"
	case "verified_destroyed":
		job.Execution, job.Outcome, job.Checkpoint = "finished", "succeeded", "verified_destroyed"
	case "destroy_blocked":
		job.Execution, job.Outcome, job.Checkpoint, job.ErrorCode = "finished", "failed", "destroy_blocked", result.Operation.ErrorCode
	}
	return map[string]any{"deployment": result.Deployment, "job": job}
}

func validAgentDestroyExecution(value string) bool {
	switch value {
	case "queued", "running", "waiting_user", "verifying", "finished":
		return true
	default:
		return false
	}
}

func validAgentDestroyOutcome(value string) bool {
	switch value {
	case "pending", "succeeded", "failed", "canceled", "timed_out", "interrupted":
		return true
	default:
		return false
	}
}

func validAgentDestroyResourceStatus(value string) bool {
	switch value {
	case "active", "destroy_scheduled", "destroying", "verified_destroyed", "destroy_blocked", "orphaned", "mixed":
		return true
	default:
		return false
	}
}

func agentDestroyableDeploymentProjection(value string) bool {
	switch value {
	case "active", "destroy_scheduled", "destroy_blocked", "mixed":
		return true
	default:
		return false
	}
}

func agentDestroyableResourceScopeStatus(value string) bool {
	switch value {
	case "active", "destroy_scheduled", "destroy_blocked", "verified_destroyed":
		return true
	default:
		return false
	}
}

func agentDestroyScopeProjection(resources []AgentCloudDestroyResourceScope) string {
	if len(resources) == 0 {
		return ""
	}
	status := resources[0].Status
	for _, resource := range resources[1:] {
		if resource.Status != status {
			return "mixed"
		}
	}
	return status
}

func agentDeploymentDestroyError(err error, found bool) *actionbase.Error {
	switch {
	case !found, errors.Is(err, ErrAgentCloudControlConflict):
		return actionbase.CodedError(http.StatusConflict, cloudDeploymentDestroyConflictCode, "cloud deployment destruction conflicts with current Agent state")
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudDeploymentDestroyInvalidCode, "cloud deployment destruction is invalid")
	case errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusUnauthorized, cloudDeploymentDestroySignatureCode, "cloud deployment destruction approval signature is invalid")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudDeploymentDestroyInvalidCode, "cloud Agent returned an invalid deployment destruction result")
	default:
		return actionbase.CodedError(http.StatusServiceUnavailable, cloudUnavailableCode, "cloud Agent control is unavailable")
	}
}
