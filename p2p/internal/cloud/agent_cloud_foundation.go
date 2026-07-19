package cloud

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

const agentFoundationApprovalSchema = "dirextalk.message-server.aws-foundation-approval/v1"

var agentFoundationDigestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

type agentFoundationApprovalV1 struct {
	SchemaVersion     string                    `json:"schema_version"`
	OperationID       string                    `json:"operation_id"`
	ApprovalID        string                    `json:"approval_id"`
	ChallengeID       string                    `json:"challenge_id"`
	SignerKeyID       string                    `json:"signer_key_id"`
	OwnerID           string                    `json:"owner_id"`
	Action            string                    `json:"action"`
	ConnectionID      string                    `json:"cloud_connection_id"`
	ScopeDigest       string                    `json:"scope_digest"`
	OperationRevision int64                     `json:"operation_revision"`
	Scope             AgentCloudFoundationScope `json:"scope"`
	ExpiresAt         time.Time                 `json:"expires_at"`
	Signature         string                    `json:"signature,omitempty"`
}

func (m *Module) prepareAgentFoundationConfirmation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "action", "cloud_connection_id", "bootstrap_session_id", "expected_bootstrap_revision", "signer_key_id", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentFoundationClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	request := AgentCloudFoundationChallengeRequest{IdempotencyKey: values.String("idempotency_key"), Action: values.String("action"),
		ConnectionID: values.String("cloud_connection_id"), BootstrapSessionID: values.String("bootstrap_session_id"),
		ExpectedBootstrapRevision: values.Int64("expected_bootstrap_revision"), SignerKeyID: values.String("signer_key_id")}
	if !validAgentFoundationAction(request.Action) || !canonicalUUID(request.IdempotencyKey) || !canonicalUUID(request.ConnectionID) || !canonicalUUID(request.BootstrapSessionID) ||
		request.ExpectedBootstrapRevision <= 0 || !cloudKeyIDPattern.MatchString(request.SignerKeyID) || ContainsSensitiveGoalMaterial(request.SignerKeyID) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud Foundation confirmation is invalid")
	}
	challenge, err := client.CreateAgentAWSFoundationChallenge(ctx, request)
	if err != nil {
		return nil, agentFoundationError(err, true)
	}
	if validateAgentFoundationChallenge(challenge, request, m.ownerMXID(), m.now().UTC()) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid Foundation challenge")
	}
	approval := agentFoundationApprovalV1{SchemaVersion: agentFoundationApprovalSchema, OperationID: challenge.OperationID, ApprovalID: challenge.ApprovalID,
		ChallengeID: challenge.ChallengeID, SignerKeyID: challenge.SignerKeyID, OwnerID: challenge.Scope.OwnerID, Action: challenge.Scope.Action,
		ConnectionID: challenge.Scope.ConnectionID, ScopeDigest: challenge.ScopeDigest, OperationRevision: challenge.Revision, Scope: challenge.Scope, ExpiresAt: challenge.ExpiresAt}
	payloadDigest := sha256.Sum256(challenge.SigningPayloadCBOR)
	return map[string]any{"confirmation": map[string]any{
		"approval": approval, "signing_payload_cbor": base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR),
		"signing_payload_digest": "sha256:" + hex.EncodeToString(payloadDigest[:]),
	}}, nil
}

func (m *Module) approveAgentFoundation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentFoundationClient()
	if !ok {
		return nil, unavailableError()
	}
	approval, signature, err := decodeAgentFoundationApproval(params["approval"])
	idempotencyKey := actionbase.Params(params).String("idempotency_key")
	owner := m.ownerMXID()
	if err != nil || !canonicalUUID(idempotencyKey) || owner == "" || approval.OwnerID != owner || approval.OperationRevision != 1 ||
		!m.now().UTC().Before(approval.ExpiresAt) || validateAgentFoundationScope(approval.Scope, owner) != nil ||
		approval.Scope.Action != approval.Action || approval.Scope.ConnectionID != approval.ConnectionID {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud Foundation approval is invalid")
	}
	request := AgentCloudFoundationApproveRequest{IdempotencyKey: idempotencyKey, ExpectedOperationID: approval.OperationID,
		ExpectedAction: approval.Action, ExpectedConnectionID: approval.ConnectionID, ExpectedScopeDigest: approval.ScopeDigest,
		ExpectedRevision: approval.OperationRevision, Approval: signature}
	operation, callErr := client.ApproveAgentAWSFoundation(ctx, request)
	if callErr == nil && validateAgentFoundationApprovedOperation(operation, approval) == nil {
		return map[string]any{"operation": agentFoundationOperationView(operation)}, nil
	}
	// Approval is durable in Agent. On a lost response, only recover the exact
	// owner-bound operation and never retry with a possibly consumed challenge.
	recovered, found, getErr := client.GetAgentAWSFoundationOperation(ctx, AgentCloudFoundationOperationRequest{OperationID: approval.OperationID})
	if getErr == nil && found && validateAgentFoundationApprovedOperation(recovered, approval) == nil {
		return map[string]any{"operation": agentFoundationOperationView(recovered)}, nil
	}
	if callErr == nil {
		callErr = ErrAgentCloudControlInvalidResponse
	}
	return nil, agentFoundationError(callErr, true)
}

func (m *Module) getAgentFoundationOperation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "operation_id"); err != nil {
		return nil, err
	}
	client, ok := m.agentFoundationClient()
	if !ok {
		return nil, unavailableError()
	}
	operationID := actionbase.Params(params).String("operation_id")
	if !canonicalUUID(operationID) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionIDInvalidCode, "cloud Foundation operation id is invalid")
	}
	operation, found, err := client.GetAgentAWSFoundationOperation(ctx, AgentCloudFoundationOperationRequest{OperationID: operationID})
	if err != nil || !found {
		return nil, agentFoundationError(err, found)
	}
	if operation.OwnerID != m.ownerMXID() || validateAgentFoundationOperation(operation) != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid Foundation operation")
	}
	return map[string]any{"operation": agentFoundationOperationView(operation)}, nil
}

func (m *Module) agentFoundationClient() (AgentCloudFoundationClient, bool) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, false
	}
	client, ok := m.cfg.AgentCloudControlClient.(AgentCloudFoundationClient)
	return client, ok && client != nil
}

func decodeAgentFoundationApproval(raw any) (agentFoundationApprovalV1, AgentCloudApprovalSignature, error) {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) > 64*1024 {
		return agentFoundationApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	var approval agentFoundationApprovalV1
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&approval); err != nil {
		return agentFoundationApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return agentFoundationApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	if approval.SchemaVersion != agentFoundationApprovalSchema || !canonicalUUID(approval.OperationID) || !canonicalUUID(approval.ApprovalID) || !canonicalUUID(approval.ChallengeID) ||
		!cloudKeyIDPattern.MatchString(approval.SignerKeyID) || !validAgentFoundationAction(approval.Action) || !canonicalUUID(approval.ConnectionID) ||
		!agentFoundationDigestPattern.MatchString(approval.ScopeDigest) || approval.OperationRevision != 1 || approval.ExpiresAt.IsZero() || err != nil || len(signature) != 64 {
		return agentFoundationApprovalV1{}, AgentCloudApprovalSignature{}, ErrAgentCloudControlInvalid
	}
	return approval, AgentCloudApprovalSignature{ApprovalID: approval.ApprovalID, ChallengeID: approval.ChallengeID, SignerKeyID: approval.SignerKeyID,
		ExpiresAt: approval.ExpiresAt.UTC(), Signature: signature}, nil
}

func validateAgentFoundationChallenge(value AgentCloudFoundationChallenge, request AgentCloudFoundationChallengeRequest, owner string, now time.Time) error {
	if !canonicalUUID(value.OperationID) || !canonicalUUID(value.ChallengeID) || !canonicalUUID(value.ApprovalID) || value.SignerKeyID != request.SignerKeyID ||
		!agentFoundationDigestPattern.MatchString(value.ScopeDigest) || value.Revision != 1 || len(value.SigningPayloadCBOR) == 0 || len(value.SigningPayloadCBOR) > 64*1024 ||
		!now.Before(value.ExpiresAt) || value.ExpiresAt.Sub(now) > 5*time.Minute || validateAgentFoundationScope(value.Scope, owner) != nil ||
		!now.Before(value.Scope.IdentityExpiresAt) || value.Scope.IdentityObservedAt.After(now.Add(30*time.Second)) ||
		value.Scope.Action != request.Action || value.Scope.ConnectionID != request.ConnectionID || value.Scope.BootstrapSessionID != request.BootstrapSessionID ||
		value.Scope.ExpectedBootstrapRevision != request.ExpectedBootstrapRevision {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateAgentFoundationScope(scope AgentCloudFoundationScope, owner string) error {
	environment := scope.ReleaseEnvironment
	if scope.SchemaVersion != "dirextalk.agent.aws-foundation-operation-scope/v1" || !canonicalUUID(scope.AgentInstanceID) || scope.OwnerID != owner || !validAgentFoundationAction(scope.Action) ||
		!canonicalUUID(scope.ConnectionID) || !canonicalUUID(scope.BootstrapSessionID) || !awsAccountIDPattern.MatchString(scope.AccountID) || !cloudRegionPattern.MatchString(scope.Region) ||
		scope.ExpectedBootstrapRevision <= 0 || !agentFoundationDigestPattern.MatchString(scope.FoundationTemplateDigest) || !strings.Contains(scope.ReaperImageURI, "@sha256:") ||
		scope.IdentityObservedAt.IsZero() || !scope.IdentityObservedAt.Before(scope.IdentityExpiresAt) || scope.IdentityExpiresAt.Sub(scope.IdentityObservedAt) > 10*time.Minute ||
		environment.PrivateSubnetCIDR != "10.255.0.0/26" || !environment.ZeroIngress || !environment.BucketVersioned || !environment.BucketSSEKMS ||
		strings.TrimSpace(environment.ArtifactBucket) == "" || !strings.HasPrefix(environment.KMSAlias, "alias/dtx-agent-") {
		return ErrAgentCloudControlInvalidResponse
	}
	if (scope.Action == "establish" && (scope.ExpectedConnectionRevision != 0 || scope.ExpectedCredentialGeneration != 0)) ||
		(scope.Action != "establish" && (scope.ExpectedConnectionRevision <= 0 || scope.ExpectedCredentialGeneration <= 0)) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateAgentFoundationApprovedOperation(operation AgentCloudFoundationOperation, approval agentFoundationApprovalV1) error {
	if validateAgentFoundationOperation(operation) != nil || operation.OperationID != approval.OperationID || operation.OwnerID != approval.OwnerID ||
		operation.ConnectionID != approval.ConnectionID || operation.Action != approval.Action || operation.ApprovalID != approval.ApprovalID ||
		operation.ScopeDigest != approval.ScopeDigest || operation.Status == "awaiting_approval" || operation.Revision < approval.OperationRevision+1 {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateAgentFoundationOperation(value AgentCloudFoundationOperation) error {
	if !canonicalUUID(value.OperationID) || strings.TrimSpace(value.OwnerID) == "" || !canonicalUUID(value.ConnectionID) || !validAgentFoundationAction(value.Action) ||
		!canonicalUUID(value.ApprovalID) || !agentFoundationDigestPattern.MatchString(value.ScopeDigest) || !validAgentFoundationStatus(value.Status) || value.Revision <= 0 ||
		value.CreatedAt.IsZero() || value.UpdatedAt.Before(value.CreatedAt) || ContainsSensitiveGoalMaterial(value.ErrorCode) || ContainsSensitiveGoalMaterial(value.BlockedReason) {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validAgentFoundationAction(value string) bool {
	return value == "establish" || value == "upgrade" || value == "teardown" || value == "remediate_destroy_blocked"
}

func validAgentFoundationExistingConnectionAction(value string) bool {
	return value == "upgrade" || value == "teardown" || value == "remediate_destroy_blocked"
}

func (m *Module) loadAgentFoundationConnection(ctx context.Context, action, connectionID string, expectedRevision int64) (AgentCloudConnection, *actionbase.Error) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return AgentCloudConnection{}, unavailableError()
	}
	if !validAgentFoundationExistingConnectionAction(action) || !canonicalUUID(connectionID) || expectedRevision <= 0 {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusBadRequest, cloudConnectionIDInvalidCode, "cloud Foundation connection request is invalid")
	}
	connection, found, err := m.cfg.AgentCloudControlClient.GetAgentCloudConnection(ctx, AgentCloudConnectionRequest{ConnectionID: connectionID})
	if err != nil {
		return AgentCloudConnection{}, agentFoundationError(err, true)
	}
	if !found {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud Foundation connection does not exist")
	}
	if validateReadableAgentCloudConnection(connection, connectionID) != nil || connection.OwnerID != m.ownerMXID() {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid Foundation connection")
	}
	if !cloudRegionPattern.MatchString(connection.Region) || ContainsSensitiveGoalMaterial(connection.Region) ||
		ContainsSensitiveGoalMaterial(connection.AccountID) {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid Foundation connection")
	}
	if connection.Revision != expectedRevision {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud Foundation connection revision conflicts with current Agent state")
	}
	allowedStatus := connection.Status == "active" || connection.Status == "degraded" || connection.Status == "teardown_blocked"
	if action == "remediate_destroy_blocked" {
		allowedStatus = connection.Status == "teardown_blocked"
	}
	if !allowedStatus {
		return AgentCloudConnection{}, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud Foundation action conflicts with current Agent state")
	}
	return connection, nil
}

func validAgentFoundationStatus(value string) bool {
	switch value {
	case "awaiting_approval", "approved", "running", "succeeded", "failed_retriable", "failed_terminal", "destroy_blocked":
		return true
	default:
		return false
	}
}

func agentFoundationOperationView(value AgentCloudFoundationOperation) map[string]any {
	return map[string]any{"operation_id": value.OperationID, "cloud_connection_id": value.ConnectionID, "action": value.Action, "approval_id": value.ApprovalID,
		"scope_digest": value.ScopeDigest, "status": value.Status, "error_code": value.ErrorCode, "blocked_reason": value.BlockedReason, "revision": value.Revision,
		"created_at": value.CreatedAt.UTC().Format(time.RFC3339Nano), "updated_at": value.UpdatedAt.UTC().Format(time.RFC3339Nano)}
}

func agentFoundationError(err error, found bool) *actionbase.Error {
	switch {
	case !found:
		return actionbase.CodedError(http.StatusNotFound, cloudConnectionIDInvalidCode, "cloud Foundation operation was not found")
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudConnectionBootstrapInvalidCode, "cloud Foundation request is invalid")
	case errors.Is(err, ErrAgentCloudControlConflict), errors.Is(err, ErrAgentCloudControlRejected):
		return actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud Foundation request conflicts with current Agent state")
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudConnectionBootstrapInvalidCode, "cloud Agent returned an invalid Foundation response")
	default:
		return unavailableError()
	}
}
