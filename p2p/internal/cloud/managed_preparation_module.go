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
	"reflect"
	"strconv"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/fxamacker/cbor/v2"
)

func (m *Module) prepareManagedPreparation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "cost_alert_amount_minor", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentManagedPreparationClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision, costAmount := values.Int64("expected_revision"), values.Int64("cost_alert_amount_minor")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || costAmount <= 0 ||
		!canonicalUUID(idempotencyKey) || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, managedPreparationRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision {
		return nil, managedPreparationConflictError()
	}
	request := AgentCloudManagedPreparationCreateRequest{
		IdempotencyKey: idempotencyKey, DeploymentID: compatibility.DeploymentID, SignerKeyID: compatibility.SignerKeyID,
		ExpectedDeploymentRevision: expectedRevision, CostAlertAmountMinor: costAmount,
	}
	challenge, err := client.CreateCloudManagedPreparation(ctx, request)
	if err != nil {
		return nil, managedPreparationAgentError(err, true)
	}
	if validateManagedPreparationChallenge(challenge, m.ownerMXID(), compatibility, costAmount, m.now().UTC()) != nil {
		return nil, managedPreparationInvalidResponseError()
	}
	approval := managedPreparationApprovalV1{
		SchemaVersion: challenge.SchemaVersion, ChallengeID: challenge.ChallengeID, OperationID: challenge.OperationID,
		SignerKeyID: challenge.SignerKeyID, Scope: challenge.Scope, ScopeDigest: challenge.ScopeDigest,
		IssuedAt: challenge.IssuedAt, ExpiresAt: challenge.ExpiresAt, OperationRevision: challenge.Revision,
	}
	return map[string]any{"confirmation": map[string]any{
		"service_id": serviceID, "approval": approval,
		"signing_payload_cbor":   base64.RawURLEncoding.EncodeToString(challenge.SigningPayloadCBOR),
		"signing_payload_digest": challenge.ScopeDigest,
	}}, nil
}

func (m *Module) approveManagedPreparation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	client, ok := m.agentManagedPreparationClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	approval, signature, err := decodeManagedPreparationApproval(params["approval"])
	if err != nil || !cloudIdentifierPattern.MatchString(serviceID) ||
		expectedRevision <= 0 || !canonicalUUID(idempotencyKey) || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, managedPreparationRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision || approval.Scope.DeploymentRevision != expectedRevision ||
		approval.Scope.DeploymentID != compatibility.DeploymentID {
		return nil, managedPreparationConflictError()
	}
	request := AgentCloudManagedPreparationApproveRequest{
		IdempotencyKey: idempotencyKey, OperationID: approval.OperationID, DeploymentID: compatibility.DeploymentID,
		ScopeDigest: approval.ScopeDigest, ExpectedRevision: approval.OperationRevision, Approval: signature,
	}
	recovered, found, getErr := client.GetCloudManagedPreparation(ctx, AgentCloudManagedPreparationGetRequest{OperationID: approval.OperationID})
	if getErr != nil {
		return nil, managedPreparationAgentError(getErr, true)
	}
	if found {
		if validateManagedPreparationOperation(recovered, m.ownerMXID(), compatibility, &approval) != nil {
			return nil, managedPreparationInvalidResponseError()
		}
		return map[string]any{"operation": managedPreparationOperationView(serviceID, recovered)}, nil
	}
	// A clean owner-bound NotFound is the only state in which the signature may
	// be sent. Exact readback remains available after expiry or signer rotation.
	if !m.now().UTC().Before(approval.ExpiresAt) || approval.SignerKeyID != compatibility.SignerKeyID {
		return nil, managedPreparationConflictError()
	}
	operation, callErr := client.ApproveCloudManagedPreparation(ctx, request)
	if callErr == nil && validateManagedPreparationOperation(operation, m.ownerMXID(), compatibility, &approval) == nil {
		return map[string]any{"operation": managedPreparationOperationView(serviceID, operation)}, nil
	}
	// Never resend a possibly consumed signature after an unknown result.
	recovered, found, getErr = client.GetCloudManagedPreparation(ctx, AgentCloudManagedPreparationGetRequest{OperationID: approval.OperationID})
	if getErr != nil {
		return nil, managedPreparationAgentError(getErr, true)
	}
	if found {
		if validateManagedPreparationOperation(recovered, m.ownerMXID(), compatibility, &approval) != nil {
			return nil, managedPreparationInvalidResponseError()
		}
		return map[string]any{"operation": managedPreparationOperationView(serviceID, recovered)}, nil
	}
	if callErr == nil {
		callErr = ErrAgentCloudControlInvalidResponse
	}
	return nil, managedPreparationAgentError(callErr, true)
}

func (m *Module) getManagedPreparation(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "operation_id"); err != nil {
		return nil, err
	}
	client, ok := m.agentManagedPreparationClient()
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, operationID := values.String("service_id"), values.String("operation_id")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || !canonicalUUID(operationID) || expectedRevision <= 0 {
		return nil, managedPreparationRequestError()
	}
	compatibility, apiErr := m.loadManagedAcceptanceCompatibility(ctx, serviceID)
	if apiErr != nil {
		return nil, apiErr
	}
	if compatibility.DeploymentRevision != expectedRevision {
		return nil, managedPreparationConflictError()
	}
	operation, found, err := client.GetCloudManagedPreparation(ctx, AgentCloudManagedPreparationGetRequest{OperationID: operationID})
	if err != nil || !found {
		return nil, managedPreparationAgentError(err, found)
	}
	if validateManagedPreparationOperation(operation, m.ownerMXID(), compatibility, nil) != nil {
		return nil, managedPreparationInvalidResponseError()
	}
	return map[string]any{"operation": managedPreparationOperationView(serviceID, operation)}, nil
}

func (m *Module) agentManagedPreparationClient() (AgentCloudManagedPreparationClient, bool) {
	if m == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, false
	}
	client, ok := m.cfg.AgentCloudControlClient.(AgentCloudManagedPreparationClient)
	return client, ok && client != nil
}

func validateManagedPreparationChallenge(value AgentCloudManagedPreparationChallenge, owner string, compatibility ManagedAcceptanceCompatibility, cost int64, now time.Time) error {
	sum := sha256.Sum256(value.SigningPayloadCBOR)
	scope := value.Scope
	if _, schemaOK := managedPreparationPayloadSchema(value.SchemaVersion, scope.SchemaVersion); !schemaOK ||
		!validManagedPreparationScopeSchema(scope) || value.Revision != 1 || !canonicalUUID(value.OperationID) || !canonicalUUID(value.ChallengeID) ||
		value.OperationID != scope.PreparationOperationID || value.SignerKeyID != compatibility.SignerKeyID ||
		value.ScopeDigest != "sha256:"+hex.EncodeToString(sum[:]) || !agentFoundationDigestPattern.MatchString(value.ScopeDigest) ||
		len(value.SigningPayloadCBOR) == 0 || len(value.SigningPayloadCBOR) > 64*1024 ||
		value.IssuedAt.IsZero() || value.IssuedAt.Location() != time.UTC || value.ExpiresAt.Location() != time.UTC ||
		!value.ExpiresAt.After(value.IssuedAt) || value.ExpiresAt.Sub(value.IssuedAt) > 5*time.Minute || !now.Before(value.ExpiresAt) ||
		scope.Intent != managedPreparationIntent || scope.OwnerID != owner ||
		scope.DeploymentID != compatibility.DeploymentID || scope.DeploymentRevision != compatibility.DeploymentRevision ||
		scope.CostAlertAmountMinor != cost {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateManagedPreparationOperation(value AgentCloudManagedPreparationOperation, owner string, compatibility ManagedAcceptanceCompatibility, approval *managedPreparationApprovalV1) error {
	if value.OperationID != value.Challenge.OperationID || value.Challenge.Scope.OwnerID != owner ||
		value.Challenge.Scope.DeploymentID != compatibility.DeploymentID ||
		value.Challenge.Scope.DeploymentRevision != compatibility.DeploymentRevision ||
		value.Revision < 1 || value.CreatedAt.IsZero() || value.CreatedAt.Location() != time.UTC ||
		value.UpdatedAt.Location() != time.UTC || value.UpdatedAt.Before(value.CreatedAt) ||
		validateManagedPreparationChallenge(value.Challenge, owner, compatibility, value.Challenge.Scope.CostAlertAmountMinor, value.Challenge.IssuedAt) != nil ||
		!validManagedPreparationStatus(value.Status) || !validManagedPreparationPhase(value.CurrentPhase) ||
		len(value.Steps) != len(managedPreparationPhases) {
		return ErrAgentCloudControlInvalidResponse
	}
	if approval != nil && (value.OperationID != approval.OperationID || value.Challenge.ChallengeID != approval.ChallengeID ||
		value.Challenge.SignerKeyID != approval.SignerKeyID || value.Challenge.ScopeDigest != approval.ScopeDigest ||
		!reflect.DeepEqual(value.Challenge.Scope, approval.Scope) || !value.Challenge.IssuedAt.Equal(approval.IssuedAt) ||
		!value.Challenge.ExpiresAt.Equal(approval.ExpiresAt) || value.Challenge.Revision != approval.OperationRevision) {
		return ErrAgentCloudControlInvalidResponse
	}
	for index, step := range value.Steps {
		if step.Phase != managedPreparationPhases[index] || step.Ordinal != int32(index+1) || step.Revision < 1 ||
			(step.Status != "pending" && step.Status != "running" && step.Status != "succeeded") ||
			(step.StartedAt != nil && step.StartedAt.Location() != time.UTC) ||
			(step.CompletedAt != nil && step.CompletedAt.Location() != time.UTC) ||
			(step.StartedAt != nil && step.CompletedAt != nil && step.CompletedAt.Before(*step.StartedAt)) {
			return ErrAgentCloudControlInvalidResponse
		}
	}
	if value.Status == "succeeded" {
		if value.Result == nil || validateManagedPreparationResult(*value.Result) != nil {
			return ErrAgentCloudControlInvalidResponse
		}
	} else if value.Result != nil {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validateManagedPreparationResult(value AgentCloudManagedPreparationResult) error {
	if !canonicalUUID(value.PreparationID) || !agentFoundationDigestPattern.MatchString(value.PreparationDigest) ||
		!agentFoundationDigestPattern.MatchString(value.FreshHealthDigest) || value.FreshHealthRevision <= 0 ||
		value.FreshHealthObservedAt.IsZero() || value.FreshHealthObservedAt.Location() != time.UTC ||
		!agentFoundationDigestPattern.MatchString(value.CostDigest) || value.CostPolicyRevision <= 0 ||
		value.CostObservedAt.IsZero() || value.CostObservedAt.Location() != time.UTC ||
		!agentFoundationDigestPattern.MatchString(value.StackDigest) || value.StackRevision <= 0 ||
		value.StackObservedAt.IsZero() || value.StackObservedAt.Location() != time.UTC {
		return ErrAgentCloudControlInvalidResponse
	}
	return nil
}

func validManagedPreparationStatus(value string) bool {
	return value == "awaiting_approval" || value == "approved" || value == "running" || value == "succeeded" || value == "failed_terminal"
}

func validManagedPreparationPhase(value string) bool {
	for _, phase := range managedPreparationPhases {
		if value == phase {
			return true
		}
	}
	return false
}

func decodeManagedPreparationApproval(raw any) (managedPreparationApprovalV1, AgentCloudManagedPreparationSignature, error) {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) > 64*1024 {
		return managedPreparationApprovalV1{}, AgentCloudManagedPreparationSignature{}, ErrAgentCloudControlInvalid
	}
	var approval managedPreparationApprovalV1
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&approval); err != nil {
		return managedPreparationApprovalV1{}, AgentCloudManagedPreparationSignature{}, ErrAgentCloudControlInvalid
	}
	if err = decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return managedPreparationApprovalV1{}, AgentCloudManagedPreparationSignature{}, ErrAgentCloudControlInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(approval.Signature)
	payload, payloadErr := managedPreparationApprovalSigningPayload(approval)
	payloadSum := sha256.Sum256(payload)
	if _, schemaOK := managedPreparationPayloadSchema(approval.SchemaVersion, approval.Scope.SchemaVersion); !schemaOK ||
		!v1ManagedPreparationApprovalOmitsV2Terms(encoded, approval) ||
		!validManagedPreparationScopeSchema(approval.Scope) || !canonicalUUID(approval.ChallengeID) ||
		!canonicalUUID(approval.OperationID) || approval.OperationID != approval.Scope.PreparationOperationID ||
		!cloudKeyIDPattern.MatchString(approval.SignerKeyID) || approval.SignerKeyID == "" ||
		!agentFoundationDigestPattern.MatchString(approval.ScopeDigest) ||
		approval.OperationRevision != 1 || approval.IssuedAt.Location() != time.UTC || approval.ExpiresAt.Location() != time.UTC ||
		!approval.ExpiresAt.After(approval.IssuedAt) || approval.ExpiresAt.Sub(approval.IssuedAt) > 5*time.Minute ||
		err != nil || len(signature) != 64 || payloadErr != nil ||
		approval.ScopeDigest != "sha256:"+hex.EncodeToString(payloadSum[:]) {
		return managedPreparationApprovalV1{}, AgentCloudManagedPreparationSignature{}, ErrAgentCloudControlInvalid
	}
	return approval, AgentCloudManagedPreparationSignature{
		ApprovalID: approval.OperationID, ChallengeID: approval.ChallengeID, OperationID: approval.OperationID,
		SignerKeyID: approval.SignerKeyID, ExpiresAt: approval.ExpiresAt, Signature: signature,
	}, nil
}

type managedPreparationApprovalSigningPayloadV1 struct {
	SchemaVersion  string                            `json:"schema_version"`
	PayloadVersion string                            `json:"payload_version"`
	Intent         string                            `json:"intent"`
	ChallengeID    string                            `json:"challenge_id"`
	OperationID    string                            `json:"operation_id"`
	SignerKeyID    string                            `json:"signer_key_id"`
	Scope          AgentCloudManagedPreparationScope `json:"scope"`
	IssuedAt       time.Time                         `json:"issued_at"`
	ExpiresAt      time.Time                         `json:"expires_at"`
}

func managedPreparationApprovalSigningPayload(value managedPreparationApprovalV1) ([]byte, error) {
	payloadVersion, schemaOK := managedPreparationPayloadSchema(value.SchemaVersion, value.Scope.SchemaVersion)
	if !schemaOK || !validManagedPreparationScopeSchema(value.Scope) {
		return nil, ErrAgentCloudControlInvalid
	}
	document := managedPreparationApprovalSigningPayloadV1{
		SchemaVersion: value.SchemaVersion, PayloadVersion: payloadVersion,
		Intent: managedPreparationIntent, ChallengeID: value.ChallengeID, OperationID: value.OperationID,
		SignerKeyID: value.SignerKeyID, Scope: value.Scope, IssuedAt: value.IssuedAt.UTC(), ExpiresAt: value.ExpiresAt.UTC(),
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var projected any
	if err = decoder.Decode(&projected); err != nil {
		return nil, err
	}
	projected, err = normalizeManagedPreparationJSONNumbers(projected)
	if err != nil {
		return nil, err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal(projected)
}

func managedPreparationPayloadSchema(challengeSchema, scopeSchema string) (string, bool) {
	switch {
	case challengeSchema == managedPreparationChallengeSchemaV1 && scopeSchema == managedPreparationScopeSchemaV1:
		return managedPreparationSigningPayloadV1, true
	case challengeSchema == managedPreparationChallengeSchemaV2 && scopeSchema == managedPreparationScopeSchemaV2:
		return managedPreparationSigningPayloadV2, true
	default:
		return "", false
	}
}

func validManagedPreparationScopeSchema(scope AgentCloudManagedPreparationScope) bool {
	isV2 := scope.SchemaVersion == managedPreparationScopeSchemaV2
	for _, volume := range scope.Volumes {
		if isV2 {
			if !managedPreparationSafeIdentifierPattern.MatchString(volume.SnapshotOperationKey) ||
				!agentFoundationDigestPattern.MatchString(volume.SnapshotSourceVolumeScopeDigest) ||
				volume.SnapshotMaxRetentionSeconds == 0 ||
				volume.SnapshotMaxRetentionSeconds > managedPreparationMaxSnapshotRetentionSeconds {
				return false
			}
			continue
		}
		if volume.SnapshotOperationKey != "" || volume.SnapshotSourceVolumeScopeDigest != "" ||
			volume.SnapshotMaxRetentionSeconds != 0 {
			return false
		}
	}
	return true
}

// V1 is a frozen signing contract. A JSON client must not be able to smuggle
// even zero-valued V2 keys into a V1 approval and have them silently omitted
// while Message Server re-creates the V1 CBOR payload.
func v1ManagedPreparationApprovalOmitsV2Terms(encoded []byte, approval managedPreparationApprovalV1) bool {
	if approval.SchemaVersion != managedPreparationChallengeSchemaV1 || approval.Scope.SchemaVersion != managedPreparationScopeSchemaV1 {
		return true
	}
	var document struct {
		Scope struct {
			Volumes []map[string]json.RawMessage `json:"volumes"`
		} `json:"scope"`
	}
	if err := json.Unmarshal(encoded, &document); err != nil {
		return false
	}
	for _, volume := range document.Scope.Volumes {
		for _, key := range []string{"snapshot_operation_key", "snapshot_source_volume_scope_digest", "snapshot_max_retention_seconds"} {
			if _, present := volume[key]; present {
				return false
			}
		}
	}
	return true
}

func normalizeManagedPreparationJSONNumbers(value any) (any, error) {
	switch value := value.(type) {
	case json.Number:
		if signed, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return nil, err
		}
		return unsigned, nil
	case []any:
		for index, item := range value {
			normalized, err := normalizeManagedPreparationJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[index] = normalized
		}
		return value, nil
	case map[string]any:
		for key, item := range value {
			normalized, err := normalizeManagedPreparationJSONNumbers(item)
			if err != nil {
				return nil, err
			}
			value[key] = normalized
		}
		return value, nil
	default:
		return value, nil
	}
}

func managedPreparationOperationView(serviceID string, value AgentCloudManagedPreparationOperation) map[string]any {
	steps := make([]map[string]any, 0, len(value.Steps))
	for _, value := range value.Steps {
		step := map[string]any{"phase": value.Phase, "ordinal": value.Ordinal, "status": value.Status, "revision": value.Revision}
		if value.StartedAt != nil {
			step["started_at"] = value.StartedAt.UTC().Format(time.RFC3339Nano)
		}
		if value.CompletedAt != nil {
			step["completed_at"] = value.CompletedAt.UTC().Format(time.RFC3339Nano)
		}
		steps = append(steps, step)
	}
	view := map[string]any{
		"operation_id": value.OperationID, "service_id": serviceID, "deployment_id": value.Challenge.Scope.DeploymentID,
		"scope_digest": value.Challenge.ScopeDigest, "status": value.Status, "current_phase": value.CurrentPhase,
		"revision": value.Revision, "steps": steps, "created_at": value.CreatedAt.UTC().Format(time.RFC3339Nano),
		"updated_at": value.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if value.ApprovedAt != nil {
		view["approved_at"] = value.ApprovedAt.UTC().Format(time.RFC3339Nano)
	}
	if value.Result != nil {
		view["result"] = map[string]any{
			"preparation_id": value.Result.PreparationID, "preparation_digest": value.Result.PreparationDigest,
			"health": map[string]any{"digest": value.Result.FreshHealthDigest, "revision": value.Result.FreshHealthRevision,
				"observed_at": value.Result.FreshHealthObservedAt.UTC().Format(time.RFC3339Nano)},
			"cost": map[string]any{"digest": value.Result.CostDigest, "policy_revision": value.Result.CostPolicyRevision,
				"observed_at": value.Result.CostObservedAt.UTC().Format(time.RFC3339Nano)},
			"stack": map[string]any{"digest": value.Result.StackDigest, "revision": value.Result.StackRevision,
				"observed_at": value.Result.StackObservedAt.UTC().Format(time.RFC3339Nano)},
		}
	}
	return view
}

func managedPreparationAgentError(err error, found bool) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentCloudControlInvalid):
		return managedPreparationRequestError()
	case errors.Is(err, ErrAgentCloudControlConflict), errors.Is(err, ErrAgentCloudControlRejected):
		return managedPreparationConflictError()
	case errors.Is(err, ErrAgentCloudControlInvalidResponse):
		return managedPreparationInvalidResponseError()
	case err != nil:
		return unavailableError()
	case !found:
		return actionbase.CodedError(http.StatusNotFound, cloudServiceManagedPreparationInvalidCode, "cloud managed preparation operation was not found")
	default:
		return unavailableError()
	}
}

func managedPreparationRequestError() *actionbase.Error {
	return actionbase.CodedError(http.StatusBadRequest, cloudServiceManagedPreparationInvalidCode, "cloud managed preparation request is invalid")
}

func managedPreparationConflictError() *actionbase.Error {
	return actionbase.CodedError(http.StatusConflict, cloudServiceManagedPreparationConflictCode, "cloud managed preparation conflicts with current state")
}

func managedPreparationInvalidResponseError() *actionbase.Error {
	return actionbase.CodedError(http.StatusBadGateway, cloudServiceManagedPreparationInvalidCode, "cloud Agent returned an invalid managed preparation response")
}
