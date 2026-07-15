package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func (m *Module) prepareServiceManagementAcceptance(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceManagementAcceptanceStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	eventID := m.newID("event")
	prepared, err := store.PrepareCloudServiceManagementAcceptance(ctx, PrepareServiceManagementAcceptanceRequest{
		OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision,
		IdempotencyHash: digest(idempotencyKey), RequestDigest: digestFields(serviceID, fmt.Sprint(expectedRevision)),
		AcceptanceID: m.newID("service_management_acceptance"), ApprovalID: m.newID("service_management_approval"), ChallengeID: m.newID("service_management_challenge"), ServiceEventID: eventID,
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(5 * time.Minute).UnixMilli(),
	})
	if err != nil {
		return nil, serviceManagementAcceptanceError(err)
	}
	if prepared.ServiceChanged {
		m.publish(ctx, "cloud.service.changed", eventID, servicePayload(prepared.Confirmation.Service))
	}
	return map[string]any{"confirmation": prepared.Confirmation}, nil
}

func (m *Module) approveServiceManagementAcceptance(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "service_id", "expected_revision", "approval", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil {
		return nil, unavailableError()
	}
	store, ok := m.store.(ServiceManagementAcceptanceStore)
	if !ok {
		return nil, unavailableError()
	}
	values := actionbase.Params(params)
	serviceID, idempotencyKey := values.String("service_id"), values.String("idempotency_key")
	expectedRevision := values.Int64("expected_revision")
	if !cloudIdentifierPattern.MatchString(serviceID) || expectedRevision <= 0 || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	if _, err := uuid.Parse(idempotencyKey); err != nil {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudIdempotencyInvalidCode, "idempotency_key must be a UUID")
	}
	approval, err := decodeServiceManagementAcceptanceApproval(params["approval"])
	if err != nil || approval.Signature == "" {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	eventID, now := m.newID("event"), m.now().UnixMilli()
	approved, err := store.ApproveCloudServiceManagementAcceptance(ctx, ApproveServiceManagementAcceptanceRequest{OwnerMXID: ownerMXID, ServiceID: serviceID, ExpectedRevision: expectedRevision, IdempotencyHash: digest(idempotencyKey), Approval: approval, ServiceEventID: eventID, CreatedAt: now})
	if err != nil {
		return nil, serviceManagementAcceptanceError(err)
	}
	if approved.Created {
		m.publish(ctx, "cloud.service.changed", eventID, servicePayload(approved.Service))
	}
	return map[string]any{"service": approved.Service, "recipe": approved.Recipe, "acceptance": approved.Acceptance}, nil
}

func decodeServiceManagementAcceptanceApproval(value any) (cloudcontracts.ServiceManagementAcceptanceApprovalV1, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, err
	}
	var approval cloudcontracts.ServiceManagementAcceptanceApprovalV1
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&approval); err != nil {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, err
	}
	var trailing any
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return cloudcontracts.ServiceManagementAcceptanceApprovalV1{}, errors.New("service management acceptance contains trailing JSON")
	}
	return approval, approval.Validate()
}

func serviceManagementAcceptanceError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrServiceManagementAcceptanceConflict):
		return actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceConflictCode, "cloud service management acceptance conflicts with current state")
	case errors.Is(err, ErrServiceManagementAcceptanceExpired):
		return actionbase.CodedError(http.StatusConflict, cloudServiceManagementAcceptanceExpiredCode, "cloud service management acceptance has expired")
	case errors.Is(err, ErrServiceManagementAcceptanceSignature):
		return actionbase.CodedError(http.StatusForbidden, cloudServiceManagementAcceptanceSignatureCode, "cloud service management acceptance signature is invalid")
	case errors.Is(err, ErrServiceManagementAcceptanceInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudServiceManagementAcceptanceInvalidCode, "cloud service management acceptance is invalid")
	default:
		return actionbase.InternalError(err)
	}
}
