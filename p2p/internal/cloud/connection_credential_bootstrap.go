package cloud

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

const (
	connectionCredentialBootstrapCreateSchema   = "dirextalk.aws-bootstrap-session-create/v1"
	connectionCredentialBootstrapResponseSchema = "dirextalk.aws-bootstrap-session/v1"
	connectionCredentialBootstrapReceiptSchema  = "dirextalk.aws-bootstrap-accepted/v1"
	connectionCredentialBootstrapHKDF           = "HKDF-SHA256 info=dirextalk.connection-bootstrap/x25519-aes256gcm/v1"
	connectionCredentialUploadEnvelopeSchema    = "dirextalk.aws-credential-upload/v1"
)

var (
	ErrConnectionCredentialBootstrapUpstreamConflict = errors.New("connection credential bootstrap upstream idempotency conflict")
	ErrConnectionCredentialBootstrapUpstreamRejected = errors.New("connection credential bootstrap upstream rejected the role plan")
	ErrConnectionCredentialBootstrapUpstreamInvalid  = errors.New("connection credential bootstrap upstream returned an invalid response")
)

func (m *Module) createConnectionCredentialBootstrap(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := only(params, "bootstrap_id", "expected_revision", "idempotency_key"); err != nil {
		return nil, err
	}
	if m == nil || m.store == nil || (m.cfg.CredentialBootstrapClient == nil && m.cfg.SecretBootstrapClient == nil) {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudConnectionCredentialBootstrapUnavailableCode, "cloud connection credential bootstrap is not configured")
	}
	store, ok := m.store.(ConnectionCredentialBootstrapStore)
	if !ok {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudConnectionCredentialBootstrapUnavailableCode, "cloud connection credential bootstrap is not configured")
	}
	values := actionbase.Params(params)
	bootstrapID := values.String("bootstrap_id")
	expectedRevision := values.Int64("expected_revision")
	idempotencyKey := values.String("idempotency_key")
	parsedID, idErr := uuid.Parse(idempotencyKey)
	if !cloudIdentifierPattern.MatchString(bootstrapID) || expectedRevision <= 0 || idErr != nil || parsedID.String() != idempotencyKey || ContainsSensitiveGoalMaterial(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionCredentialBootstrapInvalidCode, "cloud connection credential bootstrap request is invalid")
	}
	ownerMXID := m.ownerMXID()
	if ownerMXID == "" {
		return nil, actionbase.InternalError(context.Canceled)
	}
	now := m.now().UTC()
	load := LoadConnectionCredentialBootstrapRequest{OwnerMXID: ownerMXID, BootstrapID: bootstrapID, ExpectedRevision: expectedRevision, Now: now.UnixMilli()}
	rolePlan, err := store.LoadCloudConnectionCredentialBootstrap(ctx, load)
	if err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	if m.cfg.SecretBootstrapClient != nil {
		return m.createAgentConnectionCredentialBootstrap(ctx, store, load, rolePlan, idempotencyKey)
	}
	request, err := connectionCredentialBootstrapRequest(idempotencyKey, rolePlan)
	if err != nil {
		return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection role plan is incompatible with credential bootstrap")
	}
	session, err := m.cfg.CredentialBootstrapClient.CreateSession(ctx, request)
	if err != nil {
		switch {
		case errors.Is(err, ErrConnectionCredentialBootstrapUpstreamConflict):
			return nil, actionbase.CodedError(http.StatusConflict, cloudIdempotencyConflictCode, "idempotency_key was already used for a different credential bootstrap request")
		case errors.Is(err, ErrConnectionCredentialBootstrapUpstreamRejected):
			return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection role plan was rejected by credential bootstrap")
		default:
			return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudConnectionCredentialBootstrapUpstreamCode, "cloud connection credential bootstrap service is unavailable")
		}
	}
	responseNow := m.now().UTC()
	if err := validateConnectionCredentialBootstrapSession(session, request, responseNow); err != nil {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudConnectionCredentialBootstrapUpstreamCode, "cloud connection credential bootstrap service returned an invalid response")
	}
	// Do not expose a freshly issued bearer if the durable role plan changed
	// while the independent service was being called. The orphaned upstream
	// session expires without its bearer ever reaching the client.
	load.Now = responseNow.UnixMilli()
	if _, err := store.LoadCloudConnectionCredentialBootstrap(ctx, load); err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	result := map[string]any{
		"status":                   session.Status,
		"session_id":               session.SessionID,
		"server_x25519_public_key": session.ServerX25519PublicKey,
		"upload_url":               session.UploadURL,
		"upload_bearer":            session.UploadBearer,
		"expires_at":               session.ExpiresAt,
	}
	if session.Receipt != nil {
		result["receipt"] = session.Receipt
	}
	return map[string]any{"session": result}, nil
}

func connectionCredentialBootstrapStoreError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrConnectionBootstrapExpired):
		return actionbase.CodedError(http.StatusGone, cloudConnectionBootstrapExpiredCode, "cloud connection role plan has expired")
	case errors.Is(err, ErrConnectionBootstrapConflict):
		return actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection role plan revision conflicts with current state")
	case errors.Is(err, ErrConnectionBootstrapInvalid):
		return actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection role plan is not awaiting stack creation")
	default:
		return actionbase.InternalError(err)
	}
}

func connectionCredentialBootstrapRequest(requestID string, plan ConnectionRolePlan) (ConnectionCredentialBootstrapRequest, error) {
	if err := validateConnectionRolePlanTemplate(plan); err != nil {
		return ConnectionCredentialBootstrapRequest{}, err
	}
	fixed := make(map[string]string, 7)
	for _, key := range []string{"ConnectionId", "ConnectionGeneration", "NodeKeyId", "NodePublicKeySpkiBase64", "DeviceApprovalKeyId", "DeviceApprovalPublicKeySpkiBase64", "StageName"} {
		value := plan.CloudFormationParams[key]
		if value == "" {
			return ConnectionCredentialBootstrapRequest{}, errors.New("fixed CloudFormation role-plan parameter is missing")
		}
		fixed[key] = value
	}
	return ConnectionCredentialBootstrapRequest{
		Schema:    connectionCredentialBootstrapCreateSchema,
		RequestID: requestID,
		RolePlan: ConnectionCredentialBootstrapRolePlanWire{
			BootstrapID: plan.BootstrapID, ConnectionID: plan.CloudConnectionID, Region: plan.Region, StackName: plan.StackName,
			ConnectionTemplate: plan.ConnectionTemplate.Clone(), SourceTreeDigest: plan.SourceTreeDigest,
			FixedParameters: fixed, NodeKeyID: plan.CloudFormationParams["NodeKeyId"], NodeEd25519PublicKey: plan.CloudFormationParams["NodePublicKeySpkiBase64"],
			DeviceKeyID: plan.CloudFormationParams["DeviceApprovalKeyId"], DeviceEd25519PublicKey: plan.CloudFormationParams["DeviceApprovalPublicKeySpkiBase64"],
			AllowRootCredentialBootstrap: plan.AllowRootCredentialBootstrap,
			ExpiresAt:                    time.UnixMilli(plan.ExpiresAt).UTC().Format(time.RFC3339Nano),
		},
	}, nil
}

func validateConnectionRolePlanTemplate(plan ConnectionRolePlan) error {
	if plan.ConnectionTemplate.ValidateForRootCredentialBootstrap(plan.AllowRootCredentialBootstrap) != nil ||
		plan.TemplateDigest != plan.ConnectionTemplate.ContentDigest() {
		return errors.New("cloud connection role plan template reference is invalid")
	}
	switch plan.ConnectionTemplate.Mode {
	case connectionTemplateModeS3Binding:
		if plan.ConnectionTemplate.Binding == nil || validateTemplateURLForBinding(plan.TemplateURL, plan.Region, *plan.ConnectionTemplate.Binding) != nil {
			return errors.New("cloud connection role plan template URL is invalid")
		}
	case connectionTemplateModePublishIntent:
		if plan.TemplateURL != "" {
			return errors.New("cloud connection role plan template URL is invalid")
		}
	default:
		return errors.New("cloud connection role plan template reference is invalid")
	}
	return nil
}

func validateConnectionCredentialBootstrapSession(session ConnectionCredentialBootstrapSession, request ConnectionCredentialBootstrapRequest, now time.Time) error {
	if session.Schema != connectionCredentialBootstrapResponseSchema || session.RequestID != request.RequestID || session.ConnectionID != request.RolePlan.ConnectionID ||
		!cloudIdentifierPattern.MatchString(session.SessionID) || session.HKDF != connectionCredentialBootstrapHKDF {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	publicKey, err := base64.StdEncoding.DecodeString(session.ServerX25519PublicKey)
	nonzero := byte(0)
	for _, value := range publicKey {
		nonzero |= value
	}
	if err != nil || len(publicKey) != 32 || nonzero == 0 {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, session.ExpiresAt)
	roleExpiresAt, roleExpiresErr := time.Parse(time.RFC3339Nano, request.RolePlan.ExpiresAt)
	if err != nil || roleExpiresErr != nil || expiresAt.UTC().Format(time.RFC3339Nano) != session.ExpiresAt || !expiresAt.After(now) || expiresAt.After(roleExpiresAt) {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	uploadURL, err := url.Parse(session.UploadURL)
	if err != nil || uploadURL.Scheme != "https" || uploadURL.Host == "" || uploadURL.User != nil || uploadURL.RawQuery != "" || uploadURL.Fragment != "" ||
		uploadURL.Path != "/v1/aws-bootstrap/sessions/"+session.SessionID {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	aad, _ := json.Marshal(struct {
		Schema       string `json:"schema"`
		SessionID    string `json:"session_id"`
		ConnectionID string `json:"connection_id"`
		ExpiresAt    string `json:"expires_at"`
	}{connectionCredentialUploadEnvelopeSchema, session.SessionID, session.ConnectionID, session.ExpiresAt})
	if session.AAD != string(aad) {
		return ErrConnectionCredentialBootstrapUpstreamInvalid
	}
	switch session.Status {
	case "awaiting_upload":
		bearer, bearerErr := base64.RawURLEncoding.DecodeString(session.UploadBearer)
		if bearerErr != nil || len(bearer) != 32 || session.Receipt != nil {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
	case "processing", "failed":
		if session.UploadBearer != "" || session.Receipt != nil {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
	case "accepted":
		if session.UploadBearer != "" || session.Receipt == nil || session.Receipt.Schema != connectionCredentialBootstrapReceiptSchema ||
			session.Receipt.Status != "accepted" || session.Receipt.ConnectionID != session.ConnectionID ||
			ValidateConnectionRegistrationStackARN(session.Receipt.StackID, request.RolePlan.Region) != nil {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
		acceptedAt, err := time.Parse(time.RFC3339Nano, session.Receipt.AcceptedAt)
		if err != nil || acceptedAt.UTC().Format(time.RFC3339Nano) != session.Receipt.AcceptedAt {
			return ErrConnectionCredentialBootstrapUpstreamInvalid
		}
	default:
		return fmt.Errorf("%w: status", ErrConnectionCredentialBootstrapUpstreamInvalid)
	}
	return nil
}
