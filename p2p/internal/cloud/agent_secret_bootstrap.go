package cloud

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"regexp"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	"github.com/google/uuid"
)

const (
	AgentSecretBootstrapPurposeAWSConnection                 = "aws_connection"
	AgentSecretBootstrapPurposeAWSFoundationEstablish        = "aws_foundation_establish"
	AgentSecretBootstrapPurposeAWSFoundationUpgrade          = "aws_foundation_upgrade"
	AgentSecretBootstrapPurposeAWSFoundationTeardown         = "aws_foundation_teardown"
	AgentSecretBootstrapPurposeAWSFoundationRemediateBlocked = "aws_foundation_remediate_destroy_blocked"
	AgentSecretBootstrapSessionSchemaV1                      = "dirextalk.agent.secret-bootstrap.session/v1"
	AgentSecretBootstrapEnvelopeSchemaV1                     = "dirextalk.agent.secret-bootstrap.envelope/v1"
	AgentSecretBootstrapUploadPath                           = "/_p2p/cloud/secret-bootstrap/upload"
	AgentSecretBootstrapMaxCiphertextBytes                   = 1024*1024 + 16
)

var (
	ErrAgentSecretBootstrapInvalid         = errors.New("agent secret bootstrap request is invalid")
	ErrAgentSecretBootstrapConflict        = errors.New("agent secret bootstrap request conflicts with current state")
	ErrAgentSecretBootstrapRejected        = errors.New("agent secret bootstrap upload was rejected")
	ErrAgentSecretBootstrapUnavailable     = errors.New("agent secret bootstrap service is unavailable")
	ErrAgentSecretBootstrapInvalidResponse = errors.New("agent secret bootstrap returned an invalid response")
	agentSecretIdentifierPattern           = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
)

// AgentSecretBootstrapSession contains only public session binding plus the
// one-time upload token returned by Create. UploadToken must be empty on every
// non-Create response and is never persisted by Message Server.
type AgentSecretBootstrapSession struct {
	SessionSchemaVersion  string
	EnvelopeSchemaVersion string
	SessionID             string
	AgentInstanceID       string
	OwnerID               string
	Purpose               string
	TargetID              string
	ServerPublicKey       []byte
	UploadToken           []byte
	CreatedAt             string
	ExpiresAt             string
	Status                string
	Revision              int64
}

type CreateAgentSecretBootstrapRequest struct {
	IdempotencyKey string
	Purpose        string
	TargetID       string
}

type UploadAgentEncryptedSecretRequest struct {
	SessionID        string
	UploadToken      []byte
	ClientPublicKey  []byte
	Nonce            []byte
	Ciphertext       []byte
	IdempotencyKey   string
	ExpectedRevision int64
}

// SecretBootstrapClient is deliberately narrower than the Agent gRPC API. It
// has no Complete operation and therefore cannot deliver or consume a secret.
type SecretBootstrapClient interface {
	CreateAgentSecretBootstrap(context.Context, CreateAgentSecretBootstrapRequest) (AgentSecretBootstrapSession, error)
	UploadAgentEncryptedSecret(context.Context, UploadAgentEncryptedSecretRequest) (AgentSecretBootstrapSession, error)
}

func ValidAgentSecretBootstrapPurpose(value string) bool {
	switch value {
	case AgentSecretBootstrapPurposeAWSConnection,
		AgentSecretBootstrapPurposeAWSFoundationEstablish,
		AgentSecretBootstrapPurposeAWSFoundationUpgrade,
		AgentSecretBootstrapPurposeAWSFoundationTeardown,
		AgentSecretBootstrapPurposeAWSFoundationRemediateBlocked:
		return true
	default:
		return false
	}
}

func agentFoundationBootstrapPurpose(action string) (string, bool) {
	switch action {
	case "upgrade":
		return AgentSecretBootstrapPurposeAWSFoundationUpgrade, true
	case "teardown":
		return AgentSecretBootstrapPurposeAWSFoundationTeardown, true
	case "remediate_destroy_blocked":
		return AgentSecretBootstrapPurposeAWSFoundationRemediateBlocked, true
	default:
		return "", false
	}
}

func (m *Module) createAgentConnectionCredentialBootstrap(
	ctx context.Context,
	store ConnectionCredentialBootstrapStore,
	load LoadConnectionCredentialBootstrapRequest,
	rolePlan ConnectionRolePlan,
	idempotencyKey string,
) (any, *actionbase.Error) {
	return m.createAgentRolePlanCredentialBootstrap(ctx, store, load, rolePlan, idempotencyKey, AgentSecretBootstrapPurposeAWSConnection)
}

func (m *Module) createAgentFoundationEstablishCredentialBootstrap(
	ctx context.Context,
	store ConnectionCredentialBootstrapStore,
	load LoadConnectionCredentialBootstrapRequest,
	rolePlan ConnectionRolePlan,
	idempotencyKey string,
) (any, *actionbase.Error) {
	return m.createAgentRolePlanCredentialBootstrap(ctx, store, load, rolePlan, idempotencyKey, AgentSecretBootstrapPurposeAWSFoundationEstablish)
}

func (m *Module) createAgentRolePlanCredentialBootstrap(
	ctx context.Context,
	store ConnectionCredentialBootstrapStore,
	load LoadConnectionCredentialBootstrapRequest,
	rolePlan ConnectionRolePlan,
	idempotencyKey, purpose string,
) (any, *actionbase.Error) {
	if rolePlan.Provider != "aws" || !rolePlan.AllowRootCredentialBootstrap ||
		!cloudIdentifierPattern.MatchString(rolePlan.CloudConnectionID) || ContainsSensitiveGoalMaterial(rolePlan.CloudConnectionID) {
		return nil, actionbase.CodedError(http.StatusConflict, cloudConnectionBootstrapConflictCode, "cloud connection role plan is incompatible with credential bootstrap")
	}
	session, err := m.cfg.SecretBootstrapClient.CreateAgentSecretBootstrap(ctx, CreateAgentSecretBootstrapRequest{
		IdempotencyKey: idempotencyKey,
		Purpose:        purpose,
		TargetID:       rolePlan.CloudConnectionID,
	})
	if err != nil {
		return nil, agentSecretBootstrapError(err)
	}
	defer wipeBytes(session.UploadToken)
	if validateAgentSecretBootstrapSession(session) != nil || session.Purpose != purpose || session.TargetID != rolePlan.CloudConnectionID {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	}
	includeUploadCapability := false
	switch session.Status {
	case "awaiting_upload":
		includeUploadCapability = len(session.UploadToken) == 32
		if !includeUploadCapability {
			return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
		}
	case "uploaded":
		if len(session.UploadToken) != 0 {
			return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
		}
	default:
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	}
	// Re-read the role plan after the remote call so a concurrent plan revision
	// never exposes a newly issued upload token to an obsolete client request.
	load.Now = m.now().UTC().UnixMilli()
	if _, err = store.LoadCloudConnectionCredentialBootstrap(ctx, load); err != nil {
		return nil, connectionCredentialBootstrapStoreError(err)
	}
	return map[string]any{"session": agentSecretBootstrapSessionView(session, includeUploadCapability)}, nil
}

func (m *Module) createAgentFoundationCredentialBootstrap(
	ctx context.Context,
	action, connectionID string,
	expectedConnectionRevision int64,
	idempotencyKey string,
) (any, *actionbase.Error) {
	if m == nil || m.cfg.SecretBootstrapClient == nil || m.cfg.AgentCloudControlClient == nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudConnectionCredentialBootstrapUnavailableCode, "cloud connection credential bootstrap is not configured")
	}
	purpose, ok := agentFoundationBootstrapPurpose(action)
	if !ok || !canonicalUUID(connectionID) || expectedConnectionRevision <= 0 || !canonicalUUID(idempotencyKey) {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudConnectionCredentialBootstrapInvalidCode, "cloud connection credential bootstrap request is invalid")
	}
	connection, apiErr := m.loadAgentFoundationConnection(ctx, action, connectionID, expectedConnectionRevision)
	if apiErr != nil {
		return nil, apiErr
	}
	session, err := m.cfg.SecretBootstrapClient.CreateAgentSecretBootstrap(ctx, CreateAgentSecretBootstrapRequest{
		IdempotencyKey: idempotencyKey,
		Purpose:        purpose,
		TargetID:       connection.ConnectionID,
	})
	if err != nil {
		return nil, agentSecretBootstrapError(err)
	}
	defer wipeBytes(session.UploadToken)
	if validateAgentSecretBootstrapSession(session) != nil || session.Purpose != purpose || session.TargetID != connection.ConnectionID {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	}
	includeUploadCapability := false
	switch session.Status {
	case "awaiting_upload":
		includeUploadCapability = len(session.UploadToken) == 32
		if !includeUploadCapability {
			return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
		}
	case "uploaded":
		if len(session.UploadToken) != 0 {
			return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
		}
	default:
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	}
	// Fence the one-time upload capability against a concurrent Connection
	// transition after the Agent call.
	if _, apiErr = m.loadAgentFoundationConnection(ctx, action, connectionID, expectedConnectionRevision); apiErr != nil {
		return nil, apiErr
	}
	return map[string]any{"session": agentSecretBootstrapSessionView(session, includeUploadCapability)}, nil
}

// UploadAgentEncryptedSecret forwards one already-encrypted envelope. The
// caller retains ownership of all byte slices and must wipe them after return.
func (m *Module) UploadAgentEncryptedSecret(ctx context.Context, request UploadAgentEncryptedSecretRequest) (any, *actionbase.Error) {
	if m == nil || m.cfg.SecretBootstrapClient == nil {
		return nil, actionbase.CodedError(http.StatusServiceUnavailable, cloudSecretBootstrapUnavailableCode, "cloud secret bootstrap is not configured")
	}
	if !canonicalUUID(request.SessionID) || !canonicalUUID(request.IdempotencyKey) || request.ExpectedRevision <= 0 ||
		len(request.UploadToken) != 32 || len(request.ClientPublicKey) != 32 || len(request.Nonce) != 12 ||
		len(request.Ciphertext) < 17 || len(request.Ciphertext) > AgentSecretBootstrapMaxCiphertextBytes {
		return nil, actionbase.CodedError(http.StatusBadRequest, cloudSecretBootstrapInvalidCode, "cloud secret bootstrap upload is invalid")
	}
	session, err := m.cfg.SecretBootstrapClient.UploadAgentEncryptedSecret(ctx, request)
	if err != nil {
		return nil, agentSecretBootstrapError(err)
	}
	if validateAgentSecretBootstrapSession(session) != nil || session.SessionID != request.SessionID || session.Status != "uploaded" || session.Revision <= request.ExpectedRevision || len(session.UploadToken) != 0 {
		return nil, actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	}
	return map[string]any{"session": agentSecretBootstrapSessionView(session, false)}, nil
}

func agentSecretBootstrapSessionView(session AgentSecretBootstrapSession, includeUploadToken bool) map[string]any {
	result := map[string]any{
		"session_schema_version":  session.SessionSchemaVersion,
		"envelope_schema_version": session.EnvelopeSchemaVersion,
		"session_id":              session.SessionID,
		"agent_instance_id":       session.AgentInstanceID,
		"owner_id":                session.OwnerID,
		"purpose":                 session.Purpose,
		"target_id":               session.TargetID,
		"created_at":              session.CreatedAt,
		"expires_at":              session.ExpiresAt,
		"status":                  session.Status,
		"revision":                session.Revision,
	}
	if includeUploadToken {
		result["server_x25519_public_key"] = base64.RawURLEncoding.EncodeToString(session.ServerPublicKey)
		result["upload_url"] = AgentSecretBootstrapUploadPath
		result["upload_token"] = base64.RawURLEncoding.EncodeToString(session.UploadToken)
	}
	return result
}

func agentSecretBootstrapError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrAgentSecretBootstrapInvalid):
		return actionbase.CodedError(http.StatusBadRequest, cloudSecretBootstrapInvalidCode, "cloud secret bootstrap request is invalid")
	case errors.Is(err, ErrAgentSecretBootstrapConflict):
		return actionbase.CodedError(http.StatusConflict, cloudSecretBootstrapConflictCode, "cloud secret bootstrap request conflicts with current state")
	case errors.Is(err, ErrAgentSecretBootstrapRejected):
		return actionbase.CodedError(http.StatusForbidden, cloudSecretBootstrapRejectedCode, "cloud secret bootstrap upload was rejected")
	case errors.Is(err, ErrAgentSecretBootstrapInvalidResponse):
		return actionbase.CodedError(http.StatusBadGateway, cloudSecretBootstrapUpstreamCode, "cloud secret bootstrap service returned an invalid response")
	default:
		return actionbase.CodedError(http.StatusServiceUnavailable, cloudSecretBootstrapUnavailableCode, "cloud secret bootstrap service is unavailable")
	}
}

func canonicalUUID(value string) bool {
	parsed, err := uuid.Parse(strings.TrimSpace(value))
	return err == nil && parsed.String() == value
}

func validateAgentSecretBootstrapSession(session AgentSecretBootstrapSession) error {
	if session.SessionSchemaVersion != AgentSecretBootstrapSessionSchemaV1 || session.EnvelopeSchemaVersion != AgentSecretBootstrapEnvelopeSchemaV1 ||
		!canonicalUUID(session.SessionID) || !agentSecretIdentifierPattern.MatchString(session.AgentInstanceID) ||
		!agentSecretIdentifierPattern.MatchString(session.OwnerID) || !agentSecretIdentifierPattern.MatchString(session.Purpose) ||
		!agentSecretIdentifierPattern.MatchString(session.TargetID) || len(session.ServerPublicKey) != 32 || session.Revision <= 0 {
		return ErrAgentSecretBootstrapInvalidResponse
	}
	for _, value := range []string{session.AgentInstanceID, session.OwnerID, session.Purpose, session.TargetID} {
		if ContainsSensitiveGoalMaterial(value) {
			return ErrAgentSecretBootstrapInvalidResponse
		}
	}
	var nonzero byte
	for _, value := range session.ServerPublicKey {
		nonzero |= value
	}
	if nonzero == 0 {
		return ErrAgentSecretBootstrapInvalidResponse
	}
	createdAt, createdErr := time.Parse(time.RFC3339Nano, session.CreatedAt)
	expiresAt, expiresErr := time.Parse(time.RFC3339Nano, session.ExpiresAt)
	if createdErr != nil || expiresErr != nil || createdAt.UTC().Format(time.RFC3339Nano) != session.CreatedAt ||
		expiresAt.UTC().Format(time.RFC3339Nano) != session.ExpiresAt || expiresAt.Sub(createdAt) != 10*time.Minute {
		return ErrAgentSecretBootstrapInvalidResponse
	}
	switch session.Status {
	case "awaiting_upload", "uploaded", "consumed", "expired":
		return nil
	default:
		return ErrAgentSecretBootstrapInvalidResponse
	}
}

func wipeBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
