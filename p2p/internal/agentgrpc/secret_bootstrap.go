package agentgrpc

import (
	"context"
	"regexp"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const agentSecretBootstrapSessionTTL = 10 * time.Minute

var agentSecretIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// CreateAgentSecretBootstrap creates only an encrypted-upload session. The
// stable owner is injected from Runner configuration and can never be supplied
// by ProductCore parameters.
func (runner *Runner) CreateAgentSecretBootstrap(ctx context.Context, request cloudmodule.CreateAgentSecretBootstrapRequest) (cloudmodule.AgentSecretBootstrapSession, error) {
	if runner == nil || runner.secrets == nil {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapUnavailable
	}
	if !validUUID(request.IdempotencyKey) || !cloudmodule.ValidAgentSecretBootstrapPurpose(request.Purpose) ||
		!validAgentSecretIdentifier(request.TargetID) {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.secrets.CreateSession(callContext, &agentv1.CreateSessionRequest{
		IdempotencyKey: request.IdempotencyKey,
		OwnerId:        runner.ownerID,
		Purpose:        request.Purpose,
		TargetId:       request.TargetID,
	})
	if err != nil {
		return cloudmodule.AgentSecretBootstrapSession{}, mapSecretBootstrapRPCError(callContext, err, false)
	}
	if response == nil || response.GetSession() == nil || response.GetSessionId() != response.GetSession().GetSessionId() {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	defer wipeAgentSecretBytes(response.UploadToken)
	session, err := runner.mapAgentSecretBootstrapSession(response.GetSession(), request.Purpose, request.TargetID)
	if err != nil || !sameTimestamp(response.GetExpiresAt(), response.GetSession().GetExpiresAt()) {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	switch session.Status {
	case "awaiting_upload":
		if session.Revision != 1 || len(response.GetUploadToken()) != 32 ||
			!equalBytes(response.GetServerPublicKey(), response.GetSession().GetServerPublicKey()) {
			return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
		}
		session.UploadToken = append([]byte(nil), response.GetUploadToken()...)
	case "uploaded":
		if session.Revision <= 1 || len(response.GetUploadToken()) != 0 {
			return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
		}
	default:
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	return session, nil
}

// UploadAgentEncryptedSecret forwards ciphertext exactly once through the
// Agent mutation API. It never retries and never accepts plaintext.
func (runner *Runner) UploadAgentEncryptedSecret(ctx context.Context, request cloudmodule.UploadAgentEncryptedSecretRequest) (cloudmodule.AgentSecretBootstrapSession, error) {
	if runner == nil || runner.secrets == nil {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapUnavailable
	}
	if !validUUID(request.SessionID) || !validUUID(request.IdempotencyKey) || request.ExpectedRevision <= 0 ||
		len(request.UploadToken) != 32 || len(request.ClientPublicKey) != 32 || len(request.Nonce) != 12 ||
		len(request.Ciphertext) < 17 || len(request.Ciphertext) > cloudmodule.AgentSecretBootstrapMaxCiphertextBytes {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalid
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	response, err := runner.secrets.UploadEncrypted(callContext, &agentv1.UploadEncryptedRequest{
		SessionId: request.SessionID, UploadToken: request.UploadToken,
		ClientPublicKey: request.ClientPublicKey, Nonce: request.Nonce, Ciphertext: request.Ciphertext,
		IdempotencyKey: request.IdempotencyKey, ExpectedRevision: request.ExpectedRevision,
	})
	if err != nil {
		return cloudmodule.AgentSecretBootstrapSession{}, mapSecretBootstrapRPCError(callContext, err, true)
	}
	if response == nil || response.GetSession() == nil || response.GetRevision() != response.GetSession().GetRevision() ||
		response.GetSession().GetSessionId() != request.SessionID {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	session, err := runner.mapAgentSecretBootstrapSession(response.GetSession(), "", "")
	if err != nil || session.Status != "uploaded" || session.Revision <= request.ExpectedRevision {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	return session, nil
}

func (runner *Runner) mapAgentSecretBootstrapSession(remote *agentv1.SecretBootstrapSession, expectedPurpose, expectedTarget string) (cloudmodule.AgentSecretBootstrapSession, error) {
	if remote == nil || !validUUID(remote.GetSessionId()) || remote.GetOwnerId() != runner.ownerID ||
		!cloudmodule.ValidAgentSecretBootstrapPurpose(remote.GetPurpose()) || (expectedPurpose != "" && remote.GetPurpose() != expectedPurpose) || !validAgentSecretIdentifier(remote.GetAgentInstanceId()) ||
		!validAgentSecretIdentifier(remote.GetTargetId()) || (expectedTarget != "" && remote.GetTargetId() != expectedTarget) ||
		remote.GetSessionSchemaVersion() != cloudmodule.AgentSecretBootstrapSessionSchemaV1 ||
		remote.GetEnvelopeSchemaVersion() != cloudmodule.AgentSecretBootstrapEnvelopeSchemaV1 ||
		len(remote.GetServerPublicKey()) != 32 || allZero(remote.GetServerPublicKey()) || remote.GetRevision() <= 0 {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	createdAt, err := exactBootstrapTimestamp(remote.GetCreatedAt())
	if err != nil {
		return cloudmodule.AgentSecretBootstrapSession{}, err
	}
	expiresAt, err := exactBootstrapTimestamp(remote.GetExpiresAt())
	if err != nil || !expiresAt.After(createdAt) || expiresAt.Sub(createdAt) != agentSecretBootstrapSessionTTL {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	statusValue, ok := agentSecretBootstrapStatus(remote.GetStatus())
	if !ok {
		return cloudmodule.AgentSecretBootstrapSession{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	return cloudmodule.AgentSecretBootstrapSession{
		SessionSchemaVersion: remote.GetSessionSchemaVersion(), EnvelopeSchemaVersion: remote.GetEnvelopeSchemaVersion(),
		SessionID: remote.GetSessionId(), AgentInstanceID: remote.GetAgentInstanceId(), OwnerID: remote.GetOwnerId(),
		Purpose: remote.GetPurpose(), TargetID: remote.GetTargetId(), ServerPublicKey: append([]byte(nil), remote.GetServerPublicKey()...),
		CreatedAt: createdAt.Format(time.RFC3339Nano), ExpiresAt: expiresAt.Format(time.RFC3339Nano), Status: statusValue, Revision: remote.GetRevision(),
	}, nil
}

func exactBootstrapTimestamp(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	result := value.AsTime().UTC()
	if result.Unix() <= 0 {
		return time.Time{}, cloudmodule.ErrAgentSecretBootstrapInvalidResponse
	}
	return result, nil
}

func sameTimestamp(left, right *timestamppb.Timestamp) bool {
	if left == nil || right == nil || left.CheckValid() != nil || right.CheckValid() != nil {
		return false
	}
	return left.Seconds == right.Seconds && left.Nanos == right.Nanos
}

func agentSecretBootstrapStatus(value agentv1.SecretBootstrapSessionStatus) (string, bool) {
	switch value {
	case agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_AWAITING_UPLOAD:
		return "awaiting_upload", true
	case agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_UPLOADED:
		return "uploaded", true
	case agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_CONSUMED:
		return "consumed", true
	case agentv1.SecretBootstrapSessionStatus_SECRET_BOOTSTRAP_SESSION_STATUS_EXPIRED:
		return "expired", true
	default:
		return "", false
	}
}

func mapSecretBootstrapRPCError(ctx context.Context, err error, upload bool) error {
	if ctx.Err() != nil {
		return cloudmodule.ErrAgentSecretBootstrapUnavailable
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return cloudmodule.ErrAgentSecretBootstrapInvalid
	case codes.AlreadyExists, codes.Aborted, codes.FailedPrecondition:
		return cloudmodule.ErrAgentSecretBootstrapConflict
	case codes.PermissionDenied, codes.NotFound:
		if upload {
			return cloudmodule.ErrAgentSecretBootstrapRejected
		}
		return cloudmodule.ErrAgentSecretBootstrapUnavailable
	case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable, codes.Unauthenticated, codes.Internal:
		return cloudmodule.ErrAgentSecretBootstrapUnavailable
	default:
		return cloudmodule.ErrAgentSecretBootstrapUnavailable
	}
}

func validAgentSecretIdentifier(value string) bool {
	return agentSecretIdentifierPattern.MatchString(value)
}

func allZero(value []byte) bool {
	var aggregate byte
	for _, item := range value {
		aggregate |= item
	}
	return aggregate == 0
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func wipeAgentSecretBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
