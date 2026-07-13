package legacygateway

import (
	"crypto/sha256"
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

const (
	idempotencyDigestDomain = "dirextalk.agent-gateway-idempotency.v1"
	requestDigestDomain     = "dirextalk.agent-gateway-run-request.v1"
	sourceDigestDomain      = "dirextalk.legacy-matrix-invocation-source.v1"
)

// LP returns U64BE(byte_length(value)) || value.
func LP(value []byte) []byte {
	encoded := make([]byte, 8+len(value))
	binary.BigEndian.PutUint64(encoded[:8], uint64(len(value)))
	copy(encoded[8:], value)
	return encoded
}

// Commit implements the frozen agent-gateway LP/COMMIT transcript.
func Commit(domain string, parts ...[]byte) [32]byte {
	hasher := sha256.New()
	_, _ = hasher.Write(LP([]byte(domain)))
	for _, part := range parts {
		_, _ = hasher.Write(LP(part))
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest
}

func IdempotencyDigest(tenantID, matrixRoomID, idempotencyKey string) ([32]byte, error) {
	tenant, err := parseUUIDv7(tenantID, "tenant_id")
	if err != nil {
		return [32]byte{}, err
	}
	if err := validateMatrixRoomID(matrixRoomID); err != nil {
		return [32]byte{}, err
	}
	if len(idempotencyKey) == 0 || len(idempotencyKey) > MaxIdempotencyKeyBytes {
		return [32]byte{}, newContractError(ContractInvalidLength, "idempotency_key")
	}
	return Commit(
		idempotencyDigestDomain,
		tenant[:],
		[]byte(matrixRoomID),
		[]byte(idempotencyKey),
	), nil
}

func SourceDigest(
	tenantID, matrixRoomID, matrixInvokeEventID, conversationID string,
	invocation Invocation,
) ([32]byte, error) {
	tenant, err := parseUUIDv7(tenantID, "tenant_id")
	if err != nil {
		return [32]byte{}, err
	}
	if err := validateMatrixRoomID(matrixRoomID); err != nil {
		return [32]byte{}, err
	}
	if err := validateMatrixEventID(matrixInvokeEventID, "matrix_invoke_event_id"); err != nil {
		return [32]byte{}, err
	}
	if err := validateMatrixEventID(invocation.MatrixInputEventID, "input_event_id"); err != nil {
		return [32]byte{}, err
	}
	requestID, err := parseUUIDv7(invocation.RequestID, "request_id")
	if err != nil {
		return [32]byte{}, err
	}
	installationID, err := parseUUIDv7(invocation.InstallationID, "installation_id")
	if err != nil {
		return [32]byte{}, err
	}
	conversation, err := parseUUIDv7(conversationID, "conversation_id")
	if err != nil {
		return [32]byte{}, err
	}
	preferredMarker, preferred, err := optionalUUIDPart(invocation.PreferredConnectorID)
	if err != nil {
		return [32]byte{}, err
	}
	capabilities, err := normalizedCapabilities(invocation.RequiredCapabilities)
	if err != nil {
		return [32]byte{}, err
	}
	dispatch, err := dispatchNumber(invocation.DispatchMode)
	if err != nil {
		return [32]byte{}, err
	}
	if invocation.GrantVersion == 0 || invocation.GrantVersion > MaxJSONSafeUint {
		return [32]byte{}, newContractError(ContractInvalidValue, "grant_version")
	}
	parts := [][]byte{
		tenant[:],
		[]byte(matrixRoomID),
		[]byte(matrixInvokeEventID),
		[]byte(invocation.MatrixInputEventID),
		conversation[:],
		requestID[:],
		invocation.IdempotencyDigest[:],
		installationID[:],
		preferredMarker,
	}
	if preferred != nil {
		parts = append(parts, preferred)
	}
	parts = append(parts, u64Part(uint64(len(capabilities))))
	for _, capability := range capabilities {
		parts = append(parts, []byte(capability))
	}
	parts = append(parts, u64Part(dispatch), u64Part(invocation.GrantVersion))
	return Commit(sourceDigestDomain, parts...), nil
}

func RequestDigest(tenantID string, request CreateRunRequest) ([32]byte, error) {
	tenant, err := parseUUIDv7(tenantID, "tenant_id")
	if err != nil {
		return [32]byte{}, err
	}
	requestID, err := parseUUIDv7(request.RequestID, "request_id")
	if err != nil {
		return [32]byte{}, err
	}
	installationID, err := parseUUIDv7(request.InstallationID, "installation_id")
	if err != nil {
		return [32]byte{}, err
	}
	conversationID, err := parseUUIDv7(request.ConversationID, "conversation_id")
	if err != nil {
		return [32]byte{}, err
	}
	requestEventID, err := parseUUIDv7(request.RequestEventID, "request_event_id")
	if err != nil {
		return [32]byte{}, err
	}
	preferredMarker, preferred, err := optionalUUIDPart(request.PreferredConnectorID)
	if err != nil {
		return [32]byte{}, err
	}
	capabilities, err := normalizedCapabilities(request.RequiredCapabilities)
	if err != nil {
		return [32]byte{}, err
	}
	dispatch, err := dispatchNumber(request.DispatchMode)
	if err != nil {
		return [32]byte{}, err
	}
	if request.GrantVersion == 0 || request.GrantVersion > MaxJSONSafeUint {
		return [32]byte{}, newContractError(ContractInvalidValue, "grant_version")
	}
	parts := [][]byte{
		tenant[:],
		requestID[:],
		request.IdempotencyDigest[:],
		installationID[:],
		conversationID[:],
		requestEventID[:],
		preferredMarker,
	}
	if preferred != nil {
		parts = append(parts, preferred)
	}
	parts = append(parts, u64Part(uint64(len(capabilities))))
	for _, capability := range capabilities {
		parts = append(parts, []byte(capability))
	}
	parts = append(parts, u64Part(dispatch), u64Part(request.GrantVersion))
	return Commit(requestDigestDomain, parts...), nil
}

func BuildCandidate(
	tenantID, matrixRoomID, matrixInvokeEventID, conversationID, requestEventID string,
	invocation Invocation,
	createdAt time.Time,
) (InvocationCandidate, error) {
	if createdAt.IsZero() {
		return InvocationCandidate{}, newContractError(ContractInvalidValue, "created_at")
	}
	capabilities, err := normalizedCapabilities(invocation.RequiredCapabilities)
	if err != nil {
		return InvocationCandidate{}, err
	}
	invocation.RequiredCapabilities = capabilities
	request := CreateRunRequest{
		RequestID:            invocation.RequestID,
		IdempotencyDigest:    invocation.IdempotencyDigest,
		InstallationID:       invocation.InstallationID,
		ConversationID:       conversationID,
		RequestEventID:       requestEventID,
		PreferredConnectorID: invocation.PreferredConnectorID,
		RequiredCapabilities: append([]string(nil), invocation.RequiredCapabilities...),
		DispatchMode:         invocation.DispatchMode,
		GrantVersion:         invocation.GrantVersion,
	}
	requestDigest, err := RequestDigest(tenantID, request)
	if err != nil {
		return InvocationCandidate{}, err
	}
	sourceDigest, err := SourceDigest(
		tenantID,
		matrixRoomID,
		matrixInvokeEventID,
		conversationID,
		invocation,
	)
	if err != nil {
		return InvocationCandidate{}, err
	}
	return InvocationCandidate{
		MatrixRoomID:         matrixRoomID,
		RequestID:            invocation.RequestID,
		MatrixInvokeEventID:  matrixInvokeEventID,
		MatrixInputEventID:   invocation.MatrixInputEventID,
		TenantID:             tenantID,
		InstallationID:       invocation.InstallationID,
		ConversationID:       conversationID,
		RequestEventID:       requestEventID,
		SourceDigest:         sourceDigest,
		IdempotencyDigest:    invocation.IdempotencyDigest,
		RequestDigest:        requestDigest,
		PreferredConnectorID: invocation.PreferredConnectorID,
		RequiredCapabilities: append([]string(nil), invocation.RequiredCapabilities...),
		DispatchMode:         invocation.DispatchMode,
		GrantVersion:         invocation.GrantVersion,
		CreatedAt:            createdAt.UTC(),
	}, nil
}

func (candidate InvocationCandidate) CreateRunRequest() CreateRunRequest {
	return CreateRunRequest{
		RequestID:            candidate.RequestID,
		IdempotencyDigest:    candidate.IdempotencyDigest,
		InstallationID:       candidate.InstallationID,
		ConversationID:       candidate.ConversationID,
		RequestEventID:       candidate.RequestEventID,
		PreferredConnectorID: candidate.PreferredConnectorID,
		RequiredCapabilities: append([]string(nil), candidate.RequiredCapabilities...),
		DispatchMode:         candidate.DispatchMode,
		GrantVersion:         candidate.GrantVersion,
	}
}

func optionalUUIDPart(value string) ([]byte, []byte, error) {
	if value == "" {
		return []byte{0}, nil, nil
	}
	identifier, err := parseUUIDv7(value, "preferred_connector_id")
	if err != nil {
		return nil, nil, err
	}
	return []byte{1}, identifier[:], nil
}

func dispatchNumber(mode DispatchMode) (uint64, error) {
	switch mode {
	case DispatchSingle:
		return 1, nil
	case DispatchFailover:
		return 2, nil
	default:
		return 0, newContractError(ContractUnsupportedValue, "dispatch_mode")
	}
}

func parseUUIDv7(value, field string) (uuid.UUID, error) {
	identifier, err := uuid.Parse(value)
	if err != nil || identifier.Version() != 7 || identifier.String() != value {
		return uuid.Nil, newContractError(ContractInvalidIdentifier, field)
	}
	return identifier, nil
}

func u64Part(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}
