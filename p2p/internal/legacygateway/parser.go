package legacygateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"unicode"
	"unicode/utf8"

	"github.com/matrix-org/gomatrixserverlib/spec"
)

type ContractErrorKind string

const (
	ContractInvalidJSON       ContractErrorKind = "invalid_json"
	ContractInvalidIdentifier ContractErrorKind = "invalid_identifier"
	ContractInvalidLength     ContractErrorKind = "invalid_length"
	ContractInvalidValue      ContractErrorKind = "invalid_value"
	ContractUnsupportedValue  ContractErrorKind = "unsupported_value"
)

type ContractError struct {
	Kind  ContractErrorKind
	Field string
}

func (err *ContractError) Error() string {
	return fmt.Sprintf("legacy gateway contract field %s is invalid (%s)", err.Field, err.Kind)
}

func newContractError(kind ContractErrorKind, field string) *ContractError {
	return &ContractError{Kind: kind, Field: field}
}

type invocationWire struct {
	RequestID            string          `json:"request_id"`
	InstallationID       string          `json:"installation_id"`
	PreferredConnectorID *string         `json:"preferred_connector_id"`
	DispatchMode         DispatchMode    `json:"dispatch_mode"`
	GrantVersion         uint64          `json:"grant_version"`
	InputEventID         string          `json:"input_event_id"`
	RequiredCapabilities json.RawMessage `json:"required_capabilities"`
	IdempotencyKey       string          `json:"idempotency_key"`
}

// ParseInvocationContent strictly decodes one bounded Matrix event content.
// Unknown fields, duplicate fields and trailing JSON values are rejected. The
// returned value contains no raw idempotency key.
func ParseInvocationContent(tenantID, matrixRoomID string, content []byte) (Invocation, error) {
	if len(content) == 0 || len(content) > MaxInvocationContentBytes {
		return Invocation{}, newContractError(ContractInvalidLength, "content")
	}
	if !utf8.Valid(content) {
		return Invocation{}, newContractError(ContractInvalidJSON, "content")
	}
	if err := rejectDuplicateObjectFields(content); err != nil {
		return Invocation{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var wire invocationWire
	if err := decoder.Decode(&wire); err != nil {
		return Invocation{}, newContractError(ContractInvalidJSON, "content")
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Invocation{}, err
	}
	if _, err := parseUUIDv7(wire.RequestID, "request_id"); err != nil {
		return Invocation{}, err
	}
	if _, err := parseUUIDv7(wire.InstallationID, "installation_id"); err != nil {
		return Invocation{}, err
	}
	preferredConnectorID := ""
	if wire.PreferredConnectorID != nil {
		if _, err := parseUUIDv7(*wire.PreferredConnectorID, "preferred_connector_id"); err != nil {
			return Invocation{}, err
		}
		preferredConnectorID = *wire.PreferredConnectorID
	}
	if len(wire.RequiredCapabilities) == 0 || bytes.Equal(bytes.TrimSpace(wire.RequiredCapabilities), []byte("null")) {
		return Invocation{}, newContractError(ContractInvalidValue, "required_capabilities")
	}
	var requiredCapabilities []string
	if err := json.Unmarshal(wire.RequiredCapabilities, &requiredCapabilities); err != nil {
		return Invocation{}, newContractError(ContractInvalidJSON, "content")
	}
	capabilities, err := normalizedCapabilities(requiredCapabilities)
	if err != nil {
		return Invocation{}, err
	}
	if _, err := dispatchNumber(wire.DispatchMode); err != nil {
		return Invocation{}, err
	}
	if wire.GrantVersion == 0 || wire.GrantVersion > MaxJSONSafeUint {
		return Invocation{}, newContractError(ContractInvalidValue, "grant_version")
	}
	if err := validateMatrixEventID(wire.InputEventID, "input_event_id"); err != nil {
		return Invocation{}, err
	}
	idempotencyDigest, err := IdempotencyDigest(tenantID, matrixRoomID, wire.IdempotencyKey)
	if err != nil {
		return Invocation{}, err
	}
	return Invocation{
		RequestID:            wire.RequestID,
		InstallationID:       wire.InstallationID,
		PreferredConnectorID: preferredConnectorID,
		RequiredCapabilities: capabilities,
		DispatchMode:         wire.DispatchMode,
		GrantVersion:         wire.GrantVersion,
		MatrixInputEventID:   wire.InputEventID,
		IdempotencyDigest:    idempotencyDigest,
	}, nil
}

func rejectDuplicateObjectFields(content []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	first, err := decoder.Token()
	if err != nil {
		return newContractError(ContractInvalidJSON, "content")
	}
	delimiter, ok := first.(json.Delim)
	if !ok || delimiter != '{' {
		return newContractError(ContractInvalidJSON, "content")
	}
	seen := make(map[string]struct{}, 8)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return newContractError(ContractInvalidJSON, "content")
		}
		name, ok := token.(string)
		if !ok {
			return newContractError(ContractInvalidJSON, "content")
		}
		if _, exists := seen[name]; exists {
			return newContractError(ContractInvalidJSON, "content")
		}
		seen[name] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return newContractError(ContractInvalidJSON, "content")
		}
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return newContractError(ContractInvalidJSON, "content")
	}
	return requireJSONEOF(decoder)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return newContractError(ContractInvalidJSON, "content")
	}
	return nil
}

func normalizedCapabilities(values []string) ([]string, error) {
	if len(values) > MaxRequiredCapabilities {
		return nil, newContractError(ContractInvalidLength, "required_capabilities")
	}
	result := append([]string(nil), values...)
	for _, value := range result {
		if !validCapabilityName(value) {
			return nil, newContractError(ContractInvalidValue, "required_capabilities")
		}
	}
	sort.Strings(result)
	for index := 1; index < len(result); index++ {
		if result[index-1] == result[index] {
			return nil, newContractError(ContractInvalidValue, "required_capabilities")
		}
	}
	return result, nil
}

func validCapabilityName(value string) bool {
	if len(value) == 0 || len(value) > MaxCapabilityNameBytes {
		return false
	}
	for index, current := range []byte(value) {
		if index == 0 && !isASCIILowerOrDigit(current) {
			return false
		}
		if !isASCIILowerOrDigit(current) && current != '.' && current != '_' &&
			current != '-' && current != '/' && current != ':' {
			return false
		}
	}
	return true
}

func isASCIILowerOrDigit(value byte) bool {
	return value >= 'a' && value <= 'z' || value >= '0' && value <= '9'
}

func validateMatrixRoomID(value string) error {
	if len(value) == 0 || len(value) > MaxMatrixIDBytes || !utf8.ValidString(value) {
		return newContractError(ContractInvalidIdentifier, "matrix_room_id")
	}
	if _, err := spec.NewRoomID(value); err != nil {
		return newContractError(ContractInvalidIdentifier, "matrix_room_id")
	}
	return nil
}

func validateMatrixEventID(value, field string) error {
	if len(value) < 2 || len(value) > MaxMatrixIDBytes || value[0] != '$' || !utf8.ValidString(value) {
		return newContractError(ContractInvalidIdentifier, field)
	}
	for _, current := range value[1:] {
		if unicode.IsControl(current) || unicode.IsSpace(current) {
			return newContractError(ContractInvalidIdentifier, field)
		}
	}
	return nil
}
