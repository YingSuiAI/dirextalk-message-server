// Package contract owns the closed, versioned HTTPS command envelope accepted
// by Connection Stack V2. It deliberately has no AWS SDK dependency so the
// signing and parsing boundary can be tested independently of Lambda or a
// provider implementation.
package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// CommandSchema is the immutable wire schema shared with the Cloud
	// Orchestrator Broker client.
	CommandSchema = "dirextalk.aws.command/v2"
	// CommandSignatureSchema is the domain separator for node signatures.
	CommandSignatureSchema = "dirextalk.aws.command-signature/v2"

	ActionApprovalChallengeRequest = "approval.challenge.request"
	ActionRegistrationVerify       = "connection.registration.verify"
	ActionQuoteRequest             = "quote.request"
	ActionArtifactPut              = "artifact.put"
	ActionDeploymentCreate         = "deployment.create"
	ActionDeploymentObserve        = "deployment.observe"
	ActionWorkerTaskIssue          = "worker.task.issue"
	ActionWorkerTaskObserve        = "worker.task.observe"
	ActionDeploymentDestroy        = "deployment.destroy"

	maxCommandLifetime = 5 * time.Minute
	maxClockSkew       = time.Minute
	maxPayloadBytes    = 192 * 1024
	// MaxCommandBytes bounds the outer HTTP body and is shared by the Lambda
	// adapter and direct parser so future callers cannot bypass the limit.
	MaxCommandBytes = 256 * 1024
	maxSafeInteger  = int64(9007199254740991) // Number.MAX_SAFE_INTEGER

	canonicalInstantLayout = "2006-01-02T15:04:05.000Z"
)

var (
	idPattern               = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	keyIDPattern            = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	sha256Pattern           = regexp.MustCompile(`^[0-9a-f]{64}$`)
	canonicalInstantPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	canonicalIntegerPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

	baseFields = []string{
		"schema",
		"connection_id",
		"command_id",
		"node_key_id",
		"issued_at",
		"expires_at",
		"expected_generation",
		"node_counter",
		"action",
		"payload_b64",
		"payload_sha256",
		"signature_b64",
	}
)

// Error is safe to expose to an HTTP client. The error never includes a raw
// command, payload, signature, approval proof, or public key.
type Error struct {
	Code string
}

func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "connection stack command error"
	}
	return "connection stack command error: " + e.Code
}

func errCode(code string) error { return &Error{Code: code} }

// Code returns the stable safe code for an error produced by this package.
func Code(err error) string {
	var target *Error
	if errors.As(err, &target) && target.Code != "" {
		return target.Code
	}
	return "invalid_command"
}

// Command is the exact outer JSON envelope. ApprovalProof remains raw on
// purpose: this fail-closed foundation never interprets or forwards it before
// the deterministic-CBOR ApprovalV1 verifier and one-time consume store are
// ported.
type Command struct {
	Schema             string          `json:"schema"`
	ConnectionID       string          `json:"connection_id"`
	CommandID          string          `json:"command_id"`
	NodeKeyID          string          `json:"node_key_id"`
	IssuedAt           string          `json:"issued_at"`
	ExpiresAt          string          `json:"expires_at"`
	ExpectedGeneration int64           `json:"expected_generation"`
	NodeCounter        int64           `json:"node_counter"`
	Action             string          `json:"action"`
	PayloadB64         string          `json:"payload_b64"`
	PayloadSHA256      string          `json:"payload_sha256"`
	ApprovalProof      json.RawMessage `json:"approval_proof,omitempty"`
	SignatureB64       string          `json:"signature_b64"`
}

// Parse rejects unknown or duplicate fields before it unmarshals the command.
// It also verifies that the payload is canonically base64 encoded, hashes to
// payload_sha256, and contains exactly one duplicate-free JSON object. It does
// not claim action-specific payload or ApprovalV1 validation yet.
func Parse(raw []byte) (Command, error) {
	if len(raw) > MaxCommandBytes {
		return Command{}, errCode("request_too_large")
	}
	if !utf8.Valid(raw) {
		return Command{}, errCode("invalid_command")
	}
	fields, err := exactJSONObject(raw)
	if err != nil {
		return Command{}, errCode("invalid_command")
	}

	action, err := requiredString(fields, "action")
	if err != nil {
		return Command{}, errCode("invalid_command")
	}
	if !knownAction(action) {
		return Command{}, errCode("unsupported_action")
	}
	expected := baseFields
	if action == ActionDeploymentCreate {
		expected = append(append([]string(nil), baseFields...), "approval_proof")
	}
	if !exactFields(fields, expected) {
		return Command{}, errCode("invalid_command")
	}

	command := Command{Action: action}
	if command.Schema, err = requiredString(fields, "schema"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.ConnectionID, err = requiredString(fields, "connection_id"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.CommandID, err = requiredString(fields, "command_id"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.NodeKeyID, err = requiredString(fields, "node_key_id"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.IssuedAt, err = requiredString(fields, "issued_at"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.ExpiresAt, err = requiredString(fields, "expires_at"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.ExpectedGeneration, err = requiredSafeInteger(fields, "expected_generation"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.NodeCounter, err = requiredSafeInteger(fields, "node_counter"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.PayloadB64, err = requiredString(fields, "payload_b64"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.PayloadSHA256, err = requiredString(fields, "payload_sha256"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if command.SignatureB64, err = requiredString(fields, "signature_b64"); err != nil {
		return Command{}, errCode("invalid_command")
	}
	if action == ActionDeploymentCreate {
		command.ApprovalProof = append(json.RawMessage(nil), fields["approval_proof"]...)
		if err := validateJSONObjectValue(command.ApprovalProof); err != nil {
			return Command{}, errCode("invalid_approval_proof")
		}
	}
	if err := command.ValidateStructure(); err != nil {
		return Command{}, err
	}
	return command, nil
}

// ValidateStructure checks only fields that do not require time, key, storage,
// or AWS state. It is safe for an untrusted HTTP body.
func (c Command) ValidateStructure() error {
	if c.Schema != CommandSchema || !idPattern.MatchString(c.ConnectionID) || !idPattern.MatchString(c.CommandID) ||
		!keyIDPattern.MatchString(c.NodeKeyID) || !knownAction(c.Action) ||
		!sha256Pattern.MatchString(c.PayloadSHA256) || c.ExpectedGeneration <= 0 ||
		c.ExpectedGeneration > maxSafeInteger || c.NodeCounter < 0 || c.NodeCounter > maxSafeInteger {
		return errCode("invalid_command")
	}
	if _, err := parseCanonicalInstant(c.IssuedAt); err != nil {
		return errCode("invalid_command")
	}
	if _, err := parseCanonicalInstant(c.ExpiresAt); err != nil {
		return errCode("invalid_command")
	}
	payload, err := decodeCanonicalBase64(c.PayloadB64)
	if err != nil || len(payload) == 0 || len(payload) > maxPayloadBytes {
		return errCode("invalid_payload")
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != c.PayloadSHA256 {
		return errCode("payload_digest_mismatch")
	}
	if err := validateJSONObjectValue(payload); err != nil {
		return errCode("invalid_payload")
	}
	signature, err := decodeCanonicalBase64(c.SignatureB64)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errCode("invalid_command")
	}
	if c.Action == ActionDeploymentCreate && len(c.ApprovalProof) == 0 {
		return errCode("invalid_approval_proof")
	}
	if c.Action != ActionDeploymentCreate && len(c.ApprovalProof) != 0 {
		return errCode("invalid_command")
	}
	return nil
}

// ValidateAt checks the short-lived command window before a new command could
// be considered. The durable receipt store checks for an existing receipt
// before this method so an expired exact replay remains idempotent without
// causing a new side effect.
func (c Command) ValidateAt(now time.Time) error {
	if err := c.ValidateStructure(); err != nil {
		return err
	}
	issuedAt, _ := parseCanonicalInstant(c.IssuedAt)
	expiresAt, _ := parseCanonicalInstant(c.ExpiresAt)
	if !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime {
		return errCode("invalid_command")
	}
	if issuedAt.After(now.UTC().Add(maxClockSkew)) {
		return errCode("future_command")
	}
	if !expiresAt.After(now.UTC()) {
		return errCode("expired_command")
	}
	return nil
}

// VerifyNodeSignature validates the registered Ed25519 node signature for a
// non-mutating envelope. deployment.create deliberately returns
// operation_not_enabled until its ApprovalV1 deterministic-CBOR verifier is
// ported; callers must never use this method to bypass that gate.
func (c Command) VerifyNodeSignature(publicKey ed25519.PublicKey) error {
	if err := c.ValidateStructure(); err != nil {
		return err
	}
	if c.Action == ActionDeploymentCreate {
		return errCode("operation_not_enabled")
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return errCode("invalid_node_public_key")
	}
	signature, _ := base64.StdEncoding.DecodeString(c.SignatureB64)
	signatureBase, err := c.SignatureBase()
	if err != nil {
		return err
	}
	if !ed25519.Verify(publicKey, []byte(signatureBase), signature) {
		return errCode("invalid_node_signature")
	}
	return nil
}

// SignatureBase is byte-compatible with the existing V2 Go Broker client for
// every action that has no ApprovalV1 proof. Four approval lines are retained
// with empty values; omitting them would create a different signed protocol.
func (c Command) SignatureBase() (string, error) {
	if c.IsDeploymentCreate() {
		return "", errCode("operation_not_enabled")
	}
	return fmt.Sprintf(
		"%s\n"+
			"schema=%s\n"+
			"connection_id=%s\n"+
			"command_id=%s\n"+
			"node_key_id=%s\n"+
			"issued_at=%s\n"+
			"expires_at=%s\n"+
			"expected_generation=%d\n"+
			"node_counter=%d\n"+
			"action=%s\n"+
			"payload_sha256=%s\n"+
			"approval_binding_sha256=\n"+
			"approval_challenge_id=\n"+
			"approval_signature_sha256=\n"+
			"approval_proof_payload_sha256=\n",
		CommandSignatureSchema,
		c.Schema,
		c.ConnectionID,
		c.CommandID,
		c.NodeKeyID,
		c.IssuedAt,
		c.ExpiresAt,
		c.ExpectedGeneration,
		c.NodeCounter,
		c.Action,
		c.PayloadSHA256,
	), nil
}

// RequestSHA256 is the durable idempotency identity for non-deployment
// commands. It is intentionally the hash of the signature base rather than of
// the outer HTTP JSON bytes. deployment.create returns operation_not_enabled
// until its ApprovalV1 proof digest can be calculated byte-compatibly.
func (c Command) RequestSHA256() (string, error) {
	signatureBase, err := c.SignatureBase()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(signatureBase))
	return hex.EncodeToString(sum[:]), nil
}

func knownAction(action string) bool {
	switch action {
	case ActionApprovalChallengeRequest,
		ActionRegistrationVerify,
		ActionQuoteRequest,
		ActionArtifactPut,
		ActionDeploymentCreate,
		ActionDeploymentObserve,
		ActionWorkerTaskIssue,
		ActionWorkerTaskObserve,
		ActionDeploymentDestroy:
		return true
	default:
		return false
	}
}

// ValidConnectionID and ValidNodeKeyID are shared by Stack configuration and
// the untrusted envelope parser so an environment value cannot weaken the
// identity format enforced on the wire.
func ValidConnectionID(value string) bool { return idPattern.MatchString(value) }

func ValidNodeKeyID(value string) bool { return keyIDPattern.MatchString(value) }

func parseCanonicalInstant(value string) (time.Time, error) {
	if !canonicalInstantPattern.MatchString(value) {
		return time.Time{}, errors.New("noncanonical instant")
	}
	parsed, err := time.Parse(canonicalInstantLayout, value)
	if err != nil || parsed.UTC().Format(canonicalInstantLayout) != value {
		return time.Time{}, errors.New("invalid instant")
	}
	return parsed.UTC(), nil
}

func decodeCanonicalBase64(value string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != value {
		return nil, errors.New("noncanonical base64")
	}
	return decoded, nil
}

func exactJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return nil, errors.New("expected object")
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok || name == "" {
			return nil, errors.New("invalid object key")
		}
		if _, exists := fields[name]; exists {
			return nil, errors.New("duplicate object key")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	last, err := decoder.Token()
	if err != nil || last != json.Delim('}') {
		return nil, errors.New("unterminated object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing JSON data")
	}
	return fields, nil
}

func exactFields(fields map[string]json.RawMessage, expected []string) bool {
	if len(fields) != len(expected) {
		return false
	}
	for _, name := range expected {
		if _, ok := fields[name]; !ok {
			return false
		}
	}
	return true
}

func requiredString(fields map[string]json.RawMessage, name string) (string, error) {
	raw, ok := fields[name]
	if !ok {
		return "", errors.New("missing field")
	}
	var result string
	if err := decodeSingle(raw, &result); err != nil || result == "" {
		return "", errors.New("invalid string")
	}
	return result, nil
}

func requiredSafeInteger(fields map[string]json.RawMessage, name string) (int64, error) {
	raw, ok := fields[name]
	if !ok {
		return 0, errors.New("missing number")
	}
	var number json.Number
	if err := decodeSingle(raw, &number); err != nil || !canonicalIntegerPattern.MatchString(number.String()) {
		return 0, errors.New("invalid number")
	}
	parsed, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || parsed > maxSafeInteger {
		return 0, errors.New("unsafe number")
	}
	return parsed, nil
}

func decodeSingle(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func validateJSONObjectValue(raw []byte) error {
	if !utf8.Valid(raw) {
		return errors.New("invalid UTF-8 JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONValue(decoder, true); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func validateJSONValue(decoder *json.Decoder, requireObject bool) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		if requireObject {
			return errors.New("expected object")
		}
		switch token.(type) {
		case nil, bool, string, json.Number:
			return nil
		default:
			return errors.New("invalid JSON value")
		}
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, exists := seen[name]; exists {
				return errors.New("duplicate object key")
			}
			seen[name] = struct{}{}
			if err := validateJSONValue(decoder, false); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("unterminated object")
		}
		return nil
	case '[':
		if requireObject {
			return errors.New("expected object")
		}
		for decoder.More() {
			if err := validateJSONValue(decoder, false); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("unterminated array")
		}
		return nil
	default:
		return errors.New("invalid JSON delimiter")
	}
}

// CanonicalInstant is exported for tests and future providers that need an
// exact V2 timestamp when building an envelope.
func CanonicalInstant(value time.Time) string {
	return value.UTC().Format(canonicalInstantLayout)
}

// IsDeploymentCreate reports whether an action must remain blocked until the
// approval verifier, reservation, and EC2 read-back paths are all ported.
func (c Command) IsDeploymentCreate() bool { return c.Action == ActionDeploymentCreate }

// HasApprovalProof reports only whether an outer deployment envelope carries
// a syntactically valid proof object. It is not an approval authorization
// decision.
func (c Command) HasApprovalProof() bool {
	return len(c.ApprovalProof) > 0 && strings.TrimSpace(string(c.ApprovalProof)) != ""
}
