package broker

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	ArtifactPutAction                     = "artifact.put"
	ArtifactPutPrepareSchema              = "dirextalk.artifact-put-prepare/v1"
	ArtifactPutCompleteSchema             = "dirextalk.artifact-put-complete/v1"
	ArtifactPutPrepareResultSchema        = "dirextalk.artifact-put-prepare-result/v1"
	ArtifactPutCompleteResultSchema       = "dirextalk.artifact-put-complete-result/v1"
	ArtifactTarMediaType                  = "application/x-tar"
	ArtifactMaximumArchiveSize      int64 = 256 << 20
)

var artifactVersionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/=-]+$`)

// ArtifactPutBinding is the immutable identity shared by prepare and complete.
// ArchiveSHA256 is the hash of the tar bytes and intentionally omits the
// "sha256:" prefix used by content-addressed control-plane artifacts.
type ArtifactPutBinding struct {
	DeploymentID   string `json:"deployment_id"`
	TaskID         string `json:"task_id"`
	ExecutionID    string `json:"execution_id"`
	RecipeDigest   string `json:"recipe_digest"`
	ArtifactDigest string `json:"artifact_digest"`
	ManifestDigest string `json:"manifest_digest"`
	ArchiveSHA256  string `json:"archive_sha256"`
	SizeBytes      int64  `json:"size_bytes"`
	MediaType      string `json:"media_type"`
}

type ArtifactPutPrepareRequest struct {
	Schema string `json:"schema"`
	ArtifactPutBinding
}

type ArtifactPutCompleteRequest struct {
	Schema string `json:"schema"`
	ArtifactPutBinding
	VersionID string `json:"version_id"`
}

type ArtifactPutCommandInput struct {
	ConnectionID       string
	CommandID          string
	NodeKeyID          string
	ExpectedGeneration int64
	NodeCounter        int64
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Prepare            *ArtifactPutPrepareRequest
	Complete           *ArtifactPutCompleteRequest
	PrivateKey         ed25519.PrivateKey
}

type ArtifactPutCommand struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	CommandID          string `json:"command_id"`
	NodeKeyID          string `json:"node_key_id"`
	IssuedAt           string `json:"issued_at"`
	ExpiresAt          string `json:"expires_at"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	Action             string `json:"action"`
	PayloadB64         string `json:"payload_b64"`
	PayloadSHA256      string `json:"payload_sha256"`
	SignatureB64       string `json:"signature_b64"`
}

type ArtifactPutReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type ArtifactUploadingState struct {
	ArtifactPutBinding
	State     string `json:"state"`
	ExpiresAt string `json:"expires_at"`
}

type ArtifactVerifiedState struct {
	ArtifactPutBinding
	State      string `json:"state"`
	VersionID  string `json:"version_id"`
	VerifiedAt string `json:"verified_at"`
}

type ArtifactUploadGrant struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	ExpiresAt string            `json:"expires_at"`
	Headers   map[string]string `json:"headers"`
}

type ArtifactPutPrepareResult struct {
	Schema   string                 `json:"schema"`
	Status   string                 `json:"status"`
	Receipt  ArtifactPutReceipt     `json:"receipt"`
	Artifact ArtifactUploadingState `json:"artifact"`
	Upload   ArtifactUploadGrant    `json:"upload"`
}

type ArtifactPutCompleteResult struct {
	Schema   string                `json:"schema"`
	Status   string                `json:"status"`
	Receipt  ArtifactPutReceipt    `json:"receipt"`
	Artifact ArtifactVerifiedState `json:"artifact"`
}

func (binding ArtifactPutBinding) Validate() error {
	if !idPattern.MatchString(binding.DeploymentID) || !idPattern.MatchString(binding.TaskID) ||
		!idPattern.MatchString(binding.ExecutionID) || !namedSHA256Pattern.MatchString(binding.RecipeDigest) ||
		!namedSHA256Pattern.MatchString(binding.ArtifactDigest) || !namedSHA256Pattern.MatchString(binding.ManifestDigest) ||
		!sha256Pattern.MatchString(binding.ArchiveSHA256) || binding.SizeBytes < 1 ||
		binding.SizeBytes > ArtifactMaximumArchiveSize || binding.MediaType != ArtifactTarMediaType {
		return errors.New("artifact put binding is invalid")
	}
	return nil
}

func (request ArtifactPutPrepareRequest) Validate() error {
	if request.Schema != ArtifactPutPrepareSchema {
		return errors.New("artifact put prepare request is invalid")
	}
	return request.ArtifactPutBinding.Validate()
}

func (request ArtifactPutCompleteRequest) Validate() error {
	if request.Schema != ArtifactPutCompleteSchema || len(request.VersionID) > 1024 || !artifactVersionIDPattern.MatchString(request.VersionID) {
		return errors.New("artifact put complete request is invalid")
	}
	return request.ArtifactPutBinding.Validate()
}

func NewArtifactPutCommand(input ArtifactPutCommandInput) (ArtifactPutCommand, error) {
	if len(input.PrivateKey) != ed25519.PrivateKeySize || (input.Prepare == nil) == (input.Complete == nil) {
		return ArtifactPutCommand{}, newError("invalid_artifact_put_command", nil)
	}
	var payload []byte
	var err error
	if input.Prepare != nil {
		if err = input.Prepare.Validate(); err == nil {
			payload, err = json.Marshal(input.Prepare)
		}
	} else {
		if err = input.Complete.Validate(); err == nil {
			payload, err = json.Marshal(input.Complete)
		}
	}
	if err != nil {
		return ArtifactPutCommand{}, newError("invalid_artifact_put_request", err)
	}
	command := ArtifactPutCommand{
		Schema: CommandSchema, ConnectionID: input.ConnectionID, CommandID: input.CommandID,
		NodeKeyID: input.NodeKeyID, IssuedAt: canonicalInstant(input.IssuedAt), ExpiresAt: canonicalInstant(input.ExpiresAt),
		ExpectedGeneration: input.ExpectedGeneration, NodeCounter: input.NodeCounter, Action: ArtifactPutAction,
		PayloadB64: base64.StdEncoding.EncodeToString(payload), PayloadSHA256: sha256Hex(payload),
	}
	if err = command.validate(false); err != nil {
		return ArtifactPutCommand{}, err
	}
	command.SignatureB64 = base64.StdEncoding.EncodeToString(ed25519.Sign(input.PrivateKey, []byte(command.SignatureBase())))
	return command, command.validate(true)
}

func ParseArtifactPutCommand(raw []byte) (ArtifactPutCommand, error) {
	if _, err := exactJSONObject(raw, commandFields); err != nil {
		return ArtifactPutCommand{}, newError("invalid_command", err)
	}
	var command ArtifactPutCommand
	if err := decodeStrictJSON(raw, &command); err != nil {
		return ArtifactPutCommand{}, err
	}
	return command, command.validate(true)
}

func (command ArtifactPutCommand) PrepareRequest() (ArtifactPutPrepareRequest, error) {
	payload, err := command.payload()
	if err != nil {
		return ArtifactPutPrepareRequest{}, err
	}
	var request ArtifactPutPrepareRequest
	if err = decodeStrictJSON(payload, &request); err != nil || request.Validate() != nil {
		return ArtifactPutPrepareRequest{}, newError("invalid_artifact_put_request", err)
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return ArtifactPutPrepareRequest{}, newError("noncanonical_payload", nil)
	}
	return request, nil
}

func (command ArtifactPutCommand) CompleteRequest() (ArtifactPutCompleteRequest, error) {
	payload, err := command.payload()
	if err != nil {
		return ArtifactPutCompleteRequest{}, err
	}
	var request ArtifactPutCompleteRequest
	if err = decodeStrictJSON(payload, &request); err != nil || request.Validate() != nil {
		return ArtifactPutCompleteRequest{}, newError("invalid_artifact_put_request", err)
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return ArtifactPutCompleteRequest{}, newError("noncanonical_payload", nil)
	}
	return request, nil
}

func (command ArtifactPutCommand) validate(requireSignature bool) error {
	if command.Schema != CommandSchema || !idPattern.MatchString(command.ConnectionID) || !idPattern.MatchString(command.CommandID) ||
		!keyIDPattern.MatchString(command.NodeKeyID) || command.Action != ArtifactPutAction || !safePositive(command.ExpectedGeneration) ||
		!safePositive(command.NodeCounter) {
		return newError("invalid_command", nil)
	}
	issuedAt, issuedErr := parseCanonicalInstant(command.IssuedAt)
	expiresAt, expiresErr := parseCanonicalInstant(command.ExpiresAt)
	if issuedErr != nil || expiresErr != nil || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > maxCommandLifetime {
		return newError("invalid_command", nil)
	}
	payload, err := command.payload()
	if err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := decodeStrictJSON(payload, &fields); err != nil {
		return newError("invalid_artifact_put_request", err)
	}
	var schema string
	if json.Unmarshal(fields["schema"], &schema) != nil {
		return newError("invalid_artifact_put_request", err)
	}
	if schema == ArtifactPutPrepareSchema {
		if _, err = command.PrepareRequest(); err != nil {
			return err
		}
	} else if schema == ArtifactPutCompleteSchema {
		if _, err = command.CompleteRequest(); err != nil {
			return err
		}
	} else {
		return newError("invalid_artifact_put_request", nil)
	}
	if requireSignature {
		signature, decodeErr := decodeCanonicalBase64(command.SignatureB64)
		if decodeErr != nil || len(signature) != ed25519.SignatureSize {
			return newError("invalid_command", decodeErr)
		}
	}
	return nil
}

func (command ArtifactPutCommand) payload() ([]byte, error) {
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil || len(payload) == 0 || len(payload) > 64*1024 || sha256Hex(payload) != command.PayloadSHA256 {
		return nil, newError("invalid_payload", err)
	}
	return payload, nil
}

func (command ArtifactPutCommand) SignatureBase() string {
	return nodeSignatureBase(nodeSignatureFields{Schema: command.Schema, ConnectionID: command.ConnectionID, CommandID: command.CommandID,
		NodeKeyID: command.NodeKeyID, IssuedAt: command.IssuedAt, ExpiresAt: command.ExpiresAt,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action,
		PayloadSHA256: command.PayloadSHA256})
}

func (command ArtifactPutCommand) RequestSHA256() string {
	return sha256Hex([]byte(command.SignatureBase()))
}

func (client *Client) SubmitArtifactPutPrepare(ctx context.Context, command ArtifactPutCommand) (ArtifactPutPrepareResult, error) {
	var result ArtifactPutPrepareResult
	if _, err := command.PrepareRequest(); err != nil {
		return result, err
	}
	if err := client.submitArtifactPut(ctx, command, &result); err != nil {
		return result, err
	}
	if err := ValidateArtifactPutPrepareResult(command, result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	return result, nil
}

func (client *Client) SubmitArtifactPutComplete(ctx context.Context, command ArtifactPutCommand) (ArtifactPutCompleteResult, error) {
	var result ArtifactPutCompleteResult
	if _, err := command.CompleteRequest(); err != nil {
		return result, err
	}
	if err := client.submitArtifactPut(ctx, command, &result); err != nil {
		return result, err
	}
	if err := ValidateArtifactPutCompleteResult(command, result); err != nil {
		return result, newError("invalid_broker_response", err)
	}
	return result, nil
}

func (client *Client) submitArtifactPut(ctx context.Context, command ArtifactPutCommand, target any) error {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return newError("broker_client_unavailable", nil)
	}
	if err := command.validate(true); err != nil {
		return err
	}
	body, _ := json.Marshal(command)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		return newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return newHTTPError(code, response.StatusCode, nil)
		}
		return newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "application/json") {
		return newError("invalid_broker_content_type", err)
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, client.maxResponseBytes+1))
	if err != nil || int64(len(raw)) > client.maxResponseBytes {
		return newError("broker_response_unavailable", err)
	}
	if err = decodeStrictJSON(raw, target); err != nil {
		return newError("invalid_broker_response", err)
	}
	return nil
}

func ValidateArtifactPutPrepareResult(command ArtifactPutCommand, result ArtifactPutPrepareResult) error {
	request, err := command.PrepareRequest()
	if err != nil || result.Schema != ArtifactPutPrepareResultSchema || result.Status != "uploading" ||
		!sameArtifactBinding(result.Artifact.ArtifactPutBinding, request.ArtifactPutBinding) || result.Artifact.State != "uploading" ||
		validateArtifactReceipt(command, result.Receipt) != nil || validateArtifactUploadGrant(result.Upload, request.ArtifactPutBinding) != nil ||
		result.Artifact.ExpiresAt != result.Upload.ExpiresAt {
		return errors.New("artifact put prepare result is invalid")
	}
	return nil
}

func ValidateArtifactPutCompleteResult(command ArtifactPutCommand, result ArtifactPutCompleteResult) error {
	request, err := command.CompleteRequest()
	if err != nil || result.Schema != ArtifactPutCompleteResultSchema || result.Status != "verified" ||
		!sameArtifactBinding(result.Artifact.ArtifactPutBinding, request.ArtifactPutBinding) || result.Artifact.State != "verified" ||
		result.Artifact.VersionID != request.VersionID || len(result.Artifact.VersionID) > 1024 || !artifactVersionIDPattern.MatchString(result.Artifact.VersionID) ||
		validateArtifactReceipt(command, result.Receipt) != nil {
		return errors.New("artifact put complete result is invalid")
	}
	if _, err = parseCanonicalInstant(result.Artifact.VerifiedAt); err != nil {
		return errors.New("artifact put complete timestamp is invalid")
	}
	return nil
}

func validateArtifactReceipt(command ArtifactPutCommand, receipt ArtifactPutReceipt) error {
	if receipt.Schema != ReceiptSchema || (receipt.Disposition != "committed" && receipt.Disposition != "idempotent") ||
		receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration ||
		receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID ||
		receipt.RequestSHA256 != command.RequestSHA256() || receipt.Action != ArtifactPutAction {
		return errors.New("artifact put receipt is invalid")
	}
	return nil
}

func validateArtifactUploadGrant(grant ArtifactUploadGrant, binding ArtifactPutBinding) error {
	if grant.Method != http.MethodPut || len(grant.Headers) != 3 || grant.Headers["Content-Type"] != ArtifactTarMediaType ||
		grant.Headers["x-amz-server-side-encryption"] != "aws:kms" {
		return errors.New("artifact upload grant is invalid")
	}
	digest, err := base64.StdEncoding.DecodeString(grant.Headers["x-amz-checksum-sha256"])
	if err != nil || len(digest) != sha256.Size || base64.StdEncoding.EncodeToString(digest) != grant.Headers["x-amz-checksum-sha256"] ||
		!bytes.Equal(digest, mustDecodeArtifactHash(binding.ArchiveSHA256)) {
		return errors.New("artifact upload checksum is invalid")
	}
	parsed, err := url.Parse(grant.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery == "" ||
		!strings.Contains(strings.ToLower(parsed.RawQuery), "x-amz-signature=") {
		return errors.New("artifact upload URL is invalid")
	}
	if _, err = parseCanonicalInstant(grant.ExpiresAt); err != nil {
		return errors.New("artifact upload expiry is invalid")
	}
	return nil
}

func mustDecodeArtifactHash(value string) []byte {
	decoded := make([]byte, sha256.Size)
	for index := 0; index < len(decoded); index++ {
		var high, low byte
		high = fromHex(value[index*2])
		low = fromHex(value[index*2+1])
		decoded[index] = high<<4 | low
	}
	return decoded
}

func fromHex(value byte) byte {
	if value >= '0' && value <= '9' {
		return value - '0'
	}
	return value - 'a' + 10
}

func sameArtifactBinding(left, right ArtifactPutBinding) bool { return left == right }
