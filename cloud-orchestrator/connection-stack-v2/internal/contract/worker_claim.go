package contract

import (
	"encoding/base64"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	WorkerSessionClaimSchema         = "dirextalk.worker-session-claim/v1"
	WorkerSessionClaimResponseSchema = "dirextalk.worker-session-claim-response/v1"
	MaxWorkerClaimBytes              = 192 * 1024
)

var workerClaimNamedSHA256Pattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

var workerClaimFields = []string{
	"schema", "connection_id", "deployment_id", "bootstrap_session_id",
	"worker_image_digest", "artifact_manifest_digest",
	"instance_identity_document_b64", "instance_identity_signature_b64",
}

type WorkerSessionClaimRequest struct {
	Schema                       string `json:"schema"`
	ConnectionID                 string `json:"connection_id"`
	DeploymentID                 string `json:"deployment_id"`
	BootstrapSessionID           string `json:"bootstrap_session_id"`
	WorkerImageDigest            string `json:"worker_image_digest"`
	ArtifactManifestDigest       string `json:"artifact_manifest_digest"`
	InstanceIdentityDocumentB64  string `json:"instance_identity_document_b64"`
	InstanceIdentitySignatureB64 string `json:"instance_identity_signature_b64"`
}

func ParseWorkerSessionClaimRequest(raw []byte) (WorkerSessionClaimRequest, error) {
	if len(raw) == 0 || len(raw) > MaxWorkerClaimBytes {
		return WorkerSessionClaimRequest{}, errCode("invalid_worker_claim")
	}
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, workerClaimFields) {
		return WorkerSessionClaimRequest{}, errCode("invalid_worker_claim")
	}
	var request WorkerSessionClaimRequest
	if err := json.Unmarshal(raw, &request); err != nil || request.Validate() != nil {
		return WorkerSessionClaimRequest{}, errCode("invalid_worker_claim")
	}
	canonical, err := json.Marshal(request)
	if err != nil || string(canonical) != string(raw) {
		return WorkerSessionClaimRequest{}, errCode("noncanonical_worker_claim")
	}
	return request, nil
}

func (request WorkerSessionClaimRequest) Validate() error {
	if request.Schema != WorkerSessionClaimSchema || !ValidConnectionID(request.ConnectionID) ||
		!ValidID(request.DeploymentID) || !ValidID(request.BootstrapSessionID) ||
		!workerClaimNamedSHA256Pattern.MatchString(request.WorkerImageDigest) ||
		!workerClaimNamedSHA256Pattern.MatchString(request.ArtifactManifestDigest) ||
		!canonicalBase64Within(request.InstanceIdentityDocumentB64, 64*1024) ||
		!canonicalBase64Within(request.InstanceIdentitySignatureB64, 32*1024) {
		return errCode("invalid_worker_claim")
	}
	return nil
}

func canonicalBase64Within(value string, maximum int) bool {
	decoded, err := base64.StdEncoding.DecodeString(value)
	return err == nil && len(decoded) > 0 && len(decoded) <= maximum && base64.StdEncoding.EncodeToString(decoded) == value
}

func ValidWorkerBootstrapEndpoint(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.Hostname() != "" && parsed.User == nil &&
		parsed.RawQuery == "" && !parsed.ForceQuery && parsed.Fragment == "" && parsed.Opaque == "" && parsed.RawPath == "" &&
		parsed.Path == "/prod/v2/worker-sessions" && parsed.Port() == "" && strings.ToLower(parsed.Host) == parsed.Host
}

type WorkerSessionClaimResponse struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	DeploymentID       string `json:"deployment_id"`
	BootstrapSessionID string `json:"bootstrap_session_id"`
	LeaseEpoch         int64  `json:"lease_epoch"`
	LeaseExpiresAt     string `json:"lease_expires_at"`
	AccessToken        string `json:"access_token"`
}

func NewWorkerSessionClaimResponse(request WorkerSessionClaimRequest, epoch int64, leaseExpiresAt, token string) (WorkerSessionClaimResponse, error) {
	response := WorkerSessionClaimResponse{
		Schema: WorkerSessionClaimResponseSchema, ConnectionID: request.ConnectionID,
		DeploymentID: request.DeploymentID, BootstrapSessionID: request.BootstrapSessionID,
		LeaseEpoch: epoch, LeaseExpiresAt: leaseExpiresAt, AccessToken: token,
	}
	if response.ValidateFor(request) != nil {
		return WorkerSessionClaimResponse{}, errCode("invalid_worker_claim_response")
	}
	return response, nil
}

func (response WorkerSessionClaimResponse) ValidateFor(request WorkerSessionClaimRequest) error {
	if response.Schema != WorkerSessionClaimResponseSchema || response.ConnectionID != request.ConnectionID ||
		response.DeploymentID != request.DeploymentID || response.BootstrapSessionID != request.BootstrapSessionID ||
		response.LeaseEpoch < 1 || len(response.AccessToken) < 16 || len(response.AccessToken) > 4096 {
		return errCode("invalid_worker_claim_response")
	}
	parsed, err := time.Parse(canonicalInstantLayout, response.LeaseExpiresAt)
	if err != nil || parsed.UTC().Format(canonicalInstantLayout) != response.LeaseExpiresAt {
		return errCode("invalid_worker_claim_response")
	}
	for _, character := range response.AccessToken {
		if character < 0x21 || character > 0x7e || character == '"' || character == '\\' {
			return errCode("invalid_worker_claim_response")
		}
	}
	return nil
}
