package contract

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
)

const (
	ArtifactPutPrepareSchema              = "dirextalk.artifact-put-prepare/v1"
	ArtifactPutCompleteSchema             = "dirextalk.artifact-put-complete/v1"
	ArtifactPutPrepareResultSchema        = "dirextalk.artifact-put-prepare-result/v1"
	ArtifactPutCompleteResultSchema       = "dirextalk.artifact-put-complete-result/v1"
	ArtifactMediaType                     = "application/x-tar"
	MaxArtifactSizeBytes            int64 = 256 << 20
)

var archiveSHA256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var artifactVersionPattern = regexp.MustCompile(`^[A-Za-z0-9._~+=/-]+$`)

type ArtifactBinding struct {
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
	ArtifactBinding
}
type ArtifactPutCompleteRequest struct {
	Schema string `json:"schema"`
	ArtifactBinding
	VersionID string `json:"version_id"`
}

type ArtifactState struct {
	ArtifactBinding
	State      string `json:"state"`
	ExpiresAt  string `json:"expires_at,omitempty"`
	VersionID  string `json:"version_id,omitempty"`
	VerifiedAt string `json:"verified_at,omitempty"`
}
type ArtifactUpload struct {
	Method    string            `json:"method"`
	URL       string            `json:"url"`
	ExpiresAt string            `json:"expires_at"`
	Headers   map[string]string `json:"headers"`
}
type ArtifactAccess struct {
	Method        string `json:"method"`
	URL           string `json:"url"`
	ExpiresAt     string `json:"expires_at"`
	VersionID     string `json:"version_id"`
	MediaType     string `json:"media_type"`
	SizeBytes     int64  `json:"size_bytes"`
	ArchiveSHA256 string `json:"archive_sha256"`
}
type ArtifactPutPrepareResult struct {
	Schema   string                   `json:"schema"`
	Status   string                   `json:"status"`
	Receipt  DeploymentCommandReceipt `json:"receipt"`
	Artifact ArtifactState            `json:"artifact"`
	Upload   ArtifactUpload           `json:"upload"`
}
type ArtifactPutCompleteResult struct {
	Schema   string                   `json:"schema"`
	Status   string                   `json:"status"`
	Receipt  DeploymentCommandReceipt `json:"receipt"`
	Artifact ArtifactState            `json:"artifact"`
}

func ParseArtifactPutPrepare(command Command) (ArtifactPutPrepareRequest, error) {
	payload, err := command.actionPayload()
	if err != nil {
		return ArtifactPutPrepareRequest{}, errCode("invalid_artifact_put")
	}
	fields, err := exactJSONObject(payload)
	if err != nil || !exactFields(fields, []string{"schema", "deployment_id", "task_id", "execution_id", "recipe_digest", "artifact_digest", "manifest_digest", "archive_sha256", "size_bytes", "media_type"}) {
		return ArtifactPutPrepareRequest{}, errCode("invalid_artifact_put")
	}
	var request ArtifactPutPrepareRequest
	if decodeSingle(payload, &request) != nil || request.Schema != ArtifactPutPrepareSchema || request.ArtifactBinding.Validate() != nil {
		return ArtifactPutPrepareRequest{}, errCode("invalid_artifact_put")
	}
	return request, nil
}
func ParseArtifactPutComplete(command Command) (ArtifactPutCompleteRequest, error) {
	payload, err := command.actionPayload()
	if err != nil {
		return ArtifactPutCompleteRequest{}, errCode("invalid_artifact_put")
	}
	fields, err := exactJSONObject(payload)
	if err != nil || !exactFields(fields, []string{"schema", "deployment_id", "task_id", "execution_id", "recipe_digest", "artifact_digest", "manifest_digest", "archive_sha256", "size_bytes", "media_type", "version_id"}) {
		return ArtifactPutCompleteRequest{}, errCode("invalid_artifact_put")
	}
	var request ArtifactPutCompleteRequest
	if decodeSingle(payload, &request) != nil || request.Schema != ArtifactPutCompleteSchema || request.ArtifactBinding.Validate() != nil || request.VersionID == "null" || len(request.VersionID) > 1024 || !artifactVersionPattern.MatchString(request.VersionID) {
		return ArtifactPutCompleteRequest{}, errCode("invalid_artifact_put")
	}
	return request, nil
}
func (binding ArtifactBinding) Validate() error {
	if !ValidID(binding.DeploymentID) || !ValidRecipeTaskID(binding.TaskID) || !idPattern.MatchString(binding.ExecutionID) || !namedSHA256Pattern.MatchString(binding.RecipeDigest) || !namedSHA256Pattern.MatchString(binding.ArtifactDigest) || !namedSHA256Pattern.MatchString(binding.ManifestDigest) || !archiveSHA256Pattern.MatchString(binding.ArchiveSHA256) || binding.SizeBytes < 1 || binding.SizeBytes > MaxArtifactSizeBytes || binding.MediaType != ArtifactMediaType {
		return errCode("invalid_artifact_put")
	}
	return nil
}
func (binding ArtifactBinding) ChecksumBase64() string {
	raw, _ := hex.DecodeString(binding.ArchiveSHA256)
	return base64.StdEncoding.EncodeToString(raw)
}
func (binding ArtifactBinding) Same(other ArtifactBinding) bool { return binding == other }
func (binding ArtifactBinding) ObjectKey() string {
	return "artifacts/" + binding.DeploymentID + "/" + binding.TaskID + "/" + binding.ArchiveSHA256 + ".tar"
}

func ArtifactReceipt(command Command) (DeploymentCommandReceipt, error) {
	hash, err := command.RequestSHA256()
	if err != nil {
		return DeploymentCommandReceipt{}, err
	}
	return DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: hash, Action: ActionArtifactPut}, nil
}

func MarshalArtifactPrepareResult(receipt DeploymentCommandReceipt, artifact ArtifactState, upload ArtifactUpload) ([]byte, error) {
	return json.Marshal(ArtifactPutPrepareResult{Schema: ArtifactPutPrepareResultSchema, Status: "uploading", Receipt: receipt, Artifact: artifact, Upload: upload})
}
func MarshalArtifactCompleteResult(receipt DeploymentCommandReceipt, artifact ArtifactState) ([]byte, error) {
	return json.Marshal(ArtifactPutCompleteResult{Schema: ArtifactPutCompleteResultSchema, Status: "verified", Receipt: receipt, Artifact: artifact})
}
