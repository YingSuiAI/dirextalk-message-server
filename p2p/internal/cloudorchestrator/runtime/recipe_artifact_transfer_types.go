package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"time"
)

const (
	RecipeArtifactTransferPhasePrepare  = "prepare"
	RecipeArtifactTransferPhaseComplete = "complete"
	RecipeArtifactTransferPhaseVerified = "verified"
	RecipeArtifactPutAction             = "artifact.put"
	RecipeArtifactPutPrepareSchema      = "dirextalk.artifact-put-prepare/v1"
	RecipeArtifactPutCompleteSchema     = "dirextalk.artifact-put-complete/v1"
	RecipeArtifactTarMediaType          = "application/x-tar"
)

var recipeArtifactVersionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/=-]+$`)

type TrustedRecipeArtifactArchive struct {
	Path                         string
	ArchiveSHA256                string
	SizeBytes                    int64
	ControllerCatalogDigest      string
	RecipeDigest                 string
	ArtifactDigest               string
	WorkerResourceManifestDigest string
}

type RecipeArtifactTransferBinding struct {
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

type RecipeArtifactPutPrepareRequest struct {
	Schema string `json:"schema"`
	RecipeArtifactTransferBinding
}

type RecipeArtifactPutCompleteRequest struct {
	Schema string `json:"schema"`
	RecipeArtifactTransferBinding
	VersionID string `json:"version_id"`
}

type RecipeArtifactTransferCommand struct {
	CommandID, ExecutionID, DeploymentID, TaskID, ConnectionID, NodeKeyID string
	ExpectedGeneration, NodeCounter                                       int64
	Phase, Action, RequestDigest, State                                   string
	PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope             string
	IssuedAt, ExpiresAt                                                   time.Time
}

type SignedRecipeArtifactTransferCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}

type RecipeArtifactUploadGrant struct {
	Method    string
	URL       string
	ExpiresAt time.Time
	Headers   map[string]string
}

type RecipeArtifactTransfer struct {
	Phase        string
	ConnectionID string
	Binding      RecipeArtifactTransferBinding
	VersionID    string
	Command      RecipeArtifactTransferCommand
}

type RecipeArtifactTransferStore interface {
	LoadOrCreateRecipeArtifactTransfer(context.Context, RecipeInstallClaim, RecipeArtifactTransferBinding) (RecipeArtifactTransfer, error)
	PersistRecipeArtifactTransferCommand(context.Context, RecipeInstallClaim, RecipeArtifactTransfer, SignedRecipeArtifactTransferCommand) error
	RecordRecipeArtifactVersion(context.Context, RecipeInstallClaim, RecipeArtifactTransfer, string) error
	CommitRecipeArtifactTransfer(context.Context, RecipeInstallClaim, RecipeArtifactTransfer) error
}

type RecipeArtifactTransferTransport interface {
	BuildRecipeArtifactPrepareCommand(RecipeArtifactTransferCommand, RecipeArtifactPutPrepareRequest, time.Time) (SignedRecipeArtifactTransferCommand, error)
	RequestRecipeArtifactPrepare(context.Context, string, RecipeArtifactTransferCommand, SignedRecipeArtifactTransferCommand, RecipeArtifactPutPrepareRequest) (RecipeArtifactUploadGrant, error)
	BuildRecipeArtifactCompleteCommand(RecipeArtifactTransferCommand, RecipeArtifactPutCompleteRequest, time.Time) (SignedRecipeArtifactTransferCommand, error)
	RequestRecipeArtifactComplete(context.Context, string, RecipeArtifactTransferCommand, SignedRecipeArtifactTransferCommand, RecipeArtifactPutCompleteRequest) error
}

type RecipeArtifactUploader interface {
	Upload(context.Context, TrustedRecipeArtifactArchive, RecipeArtifactUploadGrant) (string, error)
}

type RecipeArtifactEnsurer interface {
	Ensure(context.Context, RecipeInstallClaim) error
}

type recipeArtifactCommandExpiredError struct{ cause error }

func (errorValue recipeArtifactCommandExpiredError) Error() string {
	return "recipe_artifact_command_expired"
}
func (errorValue recipeArtifactCommandExpiredError) Unwrap() error { return errorValue.cause }

func RecipeArtifactCommandExpired(cause error) error {
	return recipeArtifactCommandExpiredError{cause: cause}
}

func recipeArtifactCommandExpired(err error) bool {
	var target recipeArtifactCommandExpiredError
	return errors.As(err, &target)
}

func (archive TrustedRecipeArtifactArchive) Validate() error {
	if archive.Path == "" || !lowerHexSHA256(archive.ArchiveSHA256) || archive.SizeBytes < 1 || archive.SizeBytes > 256<<20 ||
		!deploymentDigestPattern.MatchString(archive.ControllerCatalogDigest) || !deploymentDigestPattern.MatchString(archive.RecipeDigest) ||
		!deploymentDigestPattern.MatchString(archive.ArtifactDigest) || !deploymentDigestPattern.MatchString(archive.WorkerResourceManifestDigest) {
		return errors.New("trusted recipe artifact archive is invalid")
	}
	return nil
}

func (binding RecipeArtifactTransferBinding) Validate() error {
	if !validResearchIdentifier("deployment_id", binding.DeploymentID) || !validResearchIdentifier("task_id", binding.TaskID) ||
		!validResearchIdentifier("execution_id", binding.ExecutionID) || !deploymentDigestPattern.MatchString(binding.RecipeDigest) ||
		!deploymentDigestPattern.MatchString(binding.ArtifactDigest) || !deploymentDigestPattern.MatchString(binding.ManifestDigest) ||
		!lowerHexSHA256(binding.ArchiveSHA256) || binding.SizeBytes < 1 || binding.SizeBytes > 256<<20 || binding.MediaType != RecipeArtifactTarMediaType {
		return errors.New("recipe artifact transfer binding is invalid")
	}
	return nil
}

func (request RecipeArtifactPutPrepareRequest) Validate() error {
	if request.Schema != RecipeArtifactPutPrepareSchema {
		return errors.New("recipe artifact prepare request is invalid")
	}
	return request.RecipeArtifactTransferBinding.Validate()
}

func (request RecipeArtifactPutCompleteRequest) Validate() error {
	if request.Schema != RecipeArtifactPutCompleteSchema || !validRecipeArtifactVersionID(request.VersionID) {
		return errors.New("recipe artifact complete request is invalid")
	}
	return request.RecipeArtifactTransferBinding.Validate()
}

func (request RecipeArtifactPutPrepareRequest) Digest() (string, error) {
	return recipeArtifactTransferDigest(request)
}
func (request RecipeArtifactPutCompleteRequest) Digest() (string, error) {
	return recipeArtifactTransferDigest(request)
}

func recipeArtifactTransferDigest(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("dirextalk.cloud.recipe-artifact-transfer-request/v1\x00"), raw...))
	return hex.EncodeToString(sum[:]), nil
}

func validateRecipeArtifactTransferCommand(command RecipeArtifactTransferCommand, signed SignedRecipeArtifactTransferCommand) error {
	if command.CommandID == "" || command.ConnectionID == "" || command.NodeKeyID == "" || command.ExpectedGeneration < 1 || command.NodeCounter < 1 ||
		command.Action != RecipeArtifactPutAction || (command.Phase != RecipeArtifactTransferPhasePrepare && command.Phase != RecipeArtifactTransferPhaseComplete) ||
		signed.EnvelopeJSON == "" || signed.PayloadJSON == "" || !lowerHexSHA256(signed.PayloadSHA256) || !lowerHexSHA256(signed.RequestSHA256) ||
		signed.IssuedAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed recipe artifact transfer command is invalid")
	}
	return nil
}

func validRecipeArtifactVersionID(value string) bool {
	return len(value) > 0 && len(value) <= 1024 && recipeArtifactVersionIDPattern.MatchString(value)
}

func ValidateRecipeArtifactVersionID(value string) error {
	if !validRecipeArtifactVersionID(value) {
		return errors.New("recipe artifact version id is invalid")
	}
	return nil
}
