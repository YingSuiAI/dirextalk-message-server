package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"time"
)

const maxRecipeExecutionApprovalLifetime = 5 * time.Minute

var ErrRecipeExecutionApprovalBinding = errors.New("recipe execution approval does not match the current sealed scope")

// NewRecipeExecutionApprovalV1 derives an unsigned, short-lived challenge for
// one already approved plan and one trusted, sealed execution manifest. It
// deliberately accepts a deployment target only as an internal typed value;
// no public API may submit a manifest or artifact digest to this constructor.
func NewRecipeExecutionApprovalV1(
	plan PlanV1,
	manifest RecipeExecutionManifestV1,
	target RecipeExecutionTargetV1,
	approvalID, challengeID, signerKeyID string,
	issuedAt, expiresAt time.Time,
) (RecipeExecutionApprovalV1, error) {
	if err := plan.Validate(); err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("invalid plan: %w", err)
	}
	if plan.Status != PlanApproved {
		return RecipeExecutionApprovalV1{}, errors.New("recipe execution approval requires an approved plan")
	}
	if err := manifest.ValidateForPlan(plan); err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("invalid recipe execution manifest: %w", err)
	}
	if err := target.Validate(); err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("invalid recipe execution target: %w", err)
	}
	if target.DeploymentID != manifest.DeploymentID {
		return RecipeExecutionApprovalV1{}, errors.New("recipe execution target does not match the manifest deployment")
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("digest recipe execution manifest: %w", err)
	}
	planHash, err := plan.Hash()
	if err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("hash plan: %w", err)
	}
	normalizedPlan := normalizePlan(plan)
	normalizedManifest := normalizeRecipeExecutionManifest(manifest)
	approval := RecipeExecutionApprovalV1{
		SchemaVersion:                 SchemaVersionV1,
		Intent:                        RecipeExecutionApprovalIntentStart,
		ApprovalID:                    approvalID,
		ChallengeID:                   challengeID,
		SignerKeyID:                   signerKeyID,
		PlanID:                        normalizedPlan.PlanID,
		PlanHash:                      planHash,
		PlanRevision:                  normalizedPlan.Revision,
		CloudConnectionID:             normalizedPlan.CloudConnectionID,
		RecipeDigest:                  normalizedPlan.Recipe.Digest,
		ResourceScope:                 normalizedPlan.ResourceScope,
		NetworkScope:                  normalizedPlan.NetworkScope,
		SecretScope:                   normalizedPlan.SecretScope,
		IntegrationScope:              normalizedPlan.IntegrationScope,
		DeploymentID:                  target.DeploymentID,
		DeploymentRevision:            target.DeploymentRevision,
		RecipeExecutionManifestDigest: manifestDigest,
		WorkerResourceManifestDigest:  normalizedManifest.WorkerResourceManifestDigest,
		ArtifactDigest:                normalizedManifest.ArtifactDigest,
		ActionID:                      normalizedManifest.ActionID,
		RootRequired:                  normalizedManifest.RootRequired,
		TimeoutSeconds:                normalizedManifest.TimeoutSeconds,
		CheckpointSequence:            normalizedManifest.CheckpointSequence,
		VolumeSlots:                   normalizedManifest.VolumeSlots,
		DataSlots:                     normalizedManifest.DataSlots,
		SecretSlots:                   normalizedManifest.SecretSlots,
		IssuedAt:                      issuedAt.UTC(),
		ExpiresAt:                     expiresAt.UTC(),
	}
	if err := approval.Validate(); err != nil {
		return RecipeExecutionApprovalV1{}, fmt.Errorf("invalid recipe execution approval challenge: %w", err)
	}
	return approval, nil
}

// SigningPayload returns the deterministic-CBOR payload signed by the
// device. It includes every manifest and deployment field that could change
// execution scope, while excluding the signature itself.
func (a RecipeExecutionApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	normalized := normalizeRecipeExecutionApproval(a)
	payload := recipeExecutionApprovalSigningPayloadV1{
		SchemaVersion:                 normalized.SchemaVersion,
		PayloadVersion:                "recipe-execution-approval-signing-payload/v1",
		HashAlgorithm:                 HashAlgorithmDeterministicCBORSHA256,
		Intent:                        normalized.Intent,
		ApprovalID:                    normalized.ApprovalID,
		ChallengeID:                   normalized.ChallengeID,
		SignerKeyID:                   normalized.SignerKeyID,
		PlanID:                        normalized.PlanID,
		PlanHash:                      normalized.PlanHash,
		PlanRevision:                  normalized.PlanRevision,
		CloudConnectionID:             normalized.CloudConnectionID,
		RecipeDigest:                  normalized.RecipeDigest,
		ResourceScope:                 normalized.ResourceScope,
		NetworkScope:                  normalized.NetworkScope,
		SecretScope:                   normalized.SecretScope,
		IntegrationScope:              normalized.IntegrationScope,
		DeploymentID:                  normalized.DeploymentID,
		DeploymentRevision:            normalized.DeploymentRevision,
		RecipeExecutionManifestDigest: normalized.RecipeExecutionManifestDigest,
		WorkerResourceManifestDigest:  normalized.WorkerResourceManifestDigest,
		ArtifactDigest:                normalized.ArtifactDigest,
		ActionID:                      normalized.ActionID,
		RootRequired:                  normalized.RootRequired,
		TimeoutSeconds:                normalized.TimeoutSeconds,
		CheckpointSequence:            normalized.CheckpointSequence,
		VolumeSlots:                   normalized.VolumeSlots,
		DataSlots:                     normalized.DataSlots,
		SecretSlots:                   normalized.SecretSlots,
		IssuedAt:                      normalized.IssuedAt,
		ExpiresAt:                     normalized.ExpiresAt,
	}
	return canonicalCBOR(payload)
}

// Sign returns a copy signed by the caller-held device key. The private key
// never enters the orchestrator store, the Worker, or the Broker.
func (a RecipeExecutionApprovalV1) Sign(privateKey ed25519.PrivateKey, now time.Time) (RecipeExecutionApprovalV1, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return RecipeExecutionApprovalV1{}, errors.New("recipe execution approval signing key is not an Ed25519 private key")
	}
	if err := a.validateNotExpired(now); err != nil {
		return RecipeExecutionApprovalV1{}, err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return RecipeExecutionApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return a, nil
}

// Verify validates a signed, non-expired approval. Call ValidateAgainst with
// the currently locked Plan, execution manifest, and deployment target before
// any persistent intent or Broker command is accepted.
func (a RecipeExecutionApprovalV1) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("recipe execution approval verification key is not an Ed25519 public key")
	}
	if err := a.ValidateAt(now); err != nil {
		return err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("recipe execution approval signature is invalid")
	}
	return nil
}

// ValidateAgainst proves that this exact challenge still matches the current
// approved Plan, sealed execution manifest, and target deployment revision.
// Signature ownership is intentionally verified separately against the active
// device-key registry by the persistence boundary.
func (a RecipeExecutionApprovalV1) ValidateAgainst(plan PlanV1, manifest RecipeExecutionManifestV1, target RecipeExecutionTargetV1, now time.Time) error {
	if err := a.validateNotExpired(now); err != nil {
		return err
	}
	if err := plan.Validate(); err != nil || plan.Status != PlanApproved {
		return ErrRecipeExecutionApprovalBinding
	}
	if err := manifest.ValidateForPlan(plan); err != nil {
		return ErrRecipeExecutionApprovalBinding
	}
	if err := target.Validate(); err != nil {
		return ErrRecipeExecutionApprovalBinding
	}
	if target.DeploymentID != manifest.DeploymentID {
		return ErrRecipeExecutionApprovalBinding
	}
	planHash, err := plan.Hash()
	if err != nil {
		return ErrRecipeExecutionApprovalBinding
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return ErrRecipeExecutionApprovalBinding
	}
	normalizedPlan := normalizePlan(plan)
	normalizedManifest := normalizeRecipeExecutionManifest(manifest)
	normalizedApproval := normalizeRecipeExecutionApproval(a)
	if normalizedApproval.PlanID != normalizedPlan.PlanID || normalizedApproval.PlanHash != planHash ||
		normalizedApproval.PlanRevision != normalizedPlan.Revision || normalizedApproval.CloudConnectionID != normalizedPlan.CloudConnectionID ||
		normalizedApproval.RecipeDigest != normalizedPlan.Recipe.Digest || normalizedApproval.DeploymentID != target.DeploymentID ||
		normalizedApproval.DeploymentRevision != target.DeploymentRevision ||
		normalizedApproval.RecipeExecutionManifestDigest != manifestDigest ||
		normalizedApproval.WorkerResourceManifestDigest != normalizedManifest.WorkerResourceManifestDigest ||
		normalizedApproval.ArtifactDigest != normalizedManifest.ArtifactDigest || normalizedApproval.ActionID != normalizedManifest.ActionID ||
		normalizedApproval.RootRequired != normalizedManifest.RootRequired || normalizedApproval.TimeoutSeconds != normalizedManifest.TimeoutSeconds {
		return ErrRecipeExecutionApprovalBinding
	}
	if !reflect.DeepEqual(normalizedApproval.ResourceScope, normalizedPlan.ResourceScope) ||
		!reflect.DeepEqual(normalizedApproval.NetworkScope, normalizedPlan.NetworkScope) ||
		!reflect.DeepEqual(normalizedApproval.SecretScope, normalizedPlan.SecretScope) ||
		!reflect.DeepEqual(normalizedApproval.IntegrationScope, normalizedPlan.IntegrationScope) ||
		!reflect.DeepEqual(normalizedApproval.CheckpointSequence, normalizedManifest.CheckpointSequence) ||
		!reflect.DeepEqual(normalizedApproval.VolumeSlots, normalizedManifest.VolumeSlots) ||
		!reflect.DeepEqual(normalizedApproval.DataSlots, normalizedManifest.DataSlots) ||
		!reflect.DeepEqual(normalizedApproval.SecretSlots, normalizedManifest.SecretSlots) {
		return ErrRecipeExecutionApprovalBinding
	}
	return nil
}

func (a RecipeExecutionApprovalV1) Validate() error {
	return a.validate(false)
}

func (a RecipeExecutionApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("recipe execution approval has expired")
	}
	return nil
}

func (a RecipeExecutionApprovalV1) validateNotExpired(now time.Time) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("recipe execution approval has expired")
	}
	return nil
}

func (a RecipeExecutionApprovalV1) validate(requireSignature bool) error {
	if err := validateSchema(a.SchemaVersion); err != nil {
		return err
	}
	if a.Intent != RecipeExecutionApprovalIntentStart {
		return errors.New("recipe execution approval intent is invalid")
	}
	for label, value := range map[string]string{
		"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID,
		"plan_id": a.PlanID, "cloud_connection_id": a.CloudConnectionID, "deployment_id": a.DeploymentID,
		"action_id": a.ActionID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	for label, value := range map[string]string{
		"plan_hash": a.PlanHash, "recipe_digest": a.RecipeDigest,
		"recipe_execution_manifest_digest": a.RecipeExecutionManifestDigest,
		"worker_resource_manifest_digest":  a.WorkerResourceManifestDigest, "artifact_digest": a.ArtifactDigest,
	} {
		if err := validateDigest(label, value); err != nil {
			return err
		}
	}
	if a.PlanRevision == 0 || a.DeploymentRevision == 0 {
		return errors.New("recipe execution approval revisions must be positive")
	}
	if a.IssuedAt.IsZero() || a.ExpiresAt.IsZero() || !a.ExpiresAt.After(a.IssuedAt) || a.ExpiresAt.Sub(a.IssuedAt) > maxRecipeExecutionApprovalLifetime {
		return errors.New("recipe execution approval expiry must be within five minutes of issuance")
	}
	if a.TimeoutSeconds == 0 || a.TimeoutSeconds > 24*60*60 {
		return errors.New("recipe execution approval timeout_seconds must be between 1 and 86400")
	}
	if err := a.ResourceScope.validate(); err != nil {
		return fmt.Errorf("resource scope: %w", err)
	}
	if err := a.NetworkScope.validate(); err != nil {
		return fmt.Errorf("network scope: %w", err)
	}
	if err := validateSecretScope(a.SecretScope); err != nil {
		return err
	}
	if err := validateIntegrationScope(a.IntegrationScope); err != nil {
		return err
	}
	if err := validateCheckpointSequence(a.CheckpointSequence); err != nil {
		return err
	}
	if err := validateVolumeSlots(a.VolumeSlots); err != nil {
		return err
	}
	if err := validateDataSlots(a.DataSlots); err != nil {
		return err
	}
	if err := validateSecretSlots(a.SecretSlots); err != nil {
		return err
	}
	if requireSignature || a.Signature != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(decoded) != ed25519.SignatureSize {
			return errors.New("recipe execution approval signature must be a base64url Ed25519 signature")
		}
	}
	return nil
}

func (target RecipeExecutionTargetV1) Validate() error {
	if err := validateIdentifier("deployment_id", target.DeploymentID); err != nil {
		return err
	}
	if target.DeploymentRevision == 0 {
		return errors.New("deployment_revision must be positive")
	}
	return nil
}

type recipeExecutionApprovalSigningPayloadV1 struct {
	SchemaVersion                 string               `json:"schema_version"`
	PayloadVersion                string               `json:"payload_version"`
	HashAlgorithm                 string               `json:"hash_algorithm"`
	Intent                        string               `json:"intent"`
	ApprovalID                    string               `json:"approval_id"`
	ChallengeID                   string               `json:"challenge_id"`
	SignerKeyID                   string               `json:"signer_key_id"`
	PlanID                        string               `json:"plan_id"`
	PlanHash                      string               `json:"plan_hash"`
	PlanRevision                  uint64               `json:"plan_revision"`
	CloudConnectionID             string               `json:"cloud_connection_id"`
	RecipeDigest                  string               `json:"recipe_digest"`
	ResourceScope                 ResourceScopeV1      `json:"resource_scope"`
	NetworkScope                  NetworkScopeV1       `json:"network_scope"`
	SecretScope                   []SecretReferenceV1  `json:"secret_scope"`
	IntegrationScope              []IntegrationScopeV1 `json:"integration_scope"`
	DeploymentID                  string               `json:"deployment_id"`
	DeploymentRevision            uint64               `json:"deployment_revision"`
	RecipeExecutionManifestDigest string               `json:"recipe_execution_manifest_digest"`
	WorkerResourceManifestDigest  string               `json:"worker_resource_manifest_digest"`
	ArtifactDigest                string               `json:"artifact_digest"`
	ActionID                      string               `json:"action_id"`
	RootRequired                  bool                 `json:"root_required"`
	TimeoutSeconds                uint32               `json:"timeout_seconds"`
	CheckpointSequence            []string             `json:"checkpoint_sequence"`
	VolumeSlots                   []VolumeSlotV1       `json:"volume_slots"`
	DataSlots                     []DataSlotV1         `json:"data_slots"`
	SecretSlots                   []SecretSlotV1       `json:"secret_slots"`
	IssuedAt                      time.Time            `json:"issued_at"`
	ExpiresAt                     time.Time            `json:"expires_at"`
}

func normalizeRecipeExecutionApproval(approval RecipeExecutionApprovalV1) RecipeExecutionApprovalV1 {
	normalized := approval
	normalized.IssuedAt = approval.IssuedAt.UTC()
	normalized.ExpiresAt = approval.ExpiresAt.UTC()
	scopes := normalizePlan(PlanV1{
		ResourceScope:    approval.ResourceScope,
		NetworkScope:     approval.NetworkScope,
		SecretScope:      approval.SecretScope,
		IntegrationScope: approval.IntegrationScope,
	})
	normalized.ResourceScope = scopes.ResourceScope
	normalized.NetworkScope = scopes.NetworkScope
	normalized.SecretScope = scopes.SecretScope
	normalized.IntegrationScope = scopes.IntegrationScope
	manifest := normalizeRecipeExecutionManifest(RecipeExecutionManifestV1{
		CheckpointSequence: approval.CheckpointSequence,
		VolumeSlots:        approval.VolumeSlots,
		DataSlots:          approval.DataSlots,
		SecretSlots:        approval.SecretSlots,
	})
	normalized.CheckpointSequence = manifest.CheckpointSequence
	normalized.VolumeSlots = manifest.VolumeSlots
	normalized.DataSlots = manifest.DataSlots
	normalized.SecretSlots = manifest.SecretSlots
	return normalized
}
