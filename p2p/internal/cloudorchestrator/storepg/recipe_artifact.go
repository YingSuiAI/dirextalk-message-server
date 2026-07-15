package storepg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sort"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const trustedRecipeArtifactStatusVerified = "verified"

var _ cloudmodule.TrustedRecipeArtifactStore = (*Store)(nil)

// RegisterTrustedCloudRecipeArtifact is the Orchestrator-only compiler ingress.
// It revalidates the current Recipe row and its canonical revision in the same
// transaction that makes the compiled artifact executable.
func (s *Store) RegisterTrustedCloudRecipeArtifact(ctx context.Context, request cloudmodule.RegisterTrustedRecipeArtifactRequest) (cloudmodule.RegisterTrustedRecipeArtifactResult, error) {
	if s == nil || s.db == nil || request.RegisteredAt <= 0 || request.Artifact.Validate() != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	raw, err := json.Marshal(request.Artifact)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}
	artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1(raw)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	canonical, err := artifact.CanonicalCompiledRecipeArtifactCBOR()
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	descriptorDigest, err := artifact.Digest()
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	descriptorJSON, err := json.Marshal(artifact)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}
	defer tx.Rollback() //nolint:errcheck // commit below owns the successful path
	recipe, err := lockExactCurrentCloudRecipe(ctx, tx, artifact.RecipeID, artifact.RecipeRevision, artifact.RecipeDigest)
	if err != nil || validateCompiledArtifactAgainstRecipe(artifact, recipe) != nil {
		if err != nil && !errors.Is(err, cloudmodule.ErrRecipeArtifactInvalid) {
			return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
		}
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	stored, found, err := lockTrustedRecipeArtifact(ctx, tx, artifact.ArtifactDigest)
	if err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}
	if found {
		if stored.DescriptorDigest != descriptorDigest {
			return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactConflict
		}
		if err := tx.Commit(); err != nil {
			return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
		}
		return cloudmodule.RegisterTrustedRecipeArtifactResult{Artifact: stored}, nil
	}
	stored = cloudmodule.TrustedRecipeArtifact{
		ArtifactDigest: artifact.ArtifactDigest, DescriptorDigest: descriptorDigest, RecipeID: artifact.RecipeID,
		RecipeDigest: artifact.RecipeDigest, RecipeRevision: artifact.RecipeRevision,
		WorkerResourceManifestDigest: artifact.WorkerResourceManifestDigest, Status: trustedRecipeArtifactStatusVerified,
		Revision: 1, CreatedAt: request.RegisteredAt,
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_artifacts (
			artifact_digest, descriptor_digest, recipe_id, recipe_revision, recipe_digest,
			worker_resource_manifest_digest, canonical_cbor, descriptor_json, status, revision, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'verified',1,$9,$9)
	`, artifact.ArtifactDigest, descriptorDigest, artifact.RecipeID, artifact.RecipeRevision, artifact.RecipeDigest,
		artifact.WorkerResourceManifestDigest, canonical, string(descriptorJSON), request.RegisteredAt); err != nil {
		if sqlutil.IsUniqueConstraintViolationErr(err) {
			return cloudmodule.RegisterTrustedRecipeArtifactResult{}, cloudmodule.ErrRecipeArtifactConflict
		}
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return cloudmodule.RegisterTrustedRecipeArtifactResult{}, err
	}
	return cloudmodule.RegisterTrustedRecipeArtifactResult{Artifact: stored, Created: true}, nil
}

func lockTrustedRecipeArtifact(ctx context.Context, tx *sql.Tx, artifactDigest string) (cloudmodule.TrustedRecipeArtifact, bool, error) {
	var value cloudmodule.TrustedRecipeArtifact
	err := tx.QueryRowContext(ctx, `
		SELECT artifact_digest, descriptor_digest, recipe_id, recipe_digest, recipe_revision,
			worker_resource_manifest_digest, status, revision, created_at
		FROM p2p_cloud_recipe_artifacts WHERE artifact_digest=$1 FOR UPDATE
	`, artifactDigest).Scan(&value.ArtifactDigest, &value.DescriptorDigest, &value.RecipeID, &value.RecipeDigest,
		&value.RecipeRevision, &value.WorkerResourceManifestDigest, &value.Status, &value.Revision, &value.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.TrustedRecipeArtifact{}, false, nil
	}
	return value, err == nil, err
}

func lockExactCurrentCloudRecipe(ctx context.Context, tx *sql.Tx, recipeID string, revision uint64, digest string) (cloudcontracts.RecipeV1, error) {
	var storedID, storedDigest, displayJSON string
	var storedCanonical []byte
	var storedRevision uint64
	err := tx.QueryRowContext(ctx, `
		SELECT recipe.recipe_id, recipe.revision, recipe.digest, version.canonical_cbor, version.display_json
		FROM p2p_cloud_recipes recipe
		JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision
		WHERE recipe.recipe_id=$1 AND recipe.revision=$2 AND recipe.digest=$3
		FOR UPDATE OF recipe, version
	`, recipeID, revision, digest).Scan(&storedID, &storedRevision, &storedDigest, &storedCanonical, &displayJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudcontracts.RecipeV1{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	if err != nil {
		return cloudcontracts.RecipeV1{}, err
	}
	var recipe cloudcontracts.RecipeV1
	decoder := json.NewDecoder(bytes.NewBufferString(displayJSON))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&recipe) != nil || decoder.Decode(&struct{}{}) != io.EOF || recipe.Validate() != nil || recipe.RecipeID != storedID {
		return cloudcontracts.RecipeV1{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	computed, digestErr := recipe.Digest()
	canonical, canonicalErr := recipe.CanonicalRecipeCBOR()
	if digestErr != nil || canonicalErr != nil || computed != storedDigest || storedRevision != revision || !bytes.Equal(canonical, storedCanonical) {
		return cloudcontracts.RecipeV1{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	return recipe, nil
}

func validateCompiledArtifactAgainstRecipe(artifact cloudcontracts.CompiledRecipeArtifactV1, recipe cloudcontracts.RecipeV1) error {
	officialSet := make(map[string]struct{}, len(recipe.Sources))
	for _, source := range recipe.Sources {
		if source.Official {
			officialSet[source.ArtifactDigest] = struct{}{}
		}
	}
	official := make([]string, 0, len(officialSet))
	for digest := range officialSet {
		official = append(official, digest)
	}
	sort.Strings(official)
	wantHealth, healthErr := cloudcontracts.HealthContractDigestV1(recipe.Health)
	wantLifecycle, lifecycleErr := cloudcontracts.LifecycleContractDigestV1(recipe.Lifecycle)
	if healthErr != nil || lifecycleErr != nil || !reflect.DeepEqual(official, artifact.OfficialSourceArtifactDigests) ||
		artifact.Architecture != recipe.Requirements.Architecture || artifact.Requirements != recipe.Requirements ||
		artifact.HealthContractDigest != wantHealth || artifact.LifecycleContractDigest != wantLifecycle ||
		!recipeSlotRequirementsEqual(recipe, artifact) {
		return cloudmodule.ErrRecipeArtifactInvalid
	}
	return nil
}

func recipeSlotRequirementsEqual(recipe cloudcontracts.RecipeV1, artifact cloudcontracts.CompiledRecipeArtifactV1) bool {
	volume := make(map[string]cloudcontracts.RecipeVolumeSlotRequirementV1, len(recipe.VolumeSlots))
	for _, slot := range recipe.VolumeSlots {
		volume[slot.SlotID] = slot
	}
	if len(volume) != len(artifact.VolumeSlots) {
		return false
	}
	for _, slot := range artifact.VolumeSlots {
		if current, ok := volume[slot.SlotID]; !ok || current != slot {
			return false
		}
	}
	data := make(map[string]cloudcontracts.RecipeDataSlotRequirementV1, len(recipe.DataSlots))
	for _, slot := range recipe.DataSlots {
		data[slot.SlotID] = slot
	}
	if len(data) != len(artifact.DataSlots) {
		return false
	}
	for _, slot := range artifact.DataSlots {
		if current, ok := data[slot.SlotID]; !ok || current != slot {
			return false
		}
	}
	secrets := make(map[string]cloudcontracts.RecipeSecretSlotRequirementV1, len(recipe.SecretSlots))
	for _, slot := range recipe.SecretSlots {
		secrets[slot.SlotID] = slot
	}
	if len(secrets) != len(artifact.SecretSlots) {
		return false
	}
	for _, slot := range artifact.SecretSlots {
		if current, ok := secrets[slot.SlotID]; !ok || current != slot {
			return false
		}
	}
	return true
}
