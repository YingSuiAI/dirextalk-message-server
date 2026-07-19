package storage

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sort"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const trustedRecipeArtifactStatusVerified = "verified"

var _ cloudmodule.TrustedRecipeArtifactStore = (*DatabaseStore)(nil)

func (s *DatabaseStore) RegisterTrustedCloudRecipeArtifact(ctx context.Context, request cloudmodule.RegisterTrustedRecipeArtifactRequest) (cloudmodule.RegisterTrustedRecipeArtifactResult, error) {
	if request.RegisteredAt <= 0 || request.Artifact.Validate() != nil {
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

	result := cloudmodule.RegisterTrustedRecipeArtifactResult{}
	err = s.writer.Do(s.db, nil, func(tx *sql.Tx) error {
		stored, found, err := lockTrustedRecipeArtifact(ctx, tx, artifact.ArtifactDigest)
		if err != nil {
			return err
		}
		if found {
			if stored.DescriptorDigest != descriptorDigest {
				return cloudmodule.ErrRecipeArtifactConflict
			}
			result.Artifact = stored
			return nil
		}
		recipe, err := lockExactCurrentCloudRecipe(ctx, tx, artifact.RecipeID, artifact.RecipeRevision, artifact.RecipeDigest)
		if err != nil || validateCompiledArtifactAgainstRecipe(artifact, recipe) != nil {
			if err != nil && !errors.Is(err, cloudmodule.ErrRecipeArtifactInvalid) {
				return err
			}
			return cloudmodule.ErrRecipeArtifactInvalid
		}
		stored = cloudmodule.TrustedRecipeArtifact{
			ArtifactDigest: artifact.ArtifactDigest, DescriptorDigest: descriptorDigest, RecipeID: artifact.RecipeID,
			RecipeDigest: artifact.RecipeDigest, RecipeRevision: artifact.RecipeRevision,
			WorkerResourceManifestDigest: artifact.WorkerResourceManifestDigest, Status: trustedRecipeArtifactStatusVerified,
			Revision: 1, CreatedAt: request.RegisteredAt,
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO p2p_cloud_recipe_artifacts (
				artifact_digest, descriptor_digest, recipe_id, recipe_revision, recipe_digest,
				worker_resource_manifest_digest, canonical_cbor, descriptor_json, status, revision, created_at, updated_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'verified',1,$9,$9)
		`, artifact.ArtifactDigest, descriptorDigest, artifact.RecipeID, artifact.RecipeRevision, artifact.RecipeDigest,
			artifact.WorkerResourceManifestDigest, canonical, string(descriptorJSON), request.RegisteredAt); err != nil {
			if sqlutil.IsUniqueConstraintViolationErr(err) {
				return cloudmodule.ErrRecipeArtifactConflict
			}
			return err
		}
		result = cloudmodule.RegisterTrustedRecipeArtifactResult{Artifact: stored, Created: true}
		return nil
	})
	return result, err
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
	if decodeCloudContractJSON(displayJSON, &recipe) != nil || recipe.Validate() != nil || recipe.RecipeID != storedID {
		return cloudcontracts.RecipeV1{}, cloudmodule.ErrRecipeArtifactInvalid
	}
	computed, err := recipe.Digest()
	canonical, canonicalErr := recipe.CanonicalRecipeCBOR()
	if err != nil || canonicalErr != nil || computed != storedDigest || storedRevision != revision || !bytes.Equal(canonical, storedCanonical) {
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

func lockVerifiedRecipeArtifactForExecution(ctx context.Context, tx *sql.Tx, manifest cloudcontracts.RecipeExecutionManifestV1, plan cloudcontracts.PlanV1) (cloudcontracts.CompiledRecipeArtifactV1, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `
		SELECT descriptor_json FROM p2p_cloud_recipe_artifacts
		WHERE artifact_digest=$1 AND recipe_digest=$2 AND worker_resource_manifest_digest=$3 AND status='verified'
		FOR UPDATE
	`, manifest.ArtifactDigest, manifest.RecipeDigest, manifest.WorkerResourceManifestDigest).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudcontracts.CompiledRecipeArtifactV1{}, cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	if err != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, err
	}
	artifact, err := cloudcontracts.ParseCompiledRecipeArtifactV1([]byte(raw))
	if err != nil || validateExecutionManifestAgainstArtifact(manifest, artifact) != nil || validateExecutionManifestSecretScope(manifest, artifact, plan) != nil {
		return cloudcontracts.CompiledRecipeArtifactV1{}, cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	return artifact, nil
}

func validateExecutionManifestSecretScope(manifest cloudcontracts.RecipeExecutionManifestV1, artifact cloudcontracts.CompiledRecipeArtifactV1, plan cloudcontracts.PlanV1) error {
	if len(manifest.SecretSlots) != len(artifact.SecretSlots) || len(plan.SecretScope) != len(artifact.SecretSlots) {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	manifestBySlot := make(map[string]string, len(manifest.SecretSlots))
	for _, slot := range manifest.SecretSlots {
		manifestBySlot[slot.SlotID] = slot.SecretRef
	}
	planByRef := make(map[string]cloudcontracts.SecretReferenceV1, len(plan.SecretScope))
	for _, reference := range plan.SecretScope {
		planByRef[reference.SecretRef] = reference
	}
	for _, requirement := range artifact.SecretSlots {
		expected, err := cloudcontracts.SecretReferenceForRecipeSlot(plan.PlanID, requirement)
		if err != nil || manifestBySlot[requirement.SlotID] != expected.SecretRef {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
		if actual, ok := planByRef[expected.SecretRef]; !ok || actual != expected {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
	}
	return nil
}

func validateExecutionManifestAgainstArtifact(manifest cloudcontracts.RecipeExecutionManifestV1, artifact cloudcontracts.CompiledRecipeArtifactV1) error {
	var matched bool
	for _, action := range artifact.Actions {
		if action.ActionID == manifest.ActionID && action.Kind == cloudcontracts.CompiledRecipeActionInstall &&
			action.RootRequired == manifest.RootRequired && action.TimeoutSeconds == manifest.TimeoutSeconds &&
			reflect.DeepEqual(action.CheckpointSequence, manifest.CheckpointSequence) {
			matched = true
		}
	}
	if !matched || manifest.SemanticReadiness != artifact.SemanticReadiness || len(manifest.VolumeSlots) != len(artifact.VolumeSlots) || len(manifest.DataSlots) != len(artifact.DataSlots) || len(manifest.SecretSlots) != len(artifact.SecretSlots) {
		return cloudmodule.ErrRecipeExecutionManifestInvalid
	}
	volumes := make(map[string]cloudcontracts.CompiledVolumeSlotSchemaV1, len(artifact.VolumeSlots))
	for _, slot := range artifact.VolumeSlots {
		volumes[slot.SlotID] = slot
	}
	for _, slot := range manifest.VolumeSlots {
		schema, ok := volumes[slot.SlotID]
		if !ok || schema.ReadOnly != slot.ReadOnly {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
	}
	data := make(map[string]cloudcontracts.CompiledDataSlotSchemaV1, len(artifact.DataSlots))
	for _, slot := range artifact.DataSlots {
		data[slot.SlotID] = slot
	}
	for _, slot := range manifest.DataSlots {
		schema, ok := data[slot.SlotID]
		if !ok || schema.ReadOnly != slot.ReadOnly {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
	}
	secrets := make(map[string]struct{}, len(artifact.SecretSlots))
	for _, slot := range artifact.SecretSlots {
		secrets[slot.SlotID] = struct{}{}
	}
	for _, slot := range manifest.SecretSlots {
		if _, ok := secrets[slot.SlotID]; !ok {
			return cloudmodule.ErrRecipeExecutionManifestInvalid
		}
	}
	return nil
}
