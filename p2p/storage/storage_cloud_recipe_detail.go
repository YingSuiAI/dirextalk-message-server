package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.RecipeDetailStore = (*DatabaseStore)(nil)

func (s *DatabaseStore) GetCloudRecipeDetail(ctx context.Context, ownerMXID, recipeID string) (cloudmodule.RecipeDetail, bool, error) {
	if strings.TrimSpace(ownerMXID) == "" || strings.TrimSpace(recipeID) == "" {
		return cloudmodule.RecipeDetail{}, false, nil
	}
	var summary cloudmodule.Recipe
	var canonical []byte
	var displayJSON, versionDigest, versionMaturity string
	err := s.db.QueryRowContext(ctx, `
		SELECT recipe.recipe_id,recipe.name,recipe.version,recipe.digest,recipe.maturity,recipe.revision,
			recipe.created_at,recipe.updated_at,version.canonical_cbor,version.display_json,version.digest,version.maturity
		FROM p2p_cloud_recipes recipe
		JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision
		WHERE recipe.recipe_id=$1 AND EXISTS (
			SELECT 1 FROM p2p_cloud_plans plan
			JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
			JOIN p2p_cloud_recipe_versions owned_version ON owned_version.digest=plan.recipe_digest
			WHERE goal.owner_mxid=$2 AND owned_version.recipe_id=recipe.recipe_id
		)
	`, recipeID, ownerMXID).Scan(
		&summary.RecipeID, &summary.Name, &summary.Version, &summary.Digest, &summary.Maturity, &summary.Revision,
		&summary.CreatedAt, &summary.UpdatedAt, &canonical, &displayJSON, &versionDigest, &versionMaturity,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.RecipeDetail{}, false, nil
	}
	if err != nil {
		return cloudmodule.RecipeDetail{}, false, err
	}

	var recipe cloudcontracts.RecipeV1
	if decodeCloudContractJSON(displayJSON, &recipe) != nil || recipe.Validate() != nil ||
		recipe.RecipeID != summary.RecipeID || recipe.Name != summary.Name ||
		versionDigest != summary.Digest || versionMaturity != summary.Maturity {
		return cloudmodule.RecipeDetail{}, false, errors.New("stored cloud recipe detail is invalid")
	}
	digest, digestErr := recipe.Digest()
	computedCanonical, canonicalErr := recipe.CanonicalRecipeCBOR()
	if digestErr != nil || canonicalErr != nil || digest != summary.Digest || !bytes.Equal(computedCanonical, canonical) {
		return cloudmodule.RecipeDetail{}, false, errors.New("stored cloud recipe detail is invalid")
	}

	detail := cloudmodule.RecipeDetail{
		RecipeID: summary.RecipeID, Name: summary.Name, Version: summary.Version,
		Maturity: summary.Maturity, Revision: summary.Revision, Digest: summary.Digest,
		Requirements: cloudRecipeDetailRequirements(recipe.Requirements),
		Health:       cloudRecipeDetailHealth(recipe.Health),
		Lifecycle:    cloudRecipeDetailLifecycle(recipe.Lifecycle),
	}
	for _, slot := range recipe.VolumeSlots {
		detail.VolumeSlots = append(detail.VolumeSlots, cloudmodule.RecipeDetailVolumeSlot{SlotID: slot.SlotID, Purpose: slot.Purpose, ReadOnly: slot.ReadOnly})
	}
	for _, slot := range recipe.DataSlots {
		detail.DataSlots = append(detail.DataSlots, cloudmodule.RecipeDetailDataSlot{SlotID: slot.SlotID, Purpose: slot.Purpose, ReadOnly: slot.ReadOnly})
	}
	for _, slot := range recipe.SecretSlots {
		detail.SecretSlots = append(detail.SecretSlots, cloudmodule.RecipeDetailSecretSlot{SlotID: slot.SlotID, Purpose: slot.Purpose, Delivery: slot.Delivery})
	}
	for _, source := range recipe.Sources {
		if source.Official {
			detail.OfficialSources = append(detail.OfficialSources, cloudmodule.RecipeOfficialSource{
				Version: source.Version, Commit: source.Commit, ArtifactDigest: source.ArtifactDigest,
				License: source.License, RetrievedAt: source.RetrievedAt,
			})
		}
	}
	if detail.OfficialSources == nil {
		detail.OfficialSources = []cloudmodule.RecipeOfficialSource{}
	}
	return detail, true, nil
}

func cloudRecipeDetailRequirements(value cloudcontracts.ResourceRequirementsV1) cloudmodule.RecipeDetailRequirements {
	return cloudmodule.RecipeDetailRequirements{
		MinVCPU: value.MinVCPU, MinMemoryMiB: value.MinMemoryMiB, MinDiskGiB: value.MinDiskGiB,
		MinGPUCount: value.MinGPUCount, MinGPUMemoryMiB: value.MinGPUMemoryMiB, Architecture: value.Architecture,
	}
}

func cloudRecipeDetailHealth(value cloudcontracts.HealthContractV1) cloudmodule.RecipeDetailHealth {
	probe := func(value cloudcontracts.ProbeV1) cloudmodule.RecipeDetailProbe {
		return cloudmodule.RecipeDetailProbe{Kind: value.Kind, Target: value.Target}
	}
	return cloudmodule.RecipeDetailHealth{Liveness: probe(value.Liveness), Readiness: probe(value.Readiness), Semantic: probe(value.Semantic)}
}

func cloudRecipeDetailLifecycle(value cloudcontracts.LifecycleContractV1) cloudmodule.RecipeDetailLifecycle {
	return cloudmodule.RecipeDetailLifecycle{
		Start: value.Start, Stop: value.Stop, Restart: value.Restart, Upgrade: value.Upgrade,
		Rollback: value.Rollback, Backup: value.Backup, Restore: value.Restore, Destroy: value.Destroy,
	}
}
