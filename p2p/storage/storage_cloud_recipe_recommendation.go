package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.RecipeRecommendationStore = (*DatabaseStore)(nil)

func (s *DatabaseStore) ListCloudRecipeRecommendations(ctx context.Context, ownerMXID string) ([]cloudmodule.RecipeRecommendation, error) {
	if strings.TrimSpace(ownerMXID) == "" {
		return nil, errors.New("cloud recipe owner is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT recipe.recipe_id, recipe.name, recipe.version, recipe.digest, recipe.maturity, recipe.revision,
			version.canonical_cbor, version.display_json
		FROM p2p_cloud_recipes recipe
		JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision
		WHERE EXISTS (
			SELECT 1 FROM p2p_cloud_plans plan
			JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
			JOIN p2p_cloud_recipe_versions owned_version ON owned_version.digest=plan.recipe_digest
			WHERE owned_version.recipe_id=recipe.recipe_id AND goal.owner_mxid=$1
		)
		ORDER BY recipe.updated_at DESC, recipe.recipe_id ASC
	`, ownerMXID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []cloudmodule.RecipeRecommendation{}
	for rows.Next() {
		var recipeID, name, version, storedDigest, maturity, displayJSON string
		var revision int64
		var storedCanonical []byte
		if err := rows.Scan(&recipeID, &name, &version, &storedDigest, &maturity, &revision, &storedCanonical, &displayJSON); err != nil {
			return nil, err
		}
		var contract cloudcontracts.RecipeV1
		if err := decodeCloudContractJSON(displayJSON, &contract); err != nil || contract.Validate() != nil || contract.RecipeID != recipeID || contract.Name != name || string(contract.Maturity) != maturity {
			return nil, errors.New("stored cloud recipe recommendation is invalid")
		}
		digest, digestErr := contract.Digest()
		canonical, canonicalErr := contract.CanonicalRecipeCBOR()
		if digestErr != nil || canonicalErr != nil || digest != storedDigest || !bytes.Equal(canonical, storedCanonical) {
			return nil, errors.New("stored cloud recipe recommendation is invalid")
		}
		items = append(items, cloudmodule.RecipeRecommendation{
			RecipeID: recipeID, Name: name, Version: version, Maturity: maturity, Revision: revision,
			Resources: cloudmodule.RecipeResourceSummary{
				MinVCPU: contract.Requirements.MinVCPU, MinMemoryMiB: contract.Requirements.MinMemoryMiB,
				MinGPUMemoryMiB: contract.Requirements.MinGPUMemoryMiB, MinDiskGiB: contract.Requirements.MinDiskGiB,
				Architecture: string(contract.Requirements.Architecture),
			},
		})
	}
	return items, rows.Err()
}
