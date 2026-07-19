package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

var _ cloudmodule.SelectableRecipeStore = (*DatabaseStore)(nil)

func (s *DatabaseStore) ResolveCloudRecipeSelection(ctx context.Context, owner, connectionID, recipeID string, revision int64) (cloudmodule.SelectedRecipeBinding, bool, error) {
	return resolveCloudRecipeSelection(ctx, s.db, owner, connectionID, recipeID, revision, false, false)
}

type recipeSelectionQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func resolveCloudRecipeSelection(ctx context.Context, queryer recipeSelectionQueryer, owner, connectionID, recipeID string, revision int64, lock, currentOnly bool) (cloudmodule.SelectedRecipeBinding, bool, error) {
	lockSQL := ""
	if lock {
		lockSQL = " FOR UPDATE OF recipe,version"
	}
	currentSQL := ""
	if currentOnly {
		currentSQL = " AND recipe.revision=version.revision AND recipe.digest=version.digest"
	}
	var storedID, digest, displayJSON string
	var storedRevision int64
	var canonical []byte
	err := queryer.QueryRowContext(ctx, `
		SELECT recipe.recipe_id,version.revision,version.digest,version.canonical_cbor,version.display_json
		FROM p2p_cloud_recipes recipe
		JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=$2
		WHERE recipe.recipe_id=$1`+currentSQL+` AND EXISTS(
			SELECT 1 FROM p2p_cloud_plans plan
			JOIN p2p_cloud_goals goal ON goal.goal_id=plan.goal_id
			WHERE goal.owner_mxid=$3 AND plan.cloud_connection_id=$4 AND plan.recipe_digest=version.digest
		)`+lockSQL, recipeID, revision, owner, connectionID).Scan(&storedID, &storedRevision, &digest, &canonical, &displayJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return cloudmodule.SelectedRecipeBinding{}, false, nil
	}
	if err != nil {
		return cloudmodule.SelectedRecipeBinding{}, false, err
	}
	var recipe cloudcontracts.RecipeV1
	if decodeCloudContractJSON(displayJSON, &recipe) != nil || recipe.Validate() != nil || recipe.RecipeID != storedID {
		return cloudmodule.SelectedRecipeBinding{}, false, errors.New("stored selected cloud recipe is invalid")
	}
	computedDigest, digestErr := recipe.Digest()
	computedCanonical, canonicalErr := recipe.CanonicalRecipeCBOR()
	if digestErr != nil || canonicalErr != nil || computedDigest != digest || !bytes.Equal(computedCanonical, canonical) {
		return cloudmodule.SelectedRecipeBinding{}, false, errors.New("stored selected cloud recipe is invalid")
	}
	return cloudmodule.SelectedRecipeBinding{RecipeID: storedID, Revision: storedRevision, Digest: digest, Recipe: recipe}, true, nil
}

func validateSelectedRecipeBindingForGoal(ctx context.Context, tx *sql.Tx, request *cloudmodule.CreateGoalRequest) error {
	goal, plan, selected := request.Goal, request.Plan, request.SelectedRecipe
	if selected == nil {
		if goal.SelectedRecipeID != "" || goal.SelectedRecipeRevision != 0 || goal.SelectedRecipeDigest != "" || plan.RecipeID != "" || plan.RecipeRevision != 0 || plan.RecipeDigest != "" {
			return cloudmodule.ErrSelectedRecipeConflict
		}
		return nil
	}
	current, found, err := resolveCloudRecipeSelection(ctx, tx, goal.OwnerMXID, goal.ConnectionID, selected.RecipeID, selected.Revision, true, true)
	if err != nil {
		return err
	}
	if !found || current.RecipeID != selected.RecipeID || current.Revision != selected.Revision || current.Digest != selected.Digest {
		return cloudmodule.ErrSelectedRecipeConflict
	}
	wantCanonical, wantErr := selected.Recipe.CanonicalRecipeCBOR()
	gotCanonical, gotErr := current.Recipe.CanonicalRecipeCBOR()
	if wantErr != nil || gotErr != nil || !bytes.Equal(wantCanonical, gotCanonical) ||
		goal.SelectedRecipeID != current.RecipeID || goal.SelectedRecipeRevision != current.Revision || goal.SelectedRecipeDigest != current.Digest ||
		plan.RecipeID != current.RecipeID || plan.RecipeRevision != current.Revision || plan.RecipeDigest != current.Digest || plan.ConnectionID != goal.ConnectionID {
		return cloudmodule.ErrSelectedRecipeConflict
	}
	request.SelectedRecipe = &current
	return nil
}
