package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
)

func TestDatabaseStoreCreateGoalBindsCurrentOwnerConnectionRecipe(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, 7, 15, 14, 0, 0, 0, time.UTC)
	recipe, quote := cloudConfirmationFixtures(t, now, "connection-select-recipe-1", "quote-select-recipe-1")
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudConfirmationState(t, store, "@recipe-owner:example.com", quote.CloudConnectionID, "plan-select-source-1", recipe, quote, publicSPKI)
	if _, err := store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) SELECT recipe_id,2,canonical_cbor,display_json,digest,'managed',$2 FROM p2p_cloud_recipe_versions WHERE recipe_id=$1 AND revision=1`, recipe.RecipeID, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipes SET revision=2,version='v2',maturity='managed' WHERE recipe_id=$1`, recipe.RecipeID); err != nil {
		t.Fatal(err)
	}
	binding, found, err := store.ResolveCloudRecipeSelection(ctx, "@recipe-owner:example.com", quote.CloudConnectionID, recipe.RecipeID, 2)
	if err != nil || !found || binding.Digest == "" || binding.Recipe.Maturity == "" {
		t.Fatalf("binding=%#v found=%v err=%v", binding, found, err)
	}
	for _, tc := range []struct {
		owner, connection string
		revision          int64
	}{{"@other:example.com", quote.CloudConnectionID, 2}, {"@recipe-owner:example.com", "connection-other-1", 2}} {
		if _, ok, e := store.ResolveCloudRecipeSelection(ctx, tc.owner, tc.connection, recipe.RecipeID, tc.revision); e != nil || ok {
			t.Fatalf("unauthorized/stale selection found=%v err=%v case=%#v", ok, e, tc)
		}
	}
	request := cloudGoalCreateRequest("goal-selected-recipe-1", "plan-selected-recipe-1", "idem-selected-recipe-1", "digest-selected-recipe-1", "event-selected-recipe-1", "outbox-selected-recipe-1")
	request.Goal.OwnerMXID, request.Goal.ConnectionID = "@recipe-owner:example.com", quote.CloudConnectionID
	request.Plan.ConnectionID = quote.CloudConnectionID
	request.Goal.SelectedRecipeID, request.Goal.SelectedRecipeRevision, request.Goal.SelectedRecipeDigest = binding.RecipeID, binding.Revision, binding.Digest
	request.Plan.RecipeID, request.Plan.RecipeRevision, request.Plan.RecipeDigest = binding.RecipeID, binding.Revision, binding.Digest
	request.SelectedRecipe = &binding
	created, e := store.CreateCloudGoal(ctx, request)
	if e != nil || !created.Created || created.Plan.RecipeRevision != 2 || created.Plan.RecipeDigest != binding.Digest {
		t.Fatalf("created=%#v err=%v", created, e)
	}
	replay, e := store.CreateCloudGoal(ctx, request)
	if e != nil || replay.Created || replay.Plan.RecipeID != binding.RecipeID {
		t.Fatalf("replay=%#v err=%v", replay, e)
	}
	different := request
	different.Goal.RequestDigest = "different-selected-spec"
	if _, e = store.CreateCloudGoal(ctx, different); !errors.Is(e, cloudmodule.ErrIdempotencyConflict) {
		t.Fatalf("different selected spec err=%v", e)
	}
	if _, e = store.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) SELECT recipe_id,3,canonical_cbor,display_json,digest,'managed',$2 FROM p2p_cloud_recipe_versions WHERE recipe_id=$1 AND revision=2`, recipe.RecipeID, now.Add(time.Minute).UnixMilli()); e != nil {
		t.Fatal(e)
	}
	if _, e = store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipes SET revision=3,version='v3' WHERE recipe_id=$1`, recipe.RecipeID); e != nil {
		t.Fatal(e)
	}
	replayAfterAdvance, e := store.CreateCloudGoal(ctx, request)
	if e != nil || replayAfterAdvance.Created || replayAfterAdvance.Plan.RecipeRevision != 2 {
		t.Fatalf("exact replay after recipe advance=%#v err=%v", replayAfterAdvance, e)
	}
	stale := request
	stale.Goal.GoalID = "goal-selected-stale-1"
	stale.Goal.PlanID = "plan-selected-stale-1"
	stale.Goal.IdempotencyHash = "idem-selected-stale-1"
	stale.Plan.PlanID = stale.Goal.PlanID
	stale.Plan.GoalID = stale.Goal.GoalID
	stale.Outbox.OutboxID = "outbox-selected-stale-1"
	stale.Outbox.AggregateID = stale.Goal.GoalID
	if _, e = store.CreateCloudGoal(ctx, stale); !errors.Is(e, cloudmodule.ErrSelectedRecipeConflict) {
		t.Fatalf("stale create err=%v", e)
	}
}
