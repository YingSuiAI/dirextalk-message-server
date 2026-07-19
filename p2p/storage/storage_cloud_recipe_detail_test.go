package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDatabaseStoreCloudRecipeDetailUsesCurrentCanonicalVersionAndOwnerScope(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 16, 0, 0, 0, time.UTC)
	recipe, quote := cloudConfirmationFixtures(t, now, "connection-detail-1", "quote-detail-1")
	recipe.Sources = append(recipe.Sources, cloudcontracts.RecipeSourceV1{
		URL: "https://community.example.invalid/untrusted", Version: "edge", Commit: "fedcba9876543210fedcba9876543210fedcba98",
		ArtifactDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", License: "unknown", RetrievedAt: now, Official: false,
	})
	recipe.VolumeSlots = []cloudcontracts.RecipeVolumeSlotRequirementV1{{SlotID: "knowledge_volume", Purpose: "persistent index", ReadOnly: false}}
	recipe.DataSlots = []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "documents", Purpose: "source documents", ReadOnly: true}}
	recipe.SecretSlots = []cloudcontracts.RecipeSecretSlotRequirementV1{
		{SlotID: "github_app", Purpose: "private source access", Delivery: cloudcontracts.SecretDeliveryFile},
		{SlotID: "model_token", Purpose: "model provider access", Delivery: cloudcontracts.SecretDeliveryEnvironment},
	}
	if err := recipe.Validate(); err != nil {
		t.Fatal(err)
	}
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudConfirmationState(t, store, "@recipe-owner:example.com", quote.CloudConnectionID, "plan-detail-1", recipe, quote, publicSPKI)

	// Management maturity is authoritative metadata. The current version still
	// points at the same verified canonical deployment contract and digest.
	if _, err := store.DB().ExecContext(ctx, `
		INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at)
		SELECT recipe_id,2,canonical_cbor,display_json,digest,'managed',$2 FROM p2p_cloud_recipe_versions WHERE recipe_id=$1 AND revision=1
	`, recipe.RecipeID, now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipes SET version='v2',maturity='managed',revision=2,updated_at=$2 WHERE recipe_id=$1`, recipe.RecipeID, now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}

	detail, found, err := store.GetCloudRecipeDetail(ctx, "@recipe-owner:example.com", recipe.RecipeID)
	if err != nil || !found {
		t.Fatalf("owner detail=%#v found=%v err=%v", detail, found, err)
	}
	if detail.Version != "v2" || detail.Maturity != "managed" || detail.Revision != 2 || detail.Digest == "" ||
		detail.Requirements != cloudRecipeDetailRequirements(recipe.Requirements) || detail.Health != cloudRecipeDetailHealth(recipe.Health) || detail.Lifecycle != cloudRecipeDetailLifecycle(recipe.Lifecycle) ||
		len(detail.OfficialSources) != 1 || detail.OfficialSources[0].Commit != recipe.Sources[0].Commit ||
		len(detail.VolumeSlots) != 1 || len(detail.DataSlots) != 1 || len(detail.SecretSlots) != 2 {
		t.Fatalf("unexpected current detail: %#v", detail)
	}
	encoded, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"community.example.invalid", "secret_ref:", "s3://private-provider-object", "@recipe-owner:example.com", quote.CloudConnectionID, "artifact_body", "command_text"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("owner detail leaked %q: %s", forbidden, encoded)
		}
	}

	if other, otherFound, otherErr := store.GetCloudRecipeDetail(ctx, "@other-owner:example.com", recipe.RecipeID); otherErr != nil || otherFound || other.RecipeID != "" {
		t.Fatalf("other owner detail=%#v found=%v err=%v", other, otherFound, otherErr)
	}
	if old, oldFound, oldErr := store.GetCloudRecipeDetail(ctx, "@recipe-owner:example.com", "recipe-old-private-version"); oldErr != nil || oldFound || old.RecipeID != "" {
		t.Fatalf("old recipe detail=%#v found=%v err=%v", old, oldFound, oldErr)
	}

	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_recipe_versions SET canonical_cbor=$2 WHERE recipe_id=$1 AND revision=2`, recipe.RecipeID, []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetCloudRecipeDetail(ctx, "@recipe-owner:example.com", recipe.RecipeID); err == nil || found {
		t.Fatalf("tampered current canonical found=%v err=%v", found, err)
	}
}
