package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestDatabaseStoreTrustedRecipeArtifactBindsCurrentCanonicalRecipe(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	recipe, quote := cloudConfirmationFixtures(t, now, "connection-artifact-1", "quote-artifact-1")
	recipe.DataSlots = []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}}
	recipe.SecretSlots = []cloudcontracts.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider access", Delivery: cloudcontracts.SecretDeliveryFile}}
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudConfirmationState(t, store, "@artifact:example.com", quote.CloudConnectionID, "plan-artifact-1", recipe, quote, publicSPKI)
	artifact := trustedArtifactFixture(t, recipe, 1, "e")

	created, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: now.UnixMilli()})
	if err != nil || !created.Created || created.Artifact.Status != "verified" {
		t.Fatalf("register artifact=%#v err=%v", created, err)
	}
	replay, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: now.Add(time.Minute).UnixMilli()})
	if err != nil || replay.Created || replay.Artifact.DescriptorDigest != created.Artifact.DescriptorDigest {
		t.Fatalf("replay artifact=%#v err=%v", replay, err)
	}
	tampered := artifact
	tampered.MediaType = "application/vnd.dirextalk.other"
	if _, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: tampered, RegisteredAt: now.UnixMilli()}); err != cloudmodule.ErrRecipeArtifactConflict {
		t.Fatalf("same artifact changed descriptor err=%v", err)
	}
	second := artifact
	second.ArtifactDigest = namedArtifactDigest("f")
	if _, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: second, RegisteredAt: now.UnixMilli()}); err != cloudmodule.ErrRecipeArtifactConflict {
		t.Fatalf("second artifact for recipe revision err=%v", err)
	}
}

func TestDatabaseStoreTrustedRecipeArtifactRejectsRecipeBoundaryMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*cloudcontracts.CompiledRecipeArtifactV1)
	}{
		{"source", func(v *cloudcontracts.CompiledRecipeArtifactV1) {
			v.OfficialSourceArtifactDigests[0] = namedArtifactDigest("9")
		}},
		{"architecture", func(v *cloudcontracts.CompiledRecipeArtifactV1) {
			v.Architecture, v.Requirements.Architecture = cloudcontracts.ArchitectureARM64, cloudcontracts.ArchitectureARM64
		}},
		{"resource minimum", func(v *cloudcontracts.CompiledRecipeArtifactV1) { v.Requirements.MinMemoryMiB++ }},
		{"health", func(v *cloudcontracts.CompiledRecipeArtifactV1) { v.HealthContractDigest = namedArtifactDigest("8") }},
		{"lifecycle", func(v *cloudcontracts.CompiledRecipeArtifactV1) { v.LifecycleContractDigest = namedArtifactDigest("7") }},
		{"slot scope", func(v *cloudcontracts.CompiledRecipeArtifactV1) {
			v.DataSlots = append(v.DataSlots, cloudcontracts.RecipeDataSlotRequirementV1{SlotID: "extra_data", Purpose: "extra corpus", ReadOnly: true})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newCloudConfirmationStore(t)
			now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
			recipe, quote := cloudConfirmationFixtures(t, now, "connection-artifact-invalid", "quote-artifact-invalid")
			_, publicSPKI := cloudConfirmationDeviceKey(t)
			seedCloudConfirmationState(t, store, "@artifact:example.com", quote.CloudConnectionID, "plan-artifact-invalid", recipe, quote, publicSPKI)
			artifact := trustedArtifactFixture(t, recipe, 1, "e")
			test.mutate(&artifact)
			if _, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: now.UnixMilli()}); err != cloudmodule.ErrRecipeArtifactInvalid {
				t.Fatalf("boundary mismatch err=%v", err)
			}
			var count int
			if err := store.DB().QueryRowContext(ctx, `SELECT COUNT(*) FROM p2p_cloud_recipe_artifacts`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("artifact count=%d err=%v", count, err)
			}
		})
	}
}

func TestDatabaseStoreRecipeExecutionManifestRequiresVerifiedArtifact(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 13, 0, 0, 0, time.UTC)
	_, _, _, manifest := seedCloudRecipeExecutionReadyDeployment(t, store, now)
	request := cloudmodule.RegisterTrustedRecipeExecutionManifestRequest{Manifest: manifest, RegisteredAt: now.Add(2 * time.Minute).UnixMilli()}
	if _, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, request); err != cloudmodule.ErrRecipeExecutionManifestInvalid {
		t.Fatalf("unverified artifact registration err=%v", err)
	}
	registerTrustedArtifactForExecutionManifest(t, store, manifest, now.Add(time.Minute).UnixMilli())
	secretTampering := []struct {
		name   string
		mutate func(*cloudcontracts.RecipeExecutionManifestV1)
	}{
		{"slot", func(v *cloudcontracts.RecipeExecutionManifestV1) { v.SecretSlots[0].SlotID = "forged_slot" }},
		{"ref", func(v *cloudcontracts.RecipeExecutionManifestV1) {
			v.SecretSlots[0].SecretRef = "secret_ref:forged/ref"
		}},
		{"missing", func(v *cloudcontracts.RecipeExecutionManifestV1) { v.SecretSlots = v.SecretSlots[:1] }},
		{"extra", func(v *cloudcontracts.RecipeExecutionManifestV1) {
			v.SecretSlots = append(v.SecretSlots, cloudcontracts.SecretSlotV1{SlotID: "extra_slot", SecretRef: "secret_ref:extra/ref"})
		}},
	}
	for _, test := range secretTampering {
		t.Run(test.name, func(t *testing.T) {
			tampered := manifest
			tampered.SecretSlots = append([]cloudcontracts.SecretSlotV1(nil), manifest.SecretSlots...)
			test.mutate(&tampered)
			if _, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, cloudmodule.RegisterTrustedRecipeExecutionManifestRequest{Manifest: tampered, RegisteredAt: request.RegisteredAt}); err != cloudmodule.ErrRecipeExecutionManifestInvalid {
				t.Fatalf("tampered secret scope registration err=%v", err)
			}
		})
	}
	var storedPlanJSON string
	if err := store.DB().QueryRowContext(ctx, `SELECT display_json FROM p2p_cloud_plan_versions WHERE plan_id=$1 ORDER BY revision DESC LIMIT 1`, manifest.PlanID).Scan(&storedPlanJSON); err != nil {
		t.Fatal(err)
	}
	var storedPlan cloudcontracts.PlanV1
	if err := json.Unmarshal([]byte(storedPlanJSON), &storedPlan); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*cloudcontracts.SecretReferenceV1)
	}{
		{"purpose", func(v *cloudcontracts.SecretReferenceV1) { v.Purpose = "forged purpose" }},
		{"delivery", func(v *cloudcontracts.SecretReferenceV1) { v.Delivery = cloudcontracts.SecretDeliveryFile }},
	} {
		t.Run("plan_"+test.name, func(t *testing.T) {
			tamperedPlan := storedPlan
			tamperedPlan.SecretScope = append([]cloudcontracts.SecretReferenceV1(nil), storedPlan.SecretScope...)
			test.mutate(&tamperedPlan.SecretScope[1])
			tamperedManifest := manifest
			tamperedManifest.PlanHash = replaceStoredRecipeExecutionPlan(t, store, tamperedPlan)
			if _, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, cloudmodule.RegisterTrustedRecipeExecutionManifestRequest{Manifest: tamperedManifest, RegisteredAt: request.RegisteredAt}); err != cloudmodule.ErrRecipeExecutionManifestInvalid {
				t.Fatalf("tampered Plan secret %s registration err=%v", test.name, err)
			}
			replaceStoredRecipeExecutionPlan(t, store, storedPlan)
		})
	}
	tampered := manifest
	tampered.ActionID = "unverified-install-action"
	if _, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, cloudmodule.RegisterTrustedRecipeExecutionManifestRequest{Manifest: tampered, RegisteredAt: request.RegisteredAt}); err != cloudmodule.ErrRecipeExecutionManifestInvalid {
		t.Fatalf("unverified action registration err=%v", err)
	}
	if result, err := store.RegisterTrustedCloudRecipeExecutionManifest(ctx, request); err != nil || !result.Created {
		t.Fatalf("verified artifact registration=%#v err=%v", result, err)
	}
}

func replaceStoredRecipeExecutionPlan(t *testing.T, store *DatabaseStore, ready cloudcontracts.PlanV1) string {
	t.Helper()
	readyHash, err := ready.Hash()
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := ready.CanonicalPlanCBOR()
	if err != nil {
		t.Fatal(err)
	}
	display, err := json.Marshal(ready)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_plan_versions SET canonical_cbor=$1,display_json=$2,plan_hash=$3 WHERE plan_id=$4 AND revision=$5`, canonical, string(display), readyHash, ready.PlanID, ready.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().ExecContext(ctx, `UPDATE p2p_cloud_plans SET plan_hash=$1 WHERE plan_id=$2`, readyHash, ready.PlanID); err != nil {
		t.Fatal(err)
	}
	approved := ready
	approved.Status = cloudcontracts.PlanApproved
	approved.Revision++
	approvedHash, err := approved.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return approvedHash
}

func TestDatabaseStoreCloudRecipeRecommendationsAreOwnerScoped(t *testing.T) {
	ctx := context.Background()
	store := newCloudConfirmationStore(t)
	now := time.Date(2026, time.July, 15, 14, 0, 0, 0, time.UTC)
	recipe, quote := cloudConfirmationFixtures(t, now, "connection-recommendation-1", "quote-recommendation-1")
	_, publicSPKI := cloudConfirmationDeviceKey(t)
	seedCloudConfirmationState(t, store, "@recipe-owner:example.com", quote.CloudConnectionID, "plan-recommendation-1", recipe, quote, publicSPKI)

	items, err := store.ListCloudRecipeRecommendations(ctx, "@recipe-owner:example.com")
	if err != nil || len(items) != 1 || items[0].RecipeID != recipe.RecipeID || items[0].Resources.MinMemoryMiB != recipe.Requirements.MinMemoryMiB {
		t.Fatalf("owner recommendations=%#v err=%v", items, err)
	}
	other, err := store.ListCloudRecipeRecommendations(ctx, "@other-owner:example.com")
	if err != nil || len(other) != 0 {
		t.Fatalf("other-owner recommendations=%#v err=%v", other, err)
	}
}

func trustedArtifactFixture(t *testing.T, recipe cloudcontracts.RecipeV1, revision uint64, artifactCharacter string) cloudcontracts.CompiledRecipeArtifactV1 {
	t.Helper()
	recipeDigest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	healthDigest, err := cloudcontracts.HealthContractDigestV1(recipe.Health)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleDigest, err := cloudcontracts.LifecycleContractDigestV1(recipe.Lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	official := make([]string, 0, len(recipe.Sources))
	for _, source := range recipe.Sources {
		if source.Official {
			official = append(official, source.ArtifactDigest)
		}
	}
	return cloudcontracts.CompiledRecipeArtifactV1{
		SchemaVersion: cloudcontracts.CompiledRecipeArtifactV1Schema, RecipeID: recipe.RecipeID, RecipeDigest: recipeDigest, RecipeRevision: revision,
		OfficialSourceArtifactDigests: official, Architecture: recipe.Requirements.Architecture, Requirements: recipe.Requirements,
		WorkerResourceManifestDigest: namedArtifactDigest("c"), ArtifactDigest: namedArtifactDigest(artifactCharacter), MediaType: "application/vnd.dirextalk.recipe", SizeBytes: 1024,
		Actions:              []cloudcontracts.CompiledRecipeActionV1{{Kind: cloudcontracts.CompiledRecipeActionInstall, ActionID: "install-service", RootRequired: true, TimeoutSeconds: 1200, CheckpointSequence: []string{"artifact_verified", "health_verified"}}},
		HealthContractDigest: healthDigest, LifecycleContractDigest: lifecycleDigest,
		VolumeSlots: append([]cloudcontracts.RecipeVolumeSlotRequirementV1{}, recipe.VolumeSlots...), DataSlots: append([]cloudcontracts.RecipeDataSlotRequirementV1{}, recipe.DataSlots...), SecretSlots: append([]cloudcontracts.RecipeSecretSlotRequirementV1{}, recipe.SecretSlots...),
	}
}

func registerTrustedArtifactForExecutionManifest(t *testing.T, store *DatabaseStore, manifest cloudcontracts.RecipeExecutionManifestV1, registeredAt int64) {
	t.Helper()
	var displayJSON string
	var revision uint64
	if err := store.DB().QueryRowContext(context.Background(), `
		SELECT version.display_json, recipe.revision FROM p2p_cloud_recipes recipe
		JOIN p2p_cloud_recipe_versions version ON version.recipe_id=recipe.recipe_id AND version.revision=recipe.revision
		WHERE recipe.digest=$1
	`, manifest.RecipeDigest).Scan(&displayJSON, &revision); err != nil {
		t.Fatal(err)
	}
	var recipe cloudcontracts.RecipeV1
	if err := json.Unmarshal([]byte(displayJSON), &recipe); err != nil {
		t.Fatal(err)
	}
	artifact := trustedArtifactFixture(t, recipe, revision, strings.TrimPrefix(manifest.ArtifactDigest, "sha256:")[:1])
	artifact.ArtifactDigest = manifest.ArtifactDigest
	artifact.WorkerResourceManifestDigest = manifest.WorkerResourceManifestDigest
	artifact.Actions[0] = cloudcontracts.CompiledRecipeActionV1{Kind: cloudcontracts.CompiledRecipeActionInstall, ActionID: manifest.ActionID, RootRequired: manifest.RootRequired, TimeoutSeconds: manifest.TimeoutSeconds, CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...)}
	if _, err := store.RegisterTrustedCloudRecipeArtifact(context.Background(), cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: registeredAt}); err != nil {
		t.Fatal(err)
	}
	var descriptorJSON string
	if err := store.DB().QueryRowContext(context.Background(), `SELECT descriptor_json FROM p2p_cloud_recipe_artifacts WHERE artifact_digest=$1`, manifest.ArtifactDigest).Scan(&descriptorJSON); err != nil {
		t.Fatal(err)
	}
	stored, err := cloudcontracts.ParseCompiledRecipeArtifactV1([]byte(descriptorJSON))
	if err != nil {
		t.Fatalf("parse stored artifact: %v", err)
	}
	if err := validateExecutionManifestAgainstArtifact(manifest, stored); err != nil {
		t.Fatalf("manifest does not match stored artifact: %#v %#v", manifest, stored)
	}
}

func namedArtifactDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
