package storepg

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestStoreRegistersTrustedRecipeArtifactIdempotentlyAndRejectsDrift(t *testing.T) {
	ctx, database, closeDatabase := openMigratedStore(t)
	defer closeDatabase()
	now := time.Date(2026, time.July, 16, 3, 0, 0, 0, time.UTC)
	recipe := testResearchOutput(t, now).Recipe
	recipe.Install.CheckpointNames = cloudcontracts.OCIServiceInstallCheckpointSequenceV1()
	digest, err := recipe.Digest()
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := recipe.CanonicalRecipeCBOR()
	if err != nil {
		t.Fatal(err)
	}
	display, err := json.Marshal(recipe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = database.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipes(recipe_id,name,version,digest,maturity,revision,created_at,updated_at) VALUES($1,$2,'v1',$3,'experimental',1,$4,$4)`, recipe.RecipeID, recipe.Name, digest, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = database.DB().ExecContext(ctx, `INSERT INTO p2p_cloud_recipe_versions(recipe_id,revision,canonical_cbor,display_json,digest,maturity,created_at) VALUES($1,1,$2,$3,$4,'experimental',$5)`, recipe.RecipeID, canonical, string(display), digest, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	artifact := trustedStoreArtifact(t, recipe, digest)
	store := New(database.DB(), Config{Now: func() time.Time { return now }})
	request := cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: artifact, RegisteredAt: now.UnixMilli()}
	first, err := store.RegisterTrustedCloudRecipeArtifact(ctx, request)
	if err != nil || !first.Created || first.Artifact.Status != "verified" {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	second, err := store.RegisterTrustedCloudRecipeArtifact(ctx, request)
	if err != nil || second.Created || second.Artifact.DescriptorDigest != first.Artifact.DescriptorDigest {
		t.Fatalf("second=%#v err=%v", second, err)
	}
	sourceDrift := artifact
	sourceDrift.ImageSource = cloudcontracts.OCIImageSourceReferenceV1("quay.io/dirextalk/store-service@" + sourceDrift.ArtifactDigest)
	if _, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: sourceDrift, RegisteredAt: now.Add(time.Second).UnixMilli()}); !errors.Is(err, cloudmodule.ErrRecipeArtifactConflict) {
		t.Fatalf("source drift error=%v", err)
	}
	drift := artifact
	drift.ArtifactDigest = trustedStoreDigest("9")
	drift.ImageSource = cloudcontracts.OCIImageSourceReferenceV1("ghcr.io/dirextalk/store-service@" + drift.ArtifactDigest)
	if _, err := store.RegisterTrustedCloudRecipeArtifact(ctx, cloudmodule.RegisterTrustedRecipeArtifactRequest{Artifact: drift, RegisteredAt: now.Add(time.Second).UnixMilli()}); !errors.Is(err, cloudmodule.ErrRecipeArtifactConflict) {
		t.Fatalf("drift error=%v", err)
	}
	var count int
	if err := database.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM p2p_cloud_recipe_artifacts`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("artifact count=%d err=%v", count, err)
	}
}

func trustedStoreArtifact(t *testing.T, recipe cloudcontracts.RecipeV1, recipeDigest string) cloudcontracts.CompiledRecipeArtifactV1 {
	t.Helper()
	healthDigest, err := cloudcontracts.HealthContractDigestV1(recipe.Health)
	if err != nil {
		t.Fatal(err)
	}
	lifecycleDigest, err := cloudcontracts.LifecycleContractDigestV1(recipe.Lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	artifact := cloudcontracts.CompiledRecipeArtifactV1{
		SchemaVersion: cloudcontracts.CompiledRecipeArtifactV1Schema, RecipeID: recipe.RecipeID, RecipeDigest: recipeDigest, RecipeRevision: 1,
		OfficialSourceArtifactDigests: []string{recipe.Sources[0].ArtifactDigest}, Architecture: recipe.Requirements.Architecture, Requirements: recipe.Requirements,
		WorkerResourceManifestDigest: trustedStoreDigest("3"), ArtifactDigest: trustedStoreDigest("2"), ImageSource: cloudcontracts.OCIImageSourceReferenceV1("ghcr.io/dirextalk/store-service@" + trustedStoreDigest("2")), MediaType: "application/vnd.oci.image.manifest.v1+json", SizeBytes: 1048576,
		Actions:              []cloudcontracts.CompiledRecipeActionV1{{Kind: cloudcontracts.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: recipe.Install.RootRequired, TimeoutSeconds: recipe.Install.TimeoutSeconds, CheckpointSequence: cloudcontracts.OCIServiceInstallCheckpointSequenceV1()}},
		SemanticReadiness:    cloudcontracts.OCIServiceLoopbackProbeV1{Scheme: cloudcontracts.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: trustedStoreDigest("5")},
		HealthContractDigest: healthDigest, LifecycleContractDigest: lifecycleDigest,
		VolumeSlots: []cloudcontracts.RecipeVolumeSlotRequirementV1{}, DataSlots: []cloudcontracts.RecipeDataSlotRequirementV1{}, SecretSlots: []cloudcontracts.RecipeSecretSlotRequirementV1{},
	}
	if err := artifact.Validate(); err != nil {
		t.Fatalf("artifact fixture: %v", err)
	}
	return artifact
}

func trustedStoreDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
