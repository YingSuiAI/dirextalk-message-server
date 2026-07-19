package runtime

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRecipeArtifactTransferRegistrySelectsOnlyTheManifestBoundArchive(t *testing.T) {
	now := time.Date(2026, time.July, 16, 9, 0, 0, 0, time.UTC)
	claim := recipeInstallTestClaim(t)
	first := TrustedRecipeArtifactArchive{
		Path: "/controller/first.tar", ArchiveSHA256: strings.Repeat("9", 64), SizeBytes: 4096,
		ControllerCatalogDigest: "sha256:" + strings.Repeat("8", 64), RecipeDigest: claim.Manifest.RecipeDigest,
		ArtifactDigest: claim.Manifest.ArtifactDigest, WorkerResourceManifestDigest: claim.Manifest.WorkerResourceManifestDigest,
	}
	second := first
	second.Path = "/controller/second.tar"
	second.ArtifactDigest = "sha256:" + strings.Repeat("7", 64)
	firstUploader := &recipeArtifactUploaderMemory{versionID: "first-version"}
	secondUploader := &recipeArtifactUploaderMemory{versionID: "second-version"}
	firstManager, err := NewRecipeArtifactTransferManager(&recipeArtifactTransferMemoryStore{claim: claim}, &recipeArtifactTransferMemoryTransport{now: now}, firstUploader, first, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	secondManager, err := NewRecipeArtifactTransferManager(&recipeArtifactTransferMemoryStore{claim: claim}, &recipeArtifactTransferMemoryTransport{now: now}, secondUploader, second, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewRecipeArtifactTransferRegistry(secondManager, firstManager)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Ensure(context.Background(), claim); err != nil {
		t.Fatalf("ensure manifest-bound archive: %v", err)
	}
	if firstUploader.calls != 1 || secondUploader.calls != 0 {
		t.Fatalf("upload calls first=%d second=%d", firstUploader.calls, secondUploader.calls)
	}

	unknown := claim
	unknown.Manifest.ArtifactDigest = "sha256:" + strings.Repeat("6", 64)
	if err := registry.Ensure(context.Background(), unknown); err == nil {
		t.Fatal("registry accepted an artifact digest absent from the trusted set")
	}
}

func TestRecipeArtifactTransferRegistryRejectsDuplicateArtifactDigest(t *testing.T) {
	now := time.Date(2026, time.July, 16, 9, 0, 0, 0, time.UTC)
	claim := recipeInstallTestClaim(t)
	archive := TrustedRecipeArtifactArchive{
		Path: "/controller/first.tar", ArchiveSHA256: strings.Repeat("9", 64), SizeBytes: 4096,
		ControllerCatalogDigest: "sha256:" + strings.Repeat("8", 64), RecipeDigest: claim.Manifest.RecipeDigest,
		ArtifactDigest: claim.Manifest.ArtifactDigest, WorkerResourceManifestDigest: claim.Manifest.WorkerResourceManifestDigest,
	}
	manager, err := NewRecipeArtifactTransferManager(&recipeArtifactTransferMemoryStore{claim: claim}, &recipeArtifactTransferMemoryTransport{now: now}, &recipeArtifactUploaderMemory{versionID: "version"}, archive, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewRecipeArtifactTransferRegistry(manager, manager); err == nil {
		t.Fatal("registry accepted duplicate trusted artifact digests")
	}
}
