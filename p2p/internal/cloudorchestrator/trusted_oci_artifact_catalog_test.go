package cloudorchestrator_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestTrustedOCIArtifactCatalogV1StrictRoundTripAndVersionGate(t *testing.T) {
	catalog := trustedCatalogFixture()
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := cloudorchestrator.ParseTrustedOCIArtifactCatalogV1(raw)
	if err != nil {
		t.Fatalf("ParseTrustedOCIArtifactCatalogV1: %v", err)
	}
	want, _ := catalog.Digest()
	got, _ := parsed.Digest()
	if got != want {
		t.Fatalf("digest=%q want=%q", got, want)
	}

	for _, version := range []string{"latest", "v1.0.3", "1.0.3", "v1.2.3"} {
		invalid := catalog
		invalid.ArtifactVersion = version
		if invalid.Validate() == nil {
			t.Fatalf("version %q was accepted", version)
		}
	}
	withUnknown := append(raw[:len(raw)-1], []byte(`,"secret_ref":"forbidden"}`)...)
	if _, err := cloudorchestrator.ParseTrustedOCIArtifactCatalogV1(withUnknown); err == nil {
		t.Fatal("unknown secret field was accepted")
	}
	var legacy map[string]any
	_ = json.Unmarshal(raw, &legacy)
	delete(legacy, "image_source")
	legacyRaw, _ := json.Marshal(legacy)
	if _, err := cloudorchestrator.ParseTrustedOCIArtifactCatalogV1(legacyRaw); err == nil {
		t.Fatal("legacy catalog without pinned image source was accepted")
	}
}

func trustedCatalogFixture() cloudorchestrator.TrustedOCIArtifactCatalogV1 {
	digest := func(character string) string { return "sha256:" + strings.Repeat(character, 64) }
	return cloudorchestrator.TrustedOCIArtifactCatalogV1{
		SchemaVersion: cloudorchestrator.TrustedOCIArtifactCatalogV1Schema, ArtifactVersion: "v1.1.0-stage-s.1",
		RecipeID: "recipe-oci-0001", RecipeDigest: digest("1"), RecipeRevision: 3,
		ImageSource:                  cloudorchestrator.OCIImageSourceReferenceV1("public.ecr.aws/dirextalk/test-service@" + digest("a")),
		CompiledRecipeArtifactDigest: digest("2"), WorkerResourceManifestDigest: digest("3"), WorkerOCICatalogDigest: digest("4"), WorkerBinaryDigest: digest("5"), RuntimeIdentity: "dirextalk-podman-v1",
		Files: []cloudorchestrator.TrustedOCIArtifactFileV1{
			{Path: cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath, SHA256: digest("6"), SizeBytes: 10, Mode: 0o755},
			{Path: cloudorchestrator.TrustedOCIArtifactCompiledRecipePath, SHA256: digest("7"), SizeBytes: 11, Mode: 0o644},
			{Path: cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath, SHA256: digest("8"), SizeBytes: 12, Mode: 0o644},
			{Path: cloudorchestrator.TrustedOCIArtifactWorkerManifestPath, SHA256: digest("9"), SizeBytes: 13, Mode: 0o644},
		},
	}
}
