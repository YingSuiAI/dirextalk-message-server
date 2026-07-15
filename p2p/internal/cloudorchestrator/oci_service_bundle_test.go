package cloudorchestrator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestOCIServiceBundleIsStrictNormalizedAndDigestPinned(t *testing.T) {
	checkpointCopy := cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()
	checkpointCopy[0] = "mutated"
	if got := cloudorchestrator.OCIServiceInstallCheckpointSequenceV1(); got[0] != "artifact_verified" {
		t.Fatalf("install checkpoint contract retained caller mutation: %v", got)
	}
	bundle := ociServiceBundle()
	digest, err := bundle.Digest()
	if err != nil {
		t.Fatal(err)
	}
	reordered := bundle
	reordered.Actions = []cloudorchestrator.CompiledRecipeActionV1{bundle.Actions[1], bundle.Actions[0]}
	reorderedDigest, err := reordered.Digest()
	if err != nil || reorderedDigest != digest {
		t.Fatalf("normalized digest=%s err=%v, want %s", reorderedDigest, err, digest)
	}
	canonical, _ := bundle.CanonicalOCIServiceBundleCBOR()
	sum := sha256.Sum256(canonical)
	const want = "7a67428caca78be4a7669cbda4655e013ad4f300572599b17f7bbc948d495ad4"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("OCI service bundle golden=%s", got)
	}
	raw, _ := json.Marshal(bundle)
	if _, err := cloudorchestrator.ParseOCIServiceBundleV1(raw); err != nil {
		t.Fatal(err)
	}
	var expanded map[string]any
	_ = json.Unmarshal(raw, &expanded)
	expanded["argv"] = []string{"sh", "-c", "curl example.invalid"}
	unsafe, _ := json.Marshal(expanded)
	if _, err := cloudorchestrator.ParseOCIServiceBundleV1(unsafe); err == nil {
		t.Fatal("strict parser accepted argv")
	}
}

func TestOCIServiceBundleRejectsMutableOrUnsafeInputs(t *testing.T) {
	for name, mutate := range map[string]func(*cloudorchestrator.OCIServiceBundleV1){
		"tag": func(value *cloudorchestrator.OCIServiceBundleV1) { value.ImageDigest = "openclaw:latest" },
		"URL": func(value *cloudorchestrator.OCIServiceBundleV1) {
			value.Health.Readiness.Path = "https://example.invalid/health"
		},
		"host path": func(value *cloudorchestrator.OCIServiceBundleV1) { value.Health.Readiness.Path = "/../../etc/passwd" },
		"secret": func(value *cloudorchestrator.OCIServiceBundleV1) {
			value.Health.Readiness.Path = "/sk-abcdefghijklmnopqrstuvwxyz"
		},
		"no install":     func(value *cloudorchestrator.OCIServiceBundleV1) { value.Actions = value.Actions[:1] },
		"artifact drift": func(value *cloudorchestrator.OCIServiceBundleV1) { value.ArtifactDigest = bundleDigest("f") },
		"install checkpoint order": func(value *cloudorchestrator.OCIServiceBundleV1) {
			value.Actions[1].CheckpointSequence[0], value.Actions[1].CheckpointSequence[1] = value.Actions[1].CheckpointSequence[1], value.Actions[1].CheckpointSequence[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := ociServiceBundle()
			mutate(&value)
			if err := value.Validate(); err == nil {
				t.Fatal("unsafe OCI bundle was accepted")
			}
		})
	}
}

func ociServiceBundle() cloudorchestrator.OCIServiceBundleV1 {
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/health", ExpectedStatus: 200, BodySHA256: bundleDigest("e")}
	return cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: bundleDigest("a"), ImageDigest: bundleDigest("a"), ImageSizeBytes: 1048576,
		Architecture: cloudorchestrator.ArchitectureAMD64,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"service_restarted", "health_verified"}},
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
		},
		Health:               cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: bundleDigest("f")}},
		HealthContractDigest: bundleDigest("c"), LifecycleContractDigest: bundleDigest("d"),
	}
}

func bundleDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
