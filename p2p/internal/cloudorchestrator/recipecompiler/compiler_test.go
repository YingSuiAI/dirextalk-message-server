package recipecompiler_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/recipecompiler"
)

func TestCompileOCIServiceBundleProducesExactPinnedArtifacts(t *testing.T) {
	recipe, config := compilerFixture()
	artifact, bundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatal(err)
	}
	if artifact.ArtifactDigest != config.ImageDigest || bundle.ArtifactDigest != artifact.ArtifactDigest || bundle.ImageDigest != config.ImageDigest || artifact.WorkerResourceManifestDigest != config.WorkerResourceManifestDigest {
		t.Fatalf("compiled digest binding drifted: artifact=%#v bundle=%#v", artifact, bundle)
	}
	if !reflect.DeepEqual(artifact.Actions, bundle.Actions) {
		t.Fatal("bundle actions do not exactly bind compiled artifact actions")
	}
	firstArtifactDigest, _ := artifact.Digest()
	firstBundleDigest, _ := bundle.Digest()
	secondArtifact, secondBundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatal(err)
	}
	secondArtifactDigest, _ := secondArtifact.Digest()
	secondBundleDigest, _ := secondBundle.Digest()
	if firstArtifactDigest != secondArtifactDigest || firstBundleDigest != secondBundleDigest {
		t.Fatal("compiler output is not deterministic")
	}
}

func TestCompileOCIServiceBundleTwoStageMatchesConvenienceAndRejectsDrift(t *testing.T) {
	recipe, config := compilerFixture()
	wantArtifact, wantBundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatal(err)
	}

	preManifestConfig := config
	preManifestConfig.WorkerResourceManifestDigest = ""
	bundle, err := recipecompiler.CompileOCIServiceBundleDescriptor(recipe, preManifestConfig)
	if err != nil {
		t.Fatalf("compile descriptor: %v", err)
	}
	artifact, err := recipecompiler.FinalizeOCIServiceArtifact(recipe, preManifestConfig, bundle, config.WorkerResourceManifestDigest)
	if err != nil {
		t.Fatalf("finalize artifact: %v", err)
	}
	if !reflect.DeepEqual(bundle, wantBundle) || !reflect.DeepEqual(artifact, wantArtifact) {
		t.Fatalf("two-stage output drifted: artifact=%#v bundle=%#v", artifact, bundle)
	}
	artifactDigest, _ := artifact.Digest()
	bundleDigest, _ := bundle.Digest()
	if artifactDigest != "sha256:4d2e55a7aa9e5e63e42b2fcdc44db19ac2c2479e23c0086236f42d5371214fea" || bundleDigest != "sha256:49f25841104b994cbf28bc8852b5242b73e5da443c352e3296856f71741c3ae8" {
		t.Fatalf("golden digest drift: artifact=%q bundle=%q", artifactDigest, bundleDigest)
	}

	drifted := bundle
	drifted.Actions = append([]cloudorchestrator.CompiledRecipeActionV1(nil), bundle.Actions...)
	drifted.Actions[0].ActionID = "drifted_install_action"
	if _, err := recipecompiler.FinalizeOCIServiceArtifact(recipe, preManifestConfig, drifted, config.WorkerResourceManifestDigest); err == nil {
		t.Fatal("finalize accepted a bundle that drifted after catalog construction")
	}
}

func TestCompileOCIServiceBundleRejectsMutableOrUnboundInputs(t *testing.T) {
	for name, mutate := range map[string]func(*cloudorchestrator.RecipeV1, *recipecompiler.Config){
		"tag": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.ImageDigest = "openclaw:latest"
		},
		"architecture": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Architecture = cloudorchestrator.ArchitectureARM64
		},
		"health drift": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.HealthContract.Readiness.Target = "/other"
		},
		"lifecycle drift": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Actions[1].ActionID = "other_restart"
		},
		"shell action": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Actions[0].ActionID = "sh -c curl"
		},
		"install checkpoint order": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Actions[0].CheckpointSequence[0], config.Actions[0].CheckpointSequence[1] = config.Actions[0].CheckpointSequence[1], config.Actions[0].CheckpointSequence[0]
		},
		"unofficial only": func(recipe *cloudorchestrator.RecipeV1, _ *recipecompiler.Config) { recipe.Sources[0].Official = false },
	} {
		t.Run(name, func(t *testing.T) {
			recipe, config := compilerFixture()
			mutate(&recipe, &config)
			if _, _, err := recipecompiler.CompileOCIServiceBundle(recipe, config); err == nil {
				t.Fatal("unsafe compiler input was accepted")
			}
		})
	}
}

func compilerFixture() (cloudorchestrator.RecipeV1, recipecompiler.Config) {
	health := cloudorchestrator.HealthContractV1{
		Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/health"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/semantic"},
	}
	lifecycle := cloudorchestrator.LifecycleContractV1{Start: "service_start_v1", Stop: "service_stop_v1", Restart: "service_restart_v1", Upgrade: "service_upgrade_v1", Rollback: "service_rollback_v1", Backup: "service_backup_v1", Restore: "service_restore_v1", Destroy: "service_destroy_v1"}
	recipe := cloudorchestrator.RecipeV1{
		SchemaVersion: cloudorchestrator.SchemaVersionV1, RecipeID: "recipe-oci-0001", Name: "Verified OCI service", Maturity: cloudorchestrator.RecipeExperimental,
		Sources:      []cloudorchestrator.RecipeSourceV1{{URL: "https://github.com/example/service", Version: "v1.2.3", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: compilerDigest("1"), License: "Apache-2.0", RetrievedAt: time.Date(2026, time.July, 15, 0, 0, 0, 0, time.UTC), Official: true}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{MinVCPU: 2, MinMemoryMiB: 4096, MinDiskGiB: 40, Architecture: cloudorchestrator.ArchitectureAMD64},
		Install:      cloudorchestrator.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1(), Steps: []cloudorchestrator.InstallStepV1{{ID: "install_service", Summary: "Install verified OCI service", TimeoutSeconds: 1800}}},
		Health:       health, Lifecycle: lifecycle,
	}
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/health", ExpectedStatus: 200, BodySHA256: compilerDigest("6")}
	config := recipecompiler.Config{
		RecipeRevision: 3, ImageDigest: compilerDigest("2"), ImageSizeBytes: 1048576, Architecture: cloudorchestrator.ArchitectureAMD64, WorkerResourceManifestDigest: compilerDigest("3"),
		HealthContract: health, LifecycleContract: lifecycle,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"service_restarted", "health_verified"}},
		},
		Health: cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/ready", ExpectedStatus: 200, BodySHA256: compilerDigest("7")}, Semantic: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: compilerDigest("8")}},
	}
	return recipe, config
}

func compilerDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
