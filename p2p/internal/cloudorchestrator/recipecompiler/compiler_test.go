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
	if artifact.ArtifactDigest != config.ImageDigest || artifact.ImageSource != config.ImageSource || bundle.ImageSource != config.ImageSource || bundle.ArtifactDigest != artifact.ArtifactDigest || bundle.ImageDigest != config.ImageDigest || artifact.WorkerResourceManifestDigest != config.WorkerResourceManifestDigest {
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
	if artifactDigest != "sha256:82b4e6581aaeb0d1235e09bbc3fc586dd32d75102bd898bb3db099bcebb0f61f" || bundleDigest != "sha256:fb5cca46f9080c3d8090fcc469cee686357b3eafc56f96b7b22fce66343c1f2b" {
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
			config.ImageSource = "ghcr.io/openclaw/openclaw:latest"
		},
		"untrusted registry": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.ImageSource = cloudorchestrator.OCIImageSourceReferenceV1("registry.example.com/service@" + config.ImageDigest)
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
		"restart checkpoint drift": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Actions[1].CheckpointSequence = []string{"service_restarted", "health_verified"}
		},
		"unsupported typed action": func(_ *cloudorchestrator.RecipeV1, config *recipecompiler.Config) {
			config.Actions[1].Kind = cloudorchestrator.CompiledRecipeActionUpgrade
			config.Actions[1].ActionID = "service_upgrade_v1"
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

func TestCompileOCIServiceBundleAcceptsOnlyCanonicalTypedLifecycleActions(t *testing.T) {
	recipe, config := compilerFixture()
	install := config.Actions[0]
	config.Actions = []cloudorchestrator.CompiledRecipeActionV1{
		install,
		{Kind: cloudorchestrator.CompiledRecipeActionStart, ActionID: recipe.Lifecycle.Start, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceStartCheckpointSequenceV1()},
		{Kind: cloudorchestrator.CompiledRecipeActionStop, ActionID: recipe.Lifecycle.Stop, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceStopCheckpointSequenceV1()},
		{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: recipe.Lifecycle.Restart, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
	}
	if _, _, err := recipecompiler.CompileOCIServiceBundle(recipe, config); err != nil {
		t.Fatalf("canonical lifecycle: %v", err)
	}
	for name, mutate := range map[string]func(*recipecompiler.Config){
		"start order": func(value *recipecompiler.Config) {
			value.Actions[1].CheckpointSequence = []string{"health_verified", "container_started"}
		},
		"stop extra": func(value *recipecompiler.Config) {
			value.Actions[2].CheckpointSequence = []string{"container_stopped", "health_verified"}
		},
		"restart missing stop": func(value *recipecompiler.Config) {
			value.Actions[3].CheckpointSequence = []string{"container_started", "health_verified"}
		},
		"non-root": func(value *recipecompiler.Config) { value.Actions[1].RootRequired = false },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := config
			candidate.Actions = cloneCompilerTestActions(config.Actions)
			mutate(&candidate)
			if _, _, err := recipecompiler.CompileOCIServiceBundle(recipe, candidate); err == nil {
				t.Fatal("non-canonical lifecycle action accepted")
			}
		})
	}
}

func TestCompileOCIServiceBundleBindsCompilerOwnedRuntimeProfile(t *testing.T) {
	recipe, config := compilerFixture()
	recipe.SecretSlots = []cloudorchestrator.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider token", Delivery: cloudorchestrator.SecretDeliveryFile}}
	config.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint: "/usr/bin/tini", Argv: []string{"-s", "--", "node", "openclaw.mjs", "gateway", "--bind", "lan", "--port", "18789", "--allow-unconfigured"},
		Environment:       []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "OPENCLAW_DISABLE_BONJOUR", Value: "1"}},
		SecretEnvironment: []cloudorchestrator.OCIServiceSecretEnvironmentV1{{SlotID: "model_token", EnvironmentKey: "OPENAI_API_KEY"}},
		Tmpfs:             []cloudorchestrator.OCIServiceTmpfsV1{{ContainerTarget: "/tmp", SizeBytes: 32 << 20, Mode: 0o1777}},
		RunAs:             &cloudorchestrator.OCIServiceRunAsV1{UID: 1000, GID: 1000},
		Capabilities:      []cloudorchestrator.OCIServiceCapability{cloudorchestrator.OCIServiceCapabilitySetUID, cloudorchestrator.OCIServiceCapabilitySetGID},
		SecretReadGID:     1000,
	}
	artifact, bundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil || artifact.RuntimeProfile == nil || bundle.RuntimeProfile == nil || !reflect.DeepEqual(artifact.RuntimeProfile, bundle.RuntimeProfile) {
		t.Fatalf("artifact profile=%#v bundle=%#v err=%v", artifact.RuntimeProfile, bundle.RuntimeProfile, err)
	}
	config.RuntimeProfile.Argv[0] = "mutated-after-compile"
	if artifact.RuntimeProfile.Argv[0] != "-s" || bundle.RuntimeProfile.Argv[0] != "-s" {
		t.Fatal("compiled profile retained caller-owned slices")
	}
	unknown := config
	unknown.RuntimeProfile = cloudorchestrator.CloneOCIServiceRuntimeProfileV1(bundle.RuntimeProfile)
	unknown.RuntimeProfile.SecretEnvironment[0].SlotID = "unknown_token"
	if _, _, err := recipecompiler.CompileOCIServiceBundle(recipe, unknown); err == nil {
		t.Fatal("runtime profile secret environment accepted an undeclared recipe slot")
	}
}

func TestCompileOCIServiceBundleBindsExactRecipeStorageTargets(t *testing.T) {
	recipe, config := compilerFixture()
	recipe.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{{SlotID: "state", Purpose: "service state", ReadOnly: false}}
	recipe.DataSlots = []cloudorchestrator.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}}
	config.VolumeTargets = []cloudorchestrator.OCIServiceMountTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service", OwnerUID: 1000, OwnerGID: 1000, DirectoryMode: 0o700}}
	config.DataTargets = []cloudorchestrator.OCIServiceMountTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/service/knowledge", OwnerUID: 1000, OwnerGID: 1000, DirectoryMode: 0o550}}
	_, bundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.VolumeTargets) != 1 || bundle.VolumeTargets[0].ReadOnly || bundle.VolumeTargets[0].OwnerUID != 1000 || bundle.VolumeTargets[0].DirectoryMode != 0o700 || len(bundle.DataTargets) != 1 || !bundle.DataTargets[0].ReadOnly || bundle.DataTargets[0].DirectoryMode != 0o550 {
		t.Fatalf("storage targets=%#v %#v", bundle.VolumeTargets, bundle.DataTargets)
	}
	for name, mutate := range map[string]func(*recipecompiler.Config){
		"missing":      func(value *recipecompiler.Config) { value.DataTargets = nil },
		"unknown slot": func(value *recipecompiler.Config) { value.VolumeTargets[0].SlotID = "other" },
		"duplicate target": func(value *recipecompiler.Config) {
			value.DataTargets[0].ContainerTarget = value.VolumeTargets[0].ContainerTarget
		},
		"unsafe target": func(value *recipecompiler.Config) { value.VolumeTargets[0].ContainerTarget = "/proc/service" },
		"unsafe mode":   func(value *recipecompiler.Config) { value.VolumeTargets[0].DirectoryMode = 0o777 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := config
			candidate.VolumeTargets = append([]cloudorchestrator.OCIServiceMountTargetV1(nil), config.VolumeTargets...)
			candidate.DataTargets = append([]cloudorchestrator.OCIServiceMountTargetV1(nil), config.DataTargets...)
			mutate(&candidate)
			if _, _, err := recipecompiler.CompileOCIServiceBundle(recipe, candidate); err == nil {
				t.Fatal("unbound storage compiler input accepted")
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
		RecipeRevision: 3, ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("quay.io/dirextalk/verified-service@" + compilerDigest("2")), ImageDigest: compilerDigest("2"), ImageSizeBytes: 1048576, Architecture: cloudorchestrator.ArchitectureAMD64, WorkerResourceManifestDigest: compilerDigest("3"),
		HealthContract: health, LifecycleContract: lifecycle,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
		},
		Health: cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/ready", ExpectedStatus: 200, BodySHA256: compilerDigest("7")}, Semantic: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: compilerDigest("8")}},
	}
	return recipe, config
}

func compilerDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }

func cloneCompilerTestActions(actions []cloudorchestrator.CompiledRecipeActionV1) []cloudorchestrator.CompiledRecipeActionV1 {
	result := append([]cloudorchestrator.CompiledRecipeActionV1(nil), actions...)
	for index := range result {
		result[index].CheckpointSequence = append([]string(nil), actions[index].CheckpointSequence...)
	}
	return result
}
