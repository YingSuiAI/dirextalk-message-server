package artifactbuilder_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/artifactbuilder"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestBuildArchiveIsDeterministicAndRoundTripsBindings(t *testing.T) {
	recipe, spec := artifactFixture()
	recipeRaw, _ := json.Marshal(recipe)
	specRaw, _ := json.Marshal(spec)
	binaryPath := filepath.Join(t.TempDir(), "cloud-worker")
	binaryRaw := []byte("fixed-worker-binary-v1")
	if err := os.WriteFile(binaryPath, binaryRaw, 0o700); err != nil {
		t.Fatal(err)
	}

	var first, second bytes.Buffer
	firstResult, err := artifactbuilder.BuildArchive(recipeRaw, specRaw, binaryPath, &first)
	if err != nil {
		t.Fatalf("BuildArchive(first): %v", err)
	}
	secondResult, err := artifactbuilder.BuildArchive(recipeRaw, specRaw, binaryPath, &second)
	if err != nil {
		t.Fatalf("BuildArchive(second): %v", err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) || firstResult.ArchiveSHA256 != secondResult.ArchiveSHA256 {
		t.Fatal("identical inputs produced different archives")
	}

	entries := readTar(t, first.Bytes())
	wantOrder := []string{artifactbuilder.ControllerTrustedArtifactCatalog, cloudorchestrator.TrustedOCIArtifactCompiledRecipePath, cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath, cloudorchestrator.TrustedOCIArtifactWorkerManifestPath, cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath}
	if strings.Join(entries.order, ",") != strings.Join(wantOrder, ",") {
		t.Fatalf("tar order=%v", entries.order)
	}
	controller, err := cloudorchestrator.ParseTrustedOCIArtifactCatalogV1(entries.raw[artifactbuilder.ControllerTrustedArtifactCatalog])
	if err != nil {
		t.Fatalf("parse controller catalog: %v", err)
	}
	controllerDigest, _ := controller.Digest()
	if controllerDigest != firstResult.CatalogDigest {
		t.Fatalf("controller digest=%q result=%q", controllerDigest, firstResult.CatalogDigest)
	}
	for _, binding := range controller.Files {
		raw := entries.raw[binding.Path]
		sum := sha256.Sum256(raw)
		if got := "sha256:" + hex.EncodeToString(sum[:]); got != binding.SHA256 || uint64(len(raw)) != binding.SizeBytes || entries.mode[binding.Path] != binding.Mode {
			t.Fatalf("file binding mismatch for %s", binding.Path)
		}
	}
	artifact, err := cloudorchestrator.ParseCompiledRecipeArtifactV1(entries.raw[cloudorchestrator.TrustedOCIArtifactCompiledRecipePath])
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := recipeexec.ParseWorkerResourceManifestV1(entries.raw[cloudorchestrator.TrustedOCIArtifactWorkerManifestPath])
	if err != nil {
		t.Fatal(err)
	}
	workerCatalog, err := recipeexec.ParseWorkerOCICatalogV1(entries.raw[cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath])
	if err != nil {
		t.Fatal(err)
	}
	artifactDigest, _ := artifact.Digest()
	manifestDigest, _ := manifest.Digest()
	workerCatalogDigest, _ := workerCatalog.Digest()
	if artifactDigest != controller.CompiledRecipeArtifactDigest || artifact.ImageSource != spec.ImageSource || controller.ImageSource != spec.ImageSource || workerCatalog.Entries[0].Descriptor.ImageSource != spec.ImageSource || manifestDigest != artifact.WorkerResourceManifestDigest || manifestDigest != controller.WorkerResourceManifestDigest || workerCatalogDigest != manifest.CatalogDigest || workerCatalogDigest != controller.WorkerOCICatalogDigest || !bytes.Equal(entries.raw[cloudorchestrator.TrustedOCIArtifactWorkerBinaryPath], binaryRaw) {
		t.Fatal("cross-artifact digest binding is incomplete")
	}
}

func TestBuildArchiveRejectsMutableVersionUnknownSecretAndSymlink(t *testing.T) {
	recipe, spec := artifactFixture()
	recipeRaw, _ := json.Marshal(recipe)
	binary := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(binary, []byte("worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"latest", "v1.0.3", "v1.2.3"} {
		candidate := spec
		candidate.ArtifactVersion = version
		raw, _ := json.Marshal(candidate)
		if _, err := artifactbuilder.BuildArchive(recipeRaw, raw, binary, io.Discard); err == nil {
			t.Fatalf("version %q accepted", version)
		}
	}
	specRaw, _ := json.Marshal(spec)
	withSecret := append(specRaw[:len(specRaw)-1], []byte(`,"secret_ref":"forbidden"}`)...)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, withSecret, binary, io.Discard); err == nil {
		t.Fatal("unknown secret field accepted")
	}
	withoutSource := spec
	withoutSource.ImageSource = ""
	withoutSourceRaw, _ := json.Marshal(withoutSource)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, withoutSourceRaw, binary, io.Discard); err == nil {
		t.Fatal("legacy catalog input without a pinned image source was accepted")
	}

	symlink := filepath.Join(filepath.Dir(binary), "worker-link")
	if err := os.Symlink(binary, symlink); err == nil {
		if _, err := artifactbuilder.BuildArchive(recipeRaw, specRaw, symlink, io.Discard); err == nil {
			t.Fatal("symlink worker binary accepted")
		}
	}
}

func TestBuildArchiveRequiresExactCompilerOwnedStorageTargets(t *testing.T) {
	recipe, spec := artifactFixture()
	recipe.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{{SlotID: "state", Purpose: "service state", ReadOnly: false}}
	recipe.DataSlots = []cloudorchestrator.RecipeDataSlotRequirementV1{{SlotID: "knowledge", Purpose: "knowledge corpus", ReadOnly: true}}
	spec.VolumeTargets = []cloudorchestrator.OCIServiceMountTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service"}}
	spec.DataTargets = []cloudorchestrator.OCIServiceMountTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/service/knowledge"}}
	spec.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "MODEL_TOKEN_FILE", Value: "/run/secrets/model-token"}}}
	recipeRaw, _ := json.Marshal(recipe)
	binary := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(binary, []byte("worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	validRaw, _ := json.Marshal(spec)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, validRaw, binary, io.Discard); err != nil {
		t.Fatalf("valid storage build: %v", err)
	}
	unbound := spec
	unbound.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "MODEL_TOKEN_FILE", Value: "/run/secrets/missing"}}}
	unboundRaw, _ := json.Marshal(unbound)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, unboundRaw, binary, io.Discard); err == nil {
		t.Fatal("unbound *_FILE build accepted")
	}
	for name, mutate := range map[string]func(*artifactbuilder.BuildSpecV1){
		"missing": func(value *artifactbuilder.BuildSpecV1) { value.DataTargets = nil },
		"unknown": func(value *artifactbuilder.BuildSpecV1) { value.VolumeTargets[0].SlotID = "other" },
		"duplicate target": func(value *artifactbuilder.BuildSpecV1) {
			value.DataTargets[0].ContainerTarget = value.VolumeTargets[0].ContainerTarget
		},
		"system path": func(value *artifactbuilder.BuildSpecV1) {
			value.VolumeTargets[0].ContainerTarget = "/run/secrets/state"
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := spec
			candidate.VolumeTargets = append([]cloudorchestrator.OCIServiceMountTargetV1(nil), spec.VolumeTargets...)
			candidate.DataTargets = append([]cloudorchestrator.OCIServiceMountTargetV1(nil), spec.DataTargets...)
			mutate(&candidate)
			raw, _ := json.Marshal(candidate)
			if _, err := artifactbuilder.BuildArchive(recipeRaw, raw, binary, io.Discard); err == nil {
				t.Fatal("unsafe storage target build accepted")
			}
		})
	}
}

func TestBuildArchiveBindsVerifiedOpenClawRuntimeProfile(t *testing.T) {
	recipe, spec := artifactFixture()
	const imageDigest = "sha256:165b4992f1b4b74ffdd7a02c887ba006f9f5dc951eca420eef573a8b233b543f"
	spec.ImageSource = cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/openclaw/openclaw@" + imageDigest)
	spec.ImageDigest = imageDigest
	recipe.VolumeSlots = []cloudorchestrator.RecipeVolumeSlotRequirementV1{{SlotID: "state", Purpose: "OpenClaw state", ReadOnly: false}}
	recipe.SecretSlots = append(recipe.SecretSlots, cloudorchestrator.RecipeSecretSlotRequirementV1{SlotID: "gateway_token", Purpose: "OpenClaw gateway authentication", Delivery: cloudorchestrator.SecretDeliveryFile})
	spec.VolumeTargets = []cloudorchestrator.OCIServiceMountTargetV1{{SlotID: "state", ContainerTarget: "/home/node/.openclaw", OwnerUID: 1000, OwnerGID: 1000, DirectoryMode: 0o700}}
	spec.SecretTargets = append(spec.SecretTargets, artifactbuilder.FileSecretTargetV1{SlotID: "gateway_token", FileName: "gateway-token"})
	spec.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint: "/usr/bin/tini", Argv: []string{"-s", "--", "node", "openclaw.mjs", "gateway", "--bind", "lan", "--port", "18789", "--allow-unconfigured"},
		Environment:       []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "OPENCLAW_DISABLE_BONJOUR", Value: "1"}},
		SecretEnvironment: []cloudorchestrator.OCIServiceSecretEnvironmentV1{{SlotID: "model_token", EnvironmentKey: "OPENAI_API_KEY"}, {SlotID: "gateway_token", EnvironmentKey: "OPENCLAW_GATEWAY_TOKEN"}},
		Tmpfs:             []cloudorchestrator.OCIServiceTmpfsV1{{ContainerTarget: "/tmp", SizeBytes: 64 << 20, Mode: 0o1777}},
		RunAs:             &cloudorchestrator.OCIServiceRunAsV1{UID: 1000, GID: 1000},
		Capabilities:      []cloudorchestrator.OCIServiceCapability{cloudorchestrator.OCIServiceCapabilitySetGID, cloudorchestrator.OCIServiceCapabilitySetUID},
		SecretReadGID:     1000,
	}
	recipeRaw, _ := json.Marshal(recipe)
	specRaw, _ := json.Marshal(spec)
	binary := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(binary, []byte("measured-worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if _, err := artifactbuilder.BuildArchive(recipeRaw, specRaw, binary, &archive); err != nil {
		t.Fatalf("OpenClaw profile build: %v", err)
	}
	entries := readTar(t, archive.Bytes())
	artifact, err := cloudorchestrator.ParseCompiledRecipeArtifactV1(entries.raw[cloudorchestrator.TrustedOCIArtifactCompiledRecipePath])
	if err != nil || artifact.RuntimeProfile == nil || len(artifact.RuntimeProfile.SecretEnvironment) != 2 || artifact.RuntimeProfile.SecretEnvironment[0].EnvironmentKey != "OPENAI_API_KEY" || artifact.RuntimeProfile.SecretEnvironment[1].EnvironmentKey != "OPENCLAW_GATEWAY_TOKEN" {
		t.Fatalf("artifact profile=%#v err=%v", artifact.RuntimeProfile, err)
	}
	catalog, err := recipeexec.ParseWorkerOCICatalogV1(entries.raw[cloudorchestrator.TrustedOCIArtifactWorkerCatalogPath])
	if err != nil || catalog.Entries[0].Descriptor.RuntimeProfile == nil || catalog.Entries[0].Descriptor.VolumeTargets[0].OwnerUID != 1000 {
		t.Fatalf("catalog=%#v err=%v", catalog, err)
	}

	unsafe := spec
	unsafe.RuntimeProfile = cloudorchestrator.CloneOCIServiceRuntimeProfileV1(spec.RuntimeProfile)
	unsafe.RuntimeProfile.Capabilities = []cloudorchestrator.OCIServiceCapability{"SYS_ADMIN"}
	unsafeRaw, _ := json.Marshal(unsafe)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, unsafeRaw, binary, io.Discard); err == nil {
		t.Fatal("unsafe OpenClaw capability accepted")
	}
}

func TestBuildArchiveRejectsNonCanonicalTypedLifecycleActions(t *testing.T) {
	recipe, spec := artifactFixture()
	spec.Actions = append(spec.Actions,
		cloudorchestrator.CompiledRecipeActionV1{Kind: cloudorchestrator.CompiledRecipeActionStart, ActionID: recipe.Lifecycle.Start, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceStartCheckpointSequenceV1()},
		cloudorchestrator.CompiledRecipeActionV1{Kind: cloudorchestrator.CompiledRecipeActionStop, ActionID: recipe.Lifecycle.Stop, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceStopCheckpointSequenceV1()},
		cloudorchestrator.CompiledRecipeActionV1{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: recipe.Lifecycle.Restart, RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
	)
	recipeRaw, _ := json.Marshal(recipe)
	binary := filepath.Join(t.TempDir(), "worker")
	if err := os.WriteFile(binary, []byte("worker"), 0o700); err != nil {
		t.Fatal(err)
	}
	validRaw, _ := json.Marshal(spec)
	if _, err := artifactbuilder.BuildArchive(recipeRaw, validRaw, binary, io.Discard); err != nil {
		t.Fatalf("canonical lifecycle build: %v", err)
	}
	for name, mutate := range map[string]func(*artifactbuilder.BuildSpecV1){
		"start order": func(value *artifactbuilder.BuildSpecV1) {
			value.Actions[1].CheckpointSequence = []string{"health_verified", "container_started"}
		},
		"stop extra": func(value *artifactbuilder.BuildSpecV1) {
			value.Actions[2].CheckpointSequence = []string{"container_stopped", "health_verified"}
		},
		"restart missing stop": func(value *artifactbuilder.BuildSpecV1) {
			value.Actions[3].CheckpointSequence = []string{"container_started", "health_verified"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := spec
			candidate.Actions = cloneBuilderTestActions(spec.Actions)
			mutate(&candidate)
			raw, _ := json.Marshal(candidate)
			if _, err := artifactbuilder.BuildArchive(recipeRaw, raw, binary, io.Discard); err == nil {
				t.Fatal("non-canonical lifecycle build accepted")
			}
		})
	}
}

type tarEntries struct {
	order []string
	raw   map[string][]byte
	mode  map[string]uint32
}

func readTar(t *testing.T, raw []byte) tarEntries {
	t.Helper()
	result := tarEntries{raw: map[string][]byte{}, mode: map[string]uint32{}}
	reader := tar.NewReader(bytes.NewReader(raw))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag != tar.TypeReg || !header.ModTime.Equal(time.Unix(0, 0).UTC()) {
			t.Fatalf("non-deterministic header for %s", header.Name)
		}
		content, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		result.order = append(result.order, header.Name)
		result.raw[header.Name] = content
		result.mode[header.Name] = uint32(header.Mode)
	}
	return result
}

func artifactFixture() (cloudorchestrator.RecipeV1, artifactbuilder.BuildSpecV1) {
	digest := func(character string) string { return "sha256:" + strings.Repeat(character, 64) }
	healthContract := cloudorchestrator.HealthContractV1{Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/health"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/semantic"}}
	lifecycle := cloudorchestrator.LifecycleContractV1{Start: "start_v1", Stop: "stop_v1", Restart: "restart_v1", Upgrade: "upgrade_v1", Rollback: "rollback_v1", Backup: "backup_v1", Restore: "restore_v1", Destroy: "destroy_v1"}
	recipe := cloudorchestrator.RecipeV1{SchemaVersion: cloudorchestrator.SchemaVersionV1, RecipeID: "recipe-artifact-0001", Name: "Artifact fixture", Maturity: cloudorchestrator.RecipeExperimental,
		Sources:      []cloudorchestrator.RecipeSourceV1{{URL: "https://github.com/example/service", Version: "v1.2.3", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: digest("1"), License: "Apache-2.0", RetrievedAt: time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC), Official: true}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{MinVCPU: 2, MinMemoryMiB: 4096, MinDiskGiB: 40, Architecture: cloudorchestrator.ArchitectureAMD64},
		Install:      cloudorchestrator.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1(), Steps: []cloudorchestrator.InstallStepV1{{ID: "install", Summary: "Install service", TimeoutSeconds: 1800}}}, Health: healthContract, Lifecycle: lifecycle,
		SecretSlots: []cloudorchestrator.RecipeSecretSlotRequirementV1{{SlotID: "model_token", Purpose: "model provider token", Delivery: cloudorchestrator.SecretDeliveryFile}}}
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: cloudorchestrator.FixedReadinessEvidenceDigestV1}
	spec := artifactbuilder.BuildSpecV1{SchemaVersion: artifactbuilder.BuildSpecV1Schema, ArtifactVersion: "v1.1.0-stage-s.1", RecipeRevision: 4, ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/artifact-fixture@" + digest("2")), ImageDigest: digest("2"), ImageSizeBytes: 1 << 20,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()}},
		Health:  cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: probe}, SecretTargets: []artifactbuilder.FileSecretTargetV1{{SlotID: "model_token", FileName: "model-token"}}}
	return recipe, spec
}

func cloneBuilderTestActions(actions []cloudorchestrator.CompiledRecipeActionV1) []cloudorchestrator.CompiledRecipeActionV1 {
	result := append([]cloudorchestrator.CompiledRecipeActionV1(nil), actions...)
	for index := range result {
		result[index].CheckpointSequence = append([]string(nil), actions[index].CheckpointSequence...)
	}
	return result
}
