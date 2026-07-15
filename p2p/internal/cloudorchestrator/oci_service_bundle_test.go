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
	const want = "a05c7758ac9dce81a8af73dc66d3809b30b4618f73718ff73a70544b1675ca34"
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

func TestOCIServiceLifecycleCheckpointContractsAreFixedByActionKind(t *testing.T) {
	want := map[cloudorchestrator.CompiledRecipeActionKind][]string{
		cloudorchestrator.CompiledRecipeActionInstall: {"artifact_verified", "container_created", "container_started", "health_verified"},
		cloudorchestrator.CompiledRecipeActionStart:   {"container_started", "health_verified"},
		cloudorchestrator.CompiledRecipeActionStop:    {"container_stopped"},
		cloudorchestrator.CompiledRecipeActionRestart: {"container_stopped", "container_started", "health_verified"},
	}
	for kind, expected := range want {
		actual, ok := cloudorchestrator.OCIServiceActionCheckpointSequenceV1(kind)
		if !ok || strings.Join(actual, ",") != strings.Join(expected, ",") {
			t.Fatalf("kind=%q checkpoints=%v ok=%v", kind, actual, ok)
		}
		actual[0] = "mutated"
		again, _ := cloudorchestrator.OCIServiceActionCheckpointSequenceV1(kind)
		if again[0] != expected[0] {
			t.Fatalf("kind=%q retained caller mutation: %v", kind, again)
		}
	}
	if _, ok := cloudorchestrator.OCIServiceActionCheckpointSequenceV1(cloudorchestrator.CompiledRecipeActionUpgrade); ok {
		t.Fatal("unsupported OCI lifecycle action received an executable checkpoint contract")
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

func TestOCIServiceBundleStorageTargetsAreDigestBoundStrictAndLegacyCompatible(t *testing.T) {
	legacy := ociServiceBundle()
	legacyRaw, _ := json.Marshal(legacy)
	if parsed, err := cloudorchestrator.ParseOCIServiceBundleV1(legacyRaw); err != nil || len(parsed.VolumeTargets) != 0 || len(parsed.DataTargets) != 0 {
		t.Fatalf("legacy bundle=%#v err=%v", parsed, err)
	}
	legacyDigest, _ := legacy.Digest()
	bound := legacy
	bound.VolumeTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service", ReadOnly: false}}
	bound.DataTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/service/knowledge", ReadOnly: true}}
	boundDigest, err := bound.Digest()
	if err != nil || boundDigest == legacyDigest {
		t.Fatalf("bound digest=%q legacy=%q err=%v", boundDigest, legacyDigest, err)
	}
	boundRaw, _ := json.Marshal(bound)
	parsed, err := cloudorchestrator.ParseOCIServiceBundleV1(boundRaw)
	if err != nil || len(parsed.VolumeTargets) != 1 || len(parsed.DataTargets) != 1 || !parsed.DataTargets[0].ReadOnly {
		t.Fatalf("parsed=%#v err=%v", parsed, err)
	}
	for name, target := range map[string]string{
		"root": "/", "traversal": "/srv/../etc", "proc": "/proc/service", "sys": "/sys/fs", "dev": "/dev/data",
		"secrets": "/run/secrets/model", "comma": "/srv/data,ro", "control": "/srv/data\nnext",
	} {
		t.Run(name, func(t *testing.T) {
			candidate := bound
			candidate.VolumeTargets = append([]cloudorchestrator.OCIServiceStorageTargetV1(nil), bound.VolumeTargets...)
			candidate.VolumeTargets[0].ContainerTarget = target
			if err := candidate.Validate(); err == nil {
				t.Fatalf("unsafe target %q accepted", target)
			}
		})
	}
	duplicate := bound
	duplicate.DataTargets = append([]cloudorchestrator.OCIServiceStorageTargetV1(nil), bound.DataTargets...)
	duplicate.DataTargets[0].ContainerTarget = bound.VolumeTargets[0].ContainerTarget
	if err := duplicate.Validate(); err == nil {
		t.Fatal("duplicate cross-kind target accepted")
	}
}

func TestOCIServiceRuntimeProfileIsStrictCanonicalAndZeroCompatible(t *testing.T) {
	legacy := ociServiceBundle()
	legacyDigest, err := legacy.Digest()
	if err != nil {
		t.Fatal(err)
	}
	zero := legacy
	zero.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{}
	if digest, err := zero.Digest(); err != nil || digest != legacyDigest {
		t.Fatalf("zero profile digest=%q err=%v legacy=%q", digest, err, legacyDigest)
	}
	profile := &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint: "/usr/local/bin/node", Argv: []string{"/app/openclaw.mjs", "gateway", "--port", "8080"},
		Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "OPENCLAW_MODE", Value: "gateway"}, {Name: "MODEL_TOKEN_FILE", Value: "/run/secrets/model-token"}},
		Tmpfs:       []cloudorchestrator.OCIServiceTmpfsV1{{ContainerTarget: "/tmp", SizeBytes: 64 << 20, Mode: 0o1777}},
		RunAs:       &cloudorchestrator.OCIServiceRunAsV1{UID: 1000, GID: 1000}, SecretReadGID: 1000,
	}
	bound := legacy
	bound.RuntimeProfile = profile
	bound.VolumeTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/home/node/.openclaw", OwnerUID: 1000, OwnerGID: 1000, DirectoryMode: 0o700}}
	boundRaw, _ := json.Marshal(bound)
	parsed, err := cloudorchestrator.ParseOCIServiceBundleV1(boundRaw)
	if err != nil || parsed.RuntimeProfile == nil || parsed.RuntimeProfile.RunAs == nil || parsed.RuntimeProfile.RunAs.UID != 1000 || parsed.VolumeTargets[0].OwnerUID != 1000 {
		t.Fatalf("parsed profile=%#v volumes=%#v err=%v", parsed.RuntimeProfile, parsed.VolumeTargets, err)
	}
	if digest, err := bound.Digest(); err != nil || digest == legacyDigest {
		t.Fatalf("profile was not digest-bound: digest=%q legacy=%q err=%v", digest, legacyDigest, err)
	}
	rootInit := *profile
	rootInit.Tmpfs = []cloudorchestrator.OCIServiceTmpfsV1{{ContainerTarget: "/run", SizeBytes: 64 << 20, Mode: 0o755}}
	if err := rootInit.Validate(); err != nil {
		t.Fatalf("bounded root-init /run tmpfs rejected: %v", err)
	}
	storageRun := bound
	storageRun.VolumeTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/run", DirectoryMode: 0o700}}
	if err := storageRun.Validate(); err == nil {
		t.Fatal("storage target was allowed to replace protected /run")
	}

	for name, mutate := range map[string]func(*cloudorchestrator.OCIServiceRuntimeProfileV1){
		"shell":        func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Entrypoint = "/bin/sh" },
		"nul argv":     func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Argv[0] = "gateway\x00--unsafe" },
		"newline argv": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Argv[0] = "gateway\nnext" },
		"secret env": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) {
			value.Environment[0].Value = "sk-abcdefghijklmnopqrstuvwxyz"
		},
		"expanded env": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Environment[0].Value = "${HOME}" },
		"secret path": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) {
			value.Environment[0].Value = "/run/secrets/token"
		},
		"system tmpfs":   func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Tmpfs[0].ContainerTarget = "/proc" },
		"oversize tmpfs": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) { value.Tmpfs[0].SizeBytes = 1 << 50 },
		"capability": func(value *cloudorchestrator.OCIServiceRuntimeProfileV1) {
			value.Capabilities = []cloudorchestrator.OCIServiceCapability{"SYS_ADMIN"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := *profile
			candidate.Argv = append([]string(nil), profile.Argv...)
			candidate.Environment = append([]cloudorchestrator.OCIServiceEnvironmentV1(nil), profile.Environment...)
			candidate.Tmpfs = append([]cloudorchestrator.OCIServiceTmpfsV1(nil), profile.Tmpfs...)
			mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("unsafe runtime profile accepted")
			}
		})
	}
}

func TestOCIImageSourceReferenceAllowsOnlyPublicDigestPinnedRepositories(t *testing.T) {
	digest := bundleDigest("a")
	for _, value := range []string{
		"ghcr.io/openclaw/openclaw@" + digest,
		"docker.io/library/nginx@" + digest,
		"quay.io/example/knowledge-node@" + digest,
		"public.ecr.aws/example/model_runner@" + digest,
	} {
		reference := cloudorchestrator.OCIImageSourceReferenceV1(value)
		if err := reference.Validate(); err != nil {
			t.Fatalf("valid source %q: %v", value, err)
		}
		if pinned, err := reference.PinnedDigest(); err != nil || pinned != digest {
			t.Fatalf("pinned digest=%q err=%v", pinned, err)
		}
	}
	for _, value := range []string{
		"ghcr.io/openclaw/openclaw:latest",
		"https://ghcr.io/openclaw/openclaw@" + digest,
		"user:password@ghcr.io/openclaw/openclaw@" + digest,
		"registry.example.com/openclaw/openclaw@" + digest,
		"ghcr.io/OpenClaw/openclaw@" + digest,
		"ghcr.io/openclaw/openclaw@" + digest + "?token=secret",
		"ghcr.io/openclaw/openclaw@sha256:" + strings.Repeat("A", 64),
	} {
		if err := cloudorchestrator.OCIImageSourceReferenceV1(value).Validate(); err == nil {
			t.Fatalf("unsafe source %q was accepted", value)
		}
	}
}

func ociServiceBundle() cloudorchestrator.OCIServiceBundleV1 {
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/health", ExpectedStatus: 200, BodySHA256: bundleDigest("e")}
	return cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: bundleDigest("a"), ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/test-service@" + bundleDigest("a")), ImageDigest: bundleDigest("a"), ImageSizeBytes: 1048576,
		Architecture: cloudorchestrator.ArchitectureAMD64,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
		},
		Health:               cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: bundleDigest("f")}},
		HealthContractDigest: bundleDigest("c"), LifecycleContractDigest: bundleDigest("d"),
	}
}

func bundleDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
