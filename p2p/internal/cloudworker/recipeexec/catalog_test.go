package recipeexec_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestWorkerOCICatalogDigestAndResolverAreStableAndImmutable(t *testing.T) {
	catalog := workerOCICatalog(t)
	catalogDigest, err := catalog.Digest()
	if err != nil {
		t.Fatalf("catalog digest: %v", err)
	}
	if want := "sha256:2ebde483c155b179cd0bc4266ed100c4dc77fcf3c4d916003590af08338f913b"; catalogDigest != want {
		t.Errorf("catalog digest = %q, want %q", catalogDigest, want)
	}

	reordered := workerOCICatalog(t)
	reordered.Entries[0], reordered.Entries[1] = reordered.Entries[1], reordered.Entries[0]
	for index := range reordered.Entries {
		entry := &reordered.Entries[index]
		entry.ActionIDs[0], entry.ActionIDs[1] = entry.ActionIDs[1], entry.ActionIDs[0]
		entry.SecretTargets[0], entry.SecretTargets[1] = entry.SecretTargets[1], entry.SecretTargets[0]
		entry.Descriptor.Actions[0], entry.Descriptor.Actions[1] = entry.Descriptor.Actions[1], entry.Descriptor.Actions[0]
	}
	if got, err := reordered.Digest(); err != nil || got != catalogDigest {
		t.Fatalf("reordered catalog digest = %q, %v; want %q", got, err, catalogDigest)
	}

	manifest := recipeexec.WorkerResourceManifestV1{
		SchemaVersion:      recipeexec.WorkerResourceManifestV1Schema,
		WorkerBinaryDigest: workerCatalogDigest("f"),
		CatalogDigest:      catalogDigest,
		RuntimeIdentity:    recipeexec.WorkerRuntimeIdentityPodmanV1,
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest digest: %v", err)
	}
	if want := "sha256:017a572403039389d441614932e84db2e47a9ea63dbb0e2a463933fc7740caeb"; manifestDigest != want {
		t.Errorf("manifest digest = %q, want %q", manifestDigest, want)
	}

	resolver, err := recipeexec.NewOCICatalogResolver(catalog, manifest, manifestDigest, manifest.WorkerBinaryDigest)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	if resolver.CatalogDigest() != catalogDigest || resolver.WorkerResourceManifestDigest() != manifestDigest {
		t.Fatalf("resolver binding = catalog %q manifest %q", resolver.CatalogDigest(), resolver.WorkerResourceManifestDigest())
	}

	artifactDigest := catalog.Entries[0].ArtifactDigest
	wantFirstAction := catalog.Entries[0].Descriptor.Actions[0].ActionID
	wantFirstCheckpoint := catalog.Entries[0].Descriptor.Actions[0].CheckpointSequence[0]
	catalog.Entries[0].ActionIDs[0] = "mutated_action"
	catalog.Entries[0].SecretTargets[0].FileName = "mutated-token"
	catalog.Entries[0].Descriptor.Actions[0].ActionID = "mutated_descriptor_action"
	catalog.Entries[0].Descriptor.Actions[0].CheckpointSequence[0] = "mutated_checkpoint"
	manifest.CatalogDigest = workerCatalogDigest("0")

	bundle, err := resolver.Resolve(context.Background(), artifactDigest)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if strings.Contains(strings.Join(bundle.ActionIDs, ","), "mutated") || bundle.SecretTargets[0].FileName == "mutated-token" {
		t.Fatalf("resolver retained mutable catalog input: %#v", bundle)
	}
	bundle.ActionIDs[0] = "caller_mutation"
	bundle.SecretTargets[0].SlotID = "caller_mutation"
	secondBundle, err := resolver.Resolve(context.Background(), artifactDigest)
	if err != nil || secondBundle.ActionIDs[0] == "caller_mutation" || secondBundle.SecretTargets[0].SlotID == "caller_mutation" {
		t.Fatalf("resolver bundle is not immutable: %#v, %v", secondBundle, err)
	}

	descriptor, err := resolver.LookupDescriptor(context.Background(), artifactDigest)
	if err != nil {
		t.Fatalf("lookup descriptor: %v", err)
	}
	if descriptor.Actions[0].ActionID != wantFirstAction || descriptor.Actions[0].CheckpointSequence[0] != wantFirstCheckpoint {
		t.Fatalf("descriptor retained mutable input: %#v", descriptor.Actions[0])
	}
	descriptor.Actions[0].ActionID = "caller_mutation"
	descriptor.Actions[0].CheckpointSequence[0] = "caller_mutation"
	secondDescriptor, err := resolver.LookupDescriptor(context.Background(), artifactDigest)
	if err != nil || secondDescriptor.Actions[0].ActionID == "caller_mutation" || secondDescriptor.Actions[0].CheckpointSequence[0] == "caller_mutation" {
		t.Fatalf("resolver descriptor is not immutable: %#v, %v", secondDescriptor.Actions, err)
	}
}

func TestWorkerOCICatalogRejectsUnboundOrMutableCapabilities(t *testing.T) {
	tests := map[string]func(*recipeexec.WorkerOCICatalogV1){
		"duplicate artifact": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries = append(value.Entries, value.Entries[0])
		},
		"entry artifact mismatch": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].ArtifactDigest = workerCatalogDigest("9")
		},
		"bundle digest mismatch": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].BundleDigest = workerCatalogDigest("9")
		},
		"missing action": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].ActionIDs = value.Entries[0].ActionIDs[:1]
		},
		"extra action": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].ActionIDs = append(value.Entries[0].ActionIDs, "undeclared_action_v1")
		},
		"duplicate action": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].ActionIDs[1] = value.Entries[0].ActionIDs[0]
		},
		"mutable OCI tag": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].Descriptor.ImageDigest = "openclaw:latest"
		},
		"duplicate secret slot": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].SecretTargets[1].SlotID = value.Entries[0].SecretTargets[0].SlotID
		},
		"host secret path": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].SecretTargets[0] = recipeexec.SecretTarget{SlotID: "model_token", FileName: "../root-token"}
		},
		"environment secret persistence": func(value *recipeexec.WorkerOCICatalogV1) {
			value.Entries[0].SecretTargets[0] = recipeexec.SecretTarget{SlotID: "model_token", EnvironmentKey: "MODEL_TOKEN"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			catalog := workerOCICatalog(t)
			mutate(&catalog)
			if err := catalog.Validate(); !errors.Is(err, recipeexec.ErrWorkerOCICatalogInvalid) {
				t.Fatalf("validate error = %v", err)
			}
		})
	}
}

func TestWorkerOCICatalogBindsStorageTargetsThroughDescriptorDigest(t *testing.T) {
	catalog := workerOCICatalog(t)
	entry := &catalog.Entries[0]
	entry.Descriptor.VolumeTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service", ReadOnly: false}}
	entry.Descriptor.DataTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/service/knowledge", ReadOnly: true}}
	entry.BundleDigest, _ = entry.Descriptor.Digest()
	raw, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := recipeexec.ParseWorkerOCICatalogV1(raw)
	if err != nil || len(parsed.Entries[0].Descriptor.VolumeTargets) != 1 || len(parsed.Entries[0].Descriptor.DataTargets) != 1 {
		t.Fatalf("parsed=%#v err=%v", parsed, err)
	}
	parsed.Entries[0].Descriptor.VolumeTargets[0].ContainerTarget = "/var/lib/drifted"
	if err := parsed.Validate(); !errors.Is(err, recipeexec.ErrWorkerOCICatalogInvalid) {
		t.Fatalf("descriptor target drift error=%v", err)
	}
}

func TestWorkerOCICatalogResolverRejectsManifestDriftAndUnknownArtifacts(t *testing.T) {
	catalog := workerOCICatalog(t)
	catalogDigest, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest := recipeexec.WorkerResourceManifestV1{
		SchemaVersion:      recipeexec.WorkerResourceManifestV1Schema,
		WorkerBinaryDigest: workerCatalogDigest("f"),
		CatalogDigest:      catalogDigest,
		RuntimeIdentity:    recipeexec.WorkerRuntimeIdentityPodmanV1,
	}
	approvedDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*recipeexec.WorkerOCICatalogV1, *recipeexec.WorkerResourceManifestV1, *string, *string){
		"catalog mismatch": func(_ *recipeexec.WorkerOCICatalogV1, value *recipeexec.WorkerResourceManifestV1, _, _ *string) {
			value.CatalogDigest = workerCatalogDigest("1")
		},
		"approved manifest mismatch": func(_ *recipeexec.WorkerOCICatalogV1, _ *recipeexec.WorkerResourceManifestV1, approved, _ *string) {
			*approved = workerCatalogDigest("2")
		},
		"running binary mismatch": func(_ *recipeexec.WorkerOCICatalogV1, _ *recipeexec.WorkerResourceManifestV1, _, running *string) {
			*running = workerCatalogDigest("3")
		},
		"unsupported runtime": func(_ *recipeexec.WorkerOCICatalogV1, value *recipeexec.WorkerResourceManifestV1, _, _ *string) {
			value.RuntimeIdentity = "docker-v1"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidateCatalog := workerOCICatalog(t)
			candidateManifest := manifest
			candidateApproved, candidateRunning := approvedDigest, manifest.WorkerBinaryDigest
			mutate(&candidateCatalog, &candidateManifest, &candidateApproved, &candidateRunning)
			if _, err := recipeexec.NewOCICatalogResolver(candidateCatalog, candidateManifest, candidateApproved, candidateRunning); err == nil {
				t.Fatal("expected resolver construction to reject drift")
			}
		})
	}

	resolver, err := recipeexec.NewOCICatalogResolver(catalog, manifest, approvedDigest, "")
	if err != nil {
		t.Fatalf("new resolver with optional binary check disabled: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), workerCatalogDigest("9")); !errors.Is(err, recipeexec.ErrWorkerOCIBundleNotFound) {
		t.Fatalf("unknown resolve error = %v", err)
	}
	if _, err := resolver.LookupDescriptor(context.Background(), workerCatalogDigest("9")); !errors.Is(err, recipeexec.ErrWorkerOCIBundleNotFound) {
		t.Fatalf("unknown descriptor error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, catalog.Entries[0].ArtifactDigest); err == nil {
		t.Fatal("canceled resolution was accepted")
	}
	if _, err := resolver.LookupDescriptor(nil, catalog.Entries[0].ArtifactDigest); err == nil {
		t.Fatal("nil-context descriptor lookup was accepted")
	}
}

func TestDynamicOCICatalogResolverRegistersOneManifestBoundArtifactIdempotently(t *testing.T) {
	catalog := workerOCICatalog(t)
	catalog.Entries = catalog.Entries[:1]
	catalogDigest, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest := recipeexec.WorkerResourceManifestV1{SchemaVersion: recipeexec.WorkerResourceManifestV1Schema, WorkerBinaryDigest: workerCatalogDigest("f"), CatalogDigest: catalogDigest, RuntimeIdentity: recipeexec.WorkerRuntimeIdentityPodmanV1}
	approvedDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := recipeexec.NewDynamicOCICatalogResolver(approvedDigest, manifest.WorkerBinaryDigest)
	if err != nil {
		t.Fatal(err)
	}
	artifactDigest := catalog.Entries[0].ArtifactDigest
	if _, err := resolver.Resolve(context.Background(), artifactDigest); !errors.Is(err, recipeexec.ErrWorkerOCIBundleNotFound) {
		t.Fatalf("unregistered artifact error = %v", err)
	}
	if err := resolver.RegisterTrustedCatalog(catalog, manifest, artifactDigest); err != nil {
		t.Fatalf("RegisterTrustedCatalog() error = %v", err)
	}
	if err := resolver.RegisterTrustedCatalog(catalog, manifest, artifactDigest); err != nil {
		t.Fatalf("idempotent RegisterTrustedCatalog() error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), artifactDigest); err != nil {
		t.Fatalf("registered resolve error = %v", err)
	}

	multiple := workerOCICatalog(t)
	if err := resolver.RegisterTrustedCatalog(multiple, manifest, artifactDigest); err == nil {
		t.Fatal("registered a catalog with uncompiled extra entries")
	}
	wrongArtifact := catalog
	wrongArtifact.Entries[0].Descriptor.ImageSource = cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/service@" + workerCatalogDigest("9"))
	if err := resolver.RegisterTrustedCatalog(wrongArtifact, manifest, artifactDigest); err == nil {
		t.Fatal("registered a descriptor that drifted from the approved artifact")
	}
}

func TestWorkerOCICatalogAndManifestStrictParsing(t *testing.T) {
	catalog := workerOCICatalog(t)
	rawCatalog, err := json.Marshal(catalog)
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	parsedCatalog, err := recipeexec.ParseWorkerOCICatalogV1(rawCatalog)
	if err != nil {
		t.Fatalf("parse catalog: %v", err)
	}
	wantDigest, _ := catalog.Digest()
	if got, err := parsedCatalog.Digest(); err != nil || got != wantDigest {
		t.Fatalf("parsed digest = %q, %v; want %q", got, err, wantDigest)
	}

	catalogCases := map[string][]byte{
		"unknown top field":        replaceCatalogJSON(t, rawCatalog, []byte(`{"schema_version"`), []byte(`{"unknown":true,"schema_version"`)),
		"unknown entry field":      replaceCatalogJSON(t, rawCatalog, []byte(`{"artifact_digest"`), []byte(`{"unknown":true,"artifact_digest"`)),
		"unknown descriptor field": replaceCatalogJSON(t, rawCatalog, []byte(`"descriptor":{"schema_version"`), []byte(`"descriptor":{"unknown":true,"schema_version"`)),
		"environment secret":       bytes.Replace(rawCatalog, []byte(`"environment_key":""`), []byte(`"environment_key":"MODEL_TOKEN"`), 1),
		"trailing JSON":            append(append([]byte(nil), rawCatalog...), []byte("\n{}")...),
		"oversized":                bytes.Repeat([]byte(" "), (1<<20)+1),
	}
	for name, raw := range catalogCases {
		t.Run("catalog "+name, func(t *testing.T) {
			if _, err := recipeexec.ParseWorkerOCICatalogV1(raw); !errors.Is(err, recipeexec.ErrWorkerOCICatalogInvalid) {
				t.Fatalf("parse error = %v", err)
			}
		})
	}

	manifest := recipeexec.WorkerResourceManifestV1{
		SchemaVersion:      recipeexec.WorkerResourceManifestV1Schema,
		WorkerBinaryDigest: workerCatalogDigest("f"),
		CatalogDigest:      wantDigest,
		RuntimeIdentity:    recipeexec.WorkerRuntimeIdentityPodmanV1,
	}
	rawManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	parsedManifest, err := recipeexec.ParseWorkerResourceManifestV1(rawManifest)
	if err != nil || parsedManifest != manifest {
		t.Fatalf("parsed manifest = %#v, %v", parsedManifest, err)
	}
	manifestCases := map[string][]byte{
		"unknown field":       replaceCatalogJSON(t, rawManifest, []byte(`{"schema_version"`), []byte(`{"unknown":true,"schema_version"`)),
		"trailing JSON":       append(append([]byte(nil), rawManifest...), []byte("\n{}")...),
		"unsupported runtime": bytes.Replace(rawManifest, []byte(recipeexec.WorkerRuntimeIdentityPodmanV1), []byte("docker-v1"), 1),
	}
	for name, raw := range manifestCases {
		t.Run("manifest "+name, func(t *testing.T) {
			if _, err := recipeexec.ParseWorkerResourceManifestV1(raw); !errors.Is(err, recipeexec.ErrWorkerResourceManifestInvalid) {
				t.Fatalf("parse error = %v", err)
			}
		})
	}
}

func workerOCICatalog(t *testing.T) recipeexec.WorkerOCICatalogV1 {
	t.Helper()
	first := workerOCIServiceBundle("a", "service")
	second := workerOCIServiceBundle("b", "knowledge")
	firstDigest, err := first.Digest()
	if err != nil {
		t.Fatalf("first bundle digest: %v", err)
	}
	secondDigest, err := second.Digest()
	if err != nil {
		t.Fatalf("second bundle digest: %v", err)
	}
	return recipeexec.WorkerOCICatalogV1{
		SchemaVersion: recipeexec.WorkerOCICatalogV1Schema,
		Entries: []recipeexec.WorkerOCICatalogEntryV1{
			{
				ArtifactDigest: first.ArtifactDigest, BundleDigest: firstDigest,
				ActionIDs:     []string{"service_restart_v1", "service_install_v1"},
				SecretTargets: []recipeexec.SecretTarget{{SlotID: "model_token", FileName: "model-token"}, {SlotID: "config_file", FileName: "service-config.json"}},
				Descriptor:    first,
			},
			{
				ArtifactDigest: second.ArtifactDigest, BundleDigest: secondDigest,
				ActionIDs:     []string{"knowledge_restart_v1", "knowledge_install_v1"},
				SecretTargets: []recipeexec.SecretTarget{{SlotID: "model_token", FileName: "model-token"}, {SlotID: "config_file", FileName: "service-config.json"}},
				Descriptor:    second,
			},
		},
	}
}

func workerOCIServiceBundle(digestCharacter, actionPrefix string) cloudorchestrator.OCIServiceBundleV1 {
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{
		Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/health", ExpectedStatus: 200, BodySHA256: workerCatalogDigest("e"),
	}
	return cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: workerCatalogDigest(digestCharacter), ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("quay.io/dirextalk/" + actionPrefix + "@" + workerCatalogDigest(digestCharacter)), ImageDigest: workerCatalogDigest(digestCharacter), ImageSizeBytes: 1048576,
		Architecture: cloudorchestrator.ArchitectureAMD64,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: actionPrefix + "_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: actionPrefix + "_install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
		},
		Health: cloudorchestrator.OCIServiceHealthV1{
			Liveness: probe, Readiness: probe,
			Semantic: cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 8080, Path: "/semantic", ExpectedStatus: 200, BodySHA256: workerCatalogDigest("f")},
		},
		HealthContractDigest: workerCatalogDigest("c"), LifecycleContractDigest: workerCatalogDigest("d"),
	}
}

func workerCatalogDigest(character string) string {
	return "sha256:" + strings.Repeat(character, 64)
}

func replaceCatalogJSON(t *testing.T, raw, old, replacement []byte) []byte {
	t.Helper()
	if !bytes.Contains(raw, old) {
		t.Fatalf("JSON does not contain %q: %s", old, raw)
	}
	return bytes.Replace(raw, old, replacement, 1)
}
