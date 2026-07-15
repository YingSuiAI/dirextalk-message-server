package cloudworker

import (
	"context"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/recipecompiler"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/ociservice"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestOCIRecipeFlowCompilesInstallsAndReplaysWithoutHostMutation(t *testing.T) {
	flow := newOCIRecipeFlow(t)
	host, store := &ociRecipeFlowHost{}, &ociRecipeFlowCASStore{}
	executor := recipeexec.Executor{Resolver: flow.resolver, Store: store, Driver: ociservice.NewDriver(flow.resolver, host)}

	result, err := executor.Execute(context.Background(), flow.manifest)
	if err != nil {
		t.Fatalf("execute compiled OCI recipe: %v", err)
	}
	wantCheckpoints := []string{
		ociservice.CheckpointArtifactVerified,
		ociservice.CheckpointContainerCreated,
		ociservice.CheckpointContainerStarted,
		ociservice.CheckpointHealthVerified,
	}
	if !result.Completed || result.LastCheckpoint != ociservice.CheckpointHealthVerified || !reflect.DeepEqual(store.checkpoints(), wantCheckpoints) {
		t.Fatalf("result=%#v checkpoints=%v", result, store.checkpoints())
	}
	if len(host.specs) != 1 || host.specs[0].ImageDigest != flow.bundle.ImageDigest || len(host.probes) != 3 ||
		host.probes[0] != flow.bundle.Health.Liveness || host.probes[1] != flow.bundle.Health.Readiness || host.probes[2] != flow.bundle.Health.Semantic {
		t.Fatalf("typed host specs=%#v probes=%#v", host.specs, host.probes)
	}
	if host.specs[0].Name == "" || host.specs[0].BindingDigest == "" {
		t.Fatalf("container identity is empty: %#v", host.specs[0])
	}

	beforeReplay := host.callCount()
	replayed, err := executor.Execute(context.Background(), flow.manifest)
	if err != nil || !replayed.Completed || !replayed.Resumed || host.callCount() != beforeReplay {
		t.Fatalf("completed replay result=%#v err=%v host calls=%v", replayed, err, host.calls)
	}

	secondHost := &ociRecipeFlowHost{}
	secondExecutor := recipeexec.Executor{Resolver: flow.resolver, Store: &ociRecipeFlowCASStore{}, Driver: ociservice.NewDriver(flow.resolver, secondHost)}
	if _, err := secondExecutor.Execute(context.Background(), flow.manifest); err != nil {
		t.Fatalf("repeat deterministic install: %v", err)
	}
	if len(secondHost.specs) != 1 || secondHost.specs[0].Name != host.specs[0].Name || secondHost.specs[0].BindingDigest != host.specs[0].BindingDigest {
		t.Fatalf("container identity drifted: first=%#v second=%#v", host.specs, secondHost.specs)
	}
}

func TestOCIRecipeFlowResumesAndRejectsDigestDriftBeforeHostMutation(t *testing.T) {
	flow := newOCIRecipeFlow(t)
	manifestDigest, err := flow.manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	binding := recipeexec.Binding{ExecutionID: flow.manifest.ExecutionID, ManifestDigest: manifestDigest}
	store := &ociRecipeFlowCASStore{
		initialized: true,
		state: recipeexec.CheckpointState{
			Binding: binding, Checkpoint: ociservice.CheckpointContainerCreated, Index: 1,
		},
	}
	host := &ociRecipeFlowHost{}
	executor := recipeexec.Executor{Resolver: flow.resolver, Store: store, Driver: ociservice.NewDriver(flow.resolver, host)}
	result, err := executor.Execute(context.Background(), flow.manifest)
	if err != nil || !result.Completed || !result.Resumed {
		t.Fatalf("resume result=%#v err=%v", result, err)
	}
	if len(host.specs) != 1 || !reflect.DeepEqual(host.calls, []string{"ensure", "start", "probe:/live", "probe:/ready", "probe:/semantic"}) ||
		!reflect.DeepEqual(store.checkpoints(), []string{ociservice.CheckpointContainerStarted, ociservice.CheckpointHealthVerified}) {
		t.Fatalf("resume did not revalidate durable container binding: calls=%v specs=%#v checkpoints=%v", host.calls, host.specs, store.checkpoints())
	}

	driftHost := &ociRecipeFlowHost{}
	driftedManifest := flow.manifest
	driftedManifest.ArtifactDigest = ociRecipeFlowDigest("8")
	driftExecutor := recipeexec.Executor{Resolver: flow.resolver, Store: &ociRecipeFlowCASStore{}, Driver: ociservice.NewDriver(flow.resolver, driftHost)}
	if _, err := driftExecutor.Execute(context.Background(), driftedManifest); err == nil || driftHost.callCount() != 0 {
		t.Fatalf("artifact drift reached host: err=%v calls=%v", err, driftHost.calls)
	}

	catalogDrift := flow.catalog
	catalogDrift.Entries[0].BundleDigest = ociRecipeFlowDigest("7")
	if _, err := recipeexec.NewOCICatalogResolver(catalogDrift, flow.workerManifest, flow.workerManifestDigest, flow.workerManifest.WorkerBinaryDigest); err == nil {
		t.Fatal("catalog digest drift was accepted")
	}
	if _, err := recipeexec.NewOCICatalogResolver(flow.catalog, flow.workerManifest, ociRecipeFlowDigest("6"), flow.workerManifest.WorkerBinaryDigest); err == nil {
		t.Fatal("approved Worker resource manifest drift was accepted")
	}
	if driftHost.callCount() != 0 {
		t.Fatalf("manifest/catalog drift reached host: %v", driftHost.calls)
	}
}

type ociRecipeFlow struct {
	resolver             *recipeexec.OCICatalogResolver
	catalog              recipeexec.WorkerOCICatalogV1
	workerManifest       recipeexec.WorkerResourceManifestV1
	workerManifestDigest string
	manifest             cloudorchestrator.RecipeExecutionManifestV1
	bundle               cloudorchestrator.OCIServiceBundleV1
}

func newOCIRecipeFlow(t *testing.T) ociRecipeFlow {
	t.Helper()
	recipe, config := ociRecipeFlowCompilerInput()
	_, preliminaryBundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatalf("compile preliminary bundle: %v", err)
	}
	bundleDigest, err := preliminaryBundle.Digest()
	if err != nil {
		t.Fatalf("bundle digest: %v", err)
	}
	actionIDs := make([]string, len(preliminaryBundle.Actions))
	for index := range preliminaryBundle.Actions {
		actionIDs[index] = preliminaryBundle.Actions[index].ActionID
	}
	catalog := recipeexec.WorkerOCICatalogV1{
		SchemaVersion: recipeexec.WorkerOCICatalogV1Schema,
		Entries: []recipeexec.WorkerOCICatalogEntryV1{{
			ArtifactDigest: preliminaryBundle.ArtifactDigest,
			BundleDigest:   bundleDigest,
			ActionIDs:      actionIDs,
			Descriptor:     preliminaryBundle,
		}},
	}
	catalogDigest, err := catalog.Digest()
	if err != nil {
		t.Fatalf("catalog digest: %v", err)
	}
	workerManifest := recipeexec.WorkerResourceManifestV1{
		SchemaVersion:      recipeexec.WorkerResourceManifestV1Schema,
		WorkerBinaryDigest: ociRecipeFlowDigest("9"),
		CatalogDigest:      catalogDigest,
		RuntimeIdentity:    recipeexec.WorkerRuntimeIdentityPodmanV1,
	}
	workerManifestDigest, err := workerManifest.Digest()
	if err != nil {
		t.Fatalf("Worker resource manifest digest: %v", err)
	}

	config.WorkerResourceManifestDigest = workerManifestDigest
	artifact, bundle, err := recipecompiler.CompileOCIServiceBundle(recipe, config)
	if err != nil {
		t.Fatalf("compile approved artifact: %v", err)
	}
	finalBundleDigest, err := bundle.Digest()
	if err != nil || finalBundleDigest != bundleDigest {
		t.Fatalf("Worker manifest changed OCI bundle: digest=%q err=%v want=%q", finalBundleDigest, err, bundleDigest)
	}
	catalog.Entries[0].Descriptor = bundle
	resolver, err := recipeexec.NewOCICatalogResolver(catalog, workerManifest, workerManifestDigest, workerManifest.WorkerBinaryDigest)
	if err != nil {
		t.Fatalf("new OCI catalog resolver: %v", err)
	}
	install, ok := bundle.Action(cloudorchestrator.CompiledRecipeActionInstall)
	if !ok {
		t.Fatal("compiled bundle has no install action")
	}
	manifest := cloudorchestrator.RecipeExecutionManifestV1{
		SchemaVersion:                cloudorchestrator.RecipeExecutionManifestV1Schema,
		ExecutionID:                  "execution-oci-flow-0001",
		DeploymentID:                 "deployment-oci-flow-0001",
		PlanID:                       "plan-oci-flow-0001",
		PlanHash:                     ociRecipeFlowDigest("a"),
		PlanRevision:                 1,
		RecipeDigest:                 artifact.RecipeDigest,
		WorkerResourceManifestDigest: workerManifestDigest,
		ArtifactDigest:               artifact.ArtifactDigest,
		ActionID:                     install.ActionID,
		RootRequired:                 install.RootRequired,
		TimeoutSeconds:               install.TimeoutSeconds,
		CheckpointSequence:           append([]string(nil), install.CheckpointSequence...),
		SemanticReadiness:            artifact.SemanticReadiness,
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("execution manifest: %v", err)
	}
	return ociRecipeFlow{
		resolver: resolver, catalog: catalog, workerManifest: workerManifest, workerManifestDigest: workerManifestDigest,
		manifest: manifest, bundle: bundle,
	}
}

func ociRecipeFlowCompilerInput() (cloudorchestrator.RecipeV1, recipecompiler.Config) {
	health := cloudorchestrator.HealthContractV1{
		Liveness:  cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/live"},
		Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"},
		Semantic:  cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/semantic"},
	}
	lifecycle := cloudorchestrator.LifecycleContractV1{
		Start: "service_start_v1", Stop: "service_stop_v1", Restart: "service_restart_v1", Upgrade: "service_upgrade_v1",
		Rollback: "service_rollback_v1", Backup: "service_backup_v1", Restore: "service_restore_v1", Destroy: "service_destroy_v1",
	}
	checkpoints := []string{
		ociservice.CheckpointArtifactVerified,
		ociservice.CheckpointContainerCreated,
		ociservice.CheckpointContainerStarted,
		ociservice.CheckpointHealthVerified,
	}
	recipe := cloudorchestrator.RecipeV1{
		SchemaVersion: cloudorchestrator.SchemaVersionV1, RecipeID: "recipe-oci-flow-0001", Name: "Verified OCI flow", Maturity: cloudorchestrator.RecipeExperimental,
		Sources: []cloudorchestrator.RecipeSourceV1{{
			URL: "https://github.com/example/verified-service", Version: "v1.2.3", Commit: "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: ociRecipeFlowDigest("1"), License: "Apache-2.0", RetrievedAt: time.Date(2026, time.July, 15, 0, 0, 0, 0, time.UTC), Official: true,
		}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{MinVCPU: 2, MinMemoryMiB: 4096, MinDiskGiB: 40, Architecture: cloudorchestrator.ArchitectureAMD64},
		Install: cloudorchestrator.InstallContractV1{
			RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: checkpoints,
			Steps: []cloudorchestrator.InstallStepV1{{ID: "install_service", Summary: "Install verified OCI service", TimeoutSeconds: 1800}},
		},
		Health: health, Lifecycle: lifecycle,
	}
	probe := func(port uint16, path, body string) cloudorchestrator.OCIServiceLoopbackProbeV1 {
		return cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: port, Path: path, ExpectedStatus: 200, BodySHA256: ociRecipeFlowDigest(body)}
	}
	return recipe, recipecompiler.Config{
		RecipeRevision: 1, ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("public.ecr.aws/dirextalk/flow-service@" + ociRecipeFlowDigest("2")), ImageDigest: ociRecipeFlowDigest("2"), ImageSizeBytes: 1048576, Architecture: cloudorchestrator.ArchitectureAMD64,
		WorkerResourceManifestDigest: ociRecipeFlowDigest("3"), HealthContract: health, LifecycleContract: lifecycle,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{{
			Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "service_install_v1", RootRequired: true, TimeoutSeconds: 1800,
			CheckpointSequence: append([]string(nil), checkpoints...),
		}},
		Health: cloudorchestrator.OCIServiceHealthV1{
			Liveness: probe(8080, "/live", "4"), Readiness: probe(8080, "/ready", "5"), Semantic: probe(8081, "/semantic", "6"),
		},
	}
}

type ociRecipeFlowCASStore struct {
	mu          sync.Mutex
	initialized bool
	state       recipeexec.CheckpointState
	history     []string
}

func (store *ociRecipeFlowCASStore) Load(ctx context.Context, binding recipeexec.Binding) (recipeexec.CheckpointState, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return recipeexec.CheckpointState{}, err
	}
	if !store.initialized {
		store.state = recipeexec.InitialCheckpointState(binding)
		store.initialized = true
	}
	return store.state, nil
}

func (store *ociRecipeFlowCASStore) Advance(ctx context.Context, previous, next recipeexec.CheckpointState) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.state != previous {
		return recipeexec.ErrCheckpointConflict
	}
	store.state = next
	store.history = append(store.history, next.Checkpoint)
	return nil
}

func (store *ociRecipeFlowCASStore) checkpoints() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]string(nil), store.history...)
}

type ociRecipeFlowHost struct {
	mu     sync.Mutex
	calls  []string
	specs  []ociservice.ContainerSpec
	probes []cloudorchestrator.OCIServiceLoopbackProbeV1
}

func (*ociRecipeFlowHost) EffectiveUID() int { return 0 }

func (host *ociRecipeFlowHost) EnsurePinnedImage(_ context.Context, _ cloudorchestrator.OCIImageSourceReferenceV1, _ string) error {
	host.record("ensure-image")
	return nil
}

func (host *ociRecipeFlowHost) EnsureContainer(_ context.Context, spec ociservice.ContainerSpec) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.calls = append(host.calls, "ensure")
	host.specs = append(host.specs, spec)
	return nil
}

func (host *ociRecipeFlowHost) RefreshServiceSecrets(_ context.Context, spec ociservice.ContainerSpec) error {
	host.calls = append(host.calls, "refresh")
	host.specs = append(host.specs, spec)
	return nil
}

func (host *ociRecipeFlowHost) StartContainer(_ context.Context, _ string) error {
	host.record("start")
	return nil
}

func (host *ociRecipeFlowHost) StopContainer(_ context.Context, _ string) error {
	host.record("stop")
	return nil
}

func (host *ociRecipeFlowHost) RemoveContainer(_ context.Context, _ string) error {
	host.record("remove")
	return nil
}

func (host *ociRecipeFlowHost) ProbeLoopback(_ context.Context, probe cloudorchestrator.OCIServiceLoopbackProbeV1) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.calls = append(host.calls, "probe:"+probe.Path)
	host.probes = append(host.probes, probe)
	return nil
}

func (host *ociRecipeFlowHost) record(call string) {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.calls = append(host.calls, call)
}

func (host *ociRecipeFlowHost) callCount() int {
	host.mu.Lock()
	defer host.mu.Unlock()
	return len(host.calls)
}

func ociRecipeFlowDigest(character string) string {
	return "sha256:" + strings.Repeat(character, 64)
}
