package ociservice

import (
	"context"
	"errors"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

type testResolver struct {
	bundle cloudorchestrator.OCIServiceBundleV1
	err    error
}

func (resolver testResolver) LookupDescriptor(context.Context, string) (cloudorchestrator.OCIServiceBundleV1, error) {
	return resolver.bundle, resolver.err
}

type testHost struct {
	uid             int
	calls           []string
	specs           []ContainerSpec
	probes          []cloudorchestrator.OCIServiceLoopbackProbeV1
	probeFail       int
	probeAlwaysFail bool
}

func (host *testHost) EffectiveUID() int { return host.uid }
func (host *testHost) EnsurePinnedImage(_ context.Context, source cloudorchestrator.OCIImageSourceReferenceV1, digest string) error {
	host.calls = append(host.calls, "ensure-image:"+string(source)+":"+digest)
	return nil
}
func (host *testHost) EnsureContainer(_ context.Context, spec ContainerSpec) error {
	host.calls = append(host.calls, "ensure:"+spec.Name)
	host.specs = append(host.specs, spec)
	return nil
}
func (host *testHost) RefreshServiceSecrets(_ context.Context, spec ContainerSpec) error {
	host.calls = append(host.calls, "refresh:"+spec.Name)
	host.specs = append(host.specs, spec)
	return nil
}
func (host *testHost) StartContainer(_ context.Context, name string) error {
	host.calls = append(host.calls, "start:"+name)
	return nil
}
func (host *testHost) StopContainer(_ context.Context, name string) error {
	host.calls = append(host.calls, "stop:"+name)
	return nil
}
func (host *testHost) RemoveContainer(_ context.Context, name string) error {
	host.calls = append(host.calls, "remove:"+name)
	return nil
}
func (host *testHost) ProbeLoopback(_ context.Context, probe cloudorchestrator.OCIServiceLoopbackProbeV1) error {
	host.calls = append(host.calls, "probe:"+probe.Path)
	host.probes = append(host.probes, probe)
	if host.probeAlwaysFail || host.probeFail > 0 && len(host.probes) == host.probeFail {
		return errors.New("not ready")
	}
	return nil
}

type testReporter struct{ checkpoints []string }

func (reporter *testReporter) Checkpoint(_ context.Context, checkpoint string) error {
	reporter.checkpoints = append(reporter.checkpoints, checkpoint)
	return nil
}

func TestDriverInstallIsTypedAndRestartable(t *testing.T) {
	bundle := validBundle()
	request := validRequest(bundle)
	host, reporter := &testHost{}, &testReporter{}
	driver := NewDriver(testResolver{bundle: bundle}, host)
	if err := driver.Execute(context.Background(), request, reporter); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reporter.checkpoints, cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()) {
		t.Fatalf("checkpoints=%v", reporter.checkpoints)
	}
	if len(host.specs) != 1 || len(host.probes) != 3 || host.probes[0] != bundle.Health.Liveness || host.probes[1] != bundle.Health.Readiness || host.probes[2] != bundle.Health.Semantic {
		t.Fatalf("typed host boundary specs=%#v probes=%#v", host.specs, host.probes)
	}
	created := host.specs[0]
	if created.Name == "" || created.ImageDigest != bundle.ImageDigest || !reflect.DeepEqual(created.LoopbackPorts, []uint16{8080, 8081}) {
		t.Fatalf("container spec=%#v", created)
	}

	resumedHost, resumedReporter := &testHost{}, &testReporter{}
	request.ResumeAfter = CheckpointContainerCreated
	if err := NewDriver(testResolver{bundle: bundle}, resumedHost).Execute(context.Background(), request, resumedReporter); err != nil {
		t.Fatal(err)
	}
	if len(resumedHost.specs) != 1 || !reflect.DeepEqual(resumedReporter.checkpoints, []string{CheckpointContainerStarted, CheckpointHealthVerified}) {
		t.Fatalf("resume did not revalidate deterministic container: calls=%v checkpoints=%v", resumedHost.calls, resumedReporter.checkpoints)
	}
	if resumedHost.specs[0].BindingDigest != created.BindingDigest || !strings.HasPrefix(resumedHost.calls[1], "start:"+created.Name) {
		t.Fatalf("deterministic name drift: fresh=%q resumed calls=%v", created.Name, resumedHost.calls)
	}

	changed := validRequest(bundle)
	changed.DeploymentID = "deployment-oci-0002"
	changedHost := &testHost{}
	if err := NewDriver(testResolver{bundle: bundle}, changedHost).Execute(context.Background(), changed, &testReporter{}); err != nil {
		t.Fatal(err)
	}
	if changedHost.specs[0].Name == created.Name {
		t.Fatal("container name did not bind deployment")
	}
}

func TestDriverExecutesOnlyTypedLifecycleActionsWithStableContainerIdentity(t *testing.T) {
	bundle := validBundle()
	wantCalls := map[cloudorchestrator.CompiledRecipeActionKind][]string{
		cloudorchestrator.CompiledRecipeActionInstall: {"ensure-image:", "ensure:", "start:", "probe:/live", "probe:/ready", "probe:/semantic"},
		cloudorchestrator.CompiledRecipeActionStart:   {"refresh:", "start:", "probe:/live", "probe:/ready", "probe:/semantic"},
		cloudorchestrator.CompiledRecipeActionStop:    {"stop:"},
		cloudorchestrator.CompiledRecipeActionRestart: {"refresh:", "stop:", "start:", "probe:/live", "probe:/ready", "probe:/semantic"},
	}
	var installSpec ContainerSpec
	for _, kind := range []cloudorchestrator.CompiledRecipeActionKind{cloudorchestrator.CompiledRecipeActionInstall, cloudorchestrator.CompiledRecipeActionStart, cloudorchestrator.CompiledRecipeActionStop, cloudorchestrator.CompiledRecipeActionRestart} {
		t.Run(string(kind), func(t *testing.T) {
			request := requestForAction(bundle, kind)
			host, reporter := &testHost{}, &testReporter{}
			if err := NewDriver(testResolver{bundle: bundle}, host).Execute(context.Background(), request, reporter); err != nil {
				t.Fatal(err)
			}
			expectedCheckpoints, _ := cloudorchestrator.OCIServiceActionCheckpointSequenceV1(kind)
			if !reflect.DeepEqual(reporter.checkpoints, expectedCheckpoints) || len(host.calls) != len(wantCalls[kind]) {
				t.Fatalf("calls=%v checkpoints=%v", host.calls, reporter.checkpoints)
			}
			for index, prefix := range wantCalls[kind] {
				if !strings.HasPrefix(host.calls[index], prefix) {
					t.Fatalf("call[%d]=%q want prefix %q", index, host.calls[index], prefix)
				}
			}
			if kind == cloudorchestrator.CompiledRecipeActionInstall {
				installSpec = host.specs[0]
			}
			if kind == cloudorchestrator.CompiledRecipeActionStart {
				if len(host.specs) != 1 || host.specs[0].Name != installSpec.Name || host.specs[0].BindingDigest != installSpec.BindingDigest {
					t.Fatalf("operation created a second identity: install=%#v operation=%#v", installSpec, host.specs)
				}
			}
		})
	}

	resumed := requestForAction(bundle, cloudorchestrator.CompiledRecipeActionRestart)
	resumed.ResumeAfter = CheckpointContainerStopped
	host, reporter := &testHost{}, &testReporter{}
	if err := NewDriver(testResolver{bundle: bundle}, host).Execute(context.Background(), resumed, reporter); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(host.calls, ","), "stop:") || !reflect.DeepEqual(reporter.checkpoints, []string{CheckpointContainerStarted, CheckpointHealthVerified}) {
		t.Fatalf("restart recovery calls=%v checkpoints=%v", host.calls, reporter.checkpoints)
	}
}

func TestDriverDerivesFixedStorageMountsWithoutUsingOpaqueRefsAsPaths(t *testing.T) {
	bundle := validBundle()
	bundle.VolumeTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service", ReadOnly: false}}
	bundle.DataTargets = []cloudorchestrator.OCIServiceStorageTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/service/knowledge", ReadOnly: true}}
	request := validRequest(bundle)
	request.VolumeSlots = []cloudorchestrator.VolumeSlotV1{{SlotID: "state", VolumeRef: "volume_ref:opaque/state-v1", ReadOnly: false}}
	request.DataSlots = []cloudorchestrator.DataSlotV1{{SlotID: "knowledge", DataRef: "data_ref:opaque/knowledge-v1", ReadOnly: true}}
	host := &testHost{}
	if err := NewDriver(testResolver{bundle: bundle}, host).Execute(context.Background(), request, &testReporter{}); err != nil {
		t.Fatal(err)
	}
	if len(host.specs) != 1 || len(host.specs[0].StorageMounts) != 2 {
		t.Fatalf("specs=%#v", host.specs)
	}
	mounts := host.specs[0].StorageMounts
	if mounts[0].Target != "/opt/service/knowledge" || !mounts[0].ReadOnly || mounts[1].Target != "/var/lib/service" || mounts[1].ReadOnly {
		t.Fatalf("mounts=%#v", mounts)
	}
	for _, mount := range mounts {
		if !strings.HasPrefix(mount.Source, StorageRoot+"/") || strings.Contains(mount.Source, "opaque") || strings.Contains(mount.Source, "volume_ref") || strings.Contains(mount.Source, "data_ref") {
			t.Fatalf("opaque reference escaped into host path: %#v", mount)
		}
	}
	readOnlyDrift := request
	readOnlyDrift.DataSlots = append([]cloudorchestrator.DataSlotV1(nil), request.DataSlots...)
	readOnlyDrift.DataSlots[0].ReadOnly = false
	driftHost := &testHost{}
	err := NewDriver(testResolver{bundle: bundle}, driftHost).Execute(context.Background(), readOnlyDrift, &testReporter{})
	if !recipeexec.IsPermanentExecutionFailure(err) || len(driftHost.calls) != 0 {
		t.Fatalf("read-only drift err=%v calls=%v", err, driftHost.calls)
	}
}

func TestDriverRejectsUnapprovedOrMutableScopeBeforeHostMutation(t *testing.T) {
	bundle := validBundle()
	tests := []struct {
		name   string
		mutate func(*recipeexec.ActionRequest, *testHost, *testResolver)
	}{
		{"root approval", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) { request.RootRequired = false }},
		{"non-root host", func(_ *recipeexec.ActionRequest, host *testHost, _ *testResolver) { host.uid = 1000 }},
		{"action", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) {
			request.ActionID = "start_service"
		}},
		{"artifact", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) {
			request.Artifact.ArtifactDigest = digest("f")
		}},
		{"timeout", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) { request.Timeout++ }},
		{"volume", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) {
			request.VolumeSlots = []cloudorchestrator.VolumeSlotV1{{SlotID: "volume-slot", VolumeRef: "volume_ref:data"}}
		}},
		{"resume", func(request *recipeexec.ActionRequest, _ *testHost, _ *testResolver) {
			request.ResumeAfter = "attacker_checkpoint"
		}},
		{"resolver", func(_ *recipeexec.ActionRequest, _ *testHost, resolver *testResolver) {
			resolver.err = errors.New("missing")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, host, resolver := validRequest(bundle), &testHost{}, testResolver{bundle: bundle}
			test.mutate(&request, host, &resolver)
			err := NewDriver(resolver, host).Execute(context.Background(), request, &testReporter{})
			if !recipeexec.IsPermanentExecutionFailure(err) || len(host.calls) != 0 {
				t.Fatalf("err=%v host calls=%v", err, host.calls)
			}
		})
	}
}

func TestDriverRetriesHealthContractsInOrderBeforeTerminalCheckpoint(t *testing.T) {
	bundle := validBundle()
	host, reporter := &testHost{probeFail: 2}, &testReporter{}
	driver := NewDriver(testResolver{bundle: bundle}, host)
	waits := 0
	driver.waitHealthRetry = func(context.Context, time.Duration) error { waits++; return nil }
	if err := driver.Execute(context.Background(), validRequest(bundle), reporter); err != nil {
		t.Fatal(err)
	}
	if waits != 1 || len(host.probes) != 5 || host.probes[2] != bundle.Health.Liveness || reporter.checkpoints[len(reporter.checkpoints)-1] != CheckpointHealthVerified {
		t.Fatalf("waits=%d probes=%v checkpoints=%v", waits, host.probes, reporter.checkpoints)
	}
}

func TestDriverDoesNotCheckpointPersistentHealthFailure(t *testing.T) {
	bundle := validBundle()
	host, reporter := &testHost{probeAlwaysFail: true}, &testReporter{}
	driver := NewDriver(testResolver{bundle: bundle}, host)
	driver.waitHealthRetry = func(context.Context, time.Duration) error { return context.DeadlineExceeded }
	err := driver.Execute(context.Background(), validRequest(bundle), reporter)
	healthCheckpointed := false
	for _, checkpoint := range reporter.checkpoints {
		healthCheckpointed = healthCheckpointed || checkpoint == CheckpointHealthVerified
	}
	if !errors.Is(err, context.DeadlineExceeded) || healthCheckpointed {
		t.Fatalf("err=%v probes=%v checkpoints=%v", err, host.probes, reporter.checkpoints)
	}
}

func TestDriverConsumesOnlyFixedStagedSecretDestinations(t *testing.T) {
	bundle := validBundle()
	request := validRequest(bundle)
	request.Artifact.SecretTargets = []recipeexec.SecretTarget{{SlotID: "api-token", FileName: "api-token"}, {SlotID: "model-token", FileName: "model-token"}}
	request.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model"}, {SlotID: "api-token", SecretRef: "secret_ref:api"}}
	directory := SecretStagingRoot + "/" + request.DeploymentID + "-" + request.Binding.ExecutionID
	request.Secrets = recipeexec.SecretDelivery{StagingDirectory: directory, Files: map[string]string{"api-token": directory + "/api-token", "model-token": directory + "/model-token"}}
	host := &testHost{}
	driver := NewDriver(testResolver{bundle: bundle}, host)
	driver.secretValidator = func(recipeexec.SecretDelivery) error { return nil }
	if err := driver.Execute(context.Background(), request, &testReporter{}); err != nil {
		t.Fatal(err)
	}
	spec := host.specs[0]
	if len(spec.SecretMounts) != 2 || spec.SecretMounts[0].Target != "/run/secrets/api-token" || spec.SecretMounts[1].Target != "/run/secrets/model-token" {
		t.Fatalf("secret spec=%#v", spec)
	}
	for _, mount := range spec.SecretMounts {
		if !strings.HasPrefix(mount.StagedSource, SecretStagingRoot+"/") || !strings.HasPrefix(mount.StableSource, ServiceSecretRoot+"/") || strings.Contains(mount.StableSource, request.DeploymentID) || path.Base(mount.StableSource) != path.Base(mount.Target) {
			t.Fatalf("secret path escaped fixed tmpfs roots: %#v", mount)
		}
	}
	stop := requestForAction(bundle, cloudorchestrator.CompiledRecipeActionStop)
	stop.Artifact.SecretTargets, stop.SecretSlots = request.Artifact.SecretTargets, request.SecretSlots
	stopHost := &testHost{}
	if err := NewDriver(testResolver{bundle: bundle}, stopHost).Execute(context.Background(), stop, &testReporter{}); err != nil || !reflect.DeepEqual(stopHost.calls, []string{"stop:" + spec.Name}) {
		t.Fatalf("stop refreshed service secrets: err=%v calls=%v", err, stopHost.calls)
	}

	for name, mutate := range map[string]func(*recipeexec.ActionRequest){
		"outside root": func(value *recipeexec.ActionRequest) { value.Secrets.Files["api-token"] = "/tmp/api-token" },
		"environment":  func(value *recipeexec.ActionRequest) { value.Secrets.EnvironmentFile = directory + "/environment" },
		"extra file":   func(value *recipeexec.ActionRequest) { value.Secrets.Files["other"] = directory + "/other" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := request
			candidate.Secrets.Files = map[string]string{"api-token": request.Secrets.Files["api-token"], "model-token": request.Secrets.Files["model-token"]}
			mutate(&candidate)
			candidateHost := &testHost{}
			candidateDriver := NewDriver(testResolver{bundle: bundle}, candidateHost)
			candidateDriver.secretValidator = func(recipeexec.SecretDelivery) error { return nil }
			err := candidateDriver.Execute(context.Background(), candidate, &testReporter{})
			if !recipeexec.IsPermanentExecutionFailure(err) || len(candidateHost.calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, candidateHost.calls)
			}
		})
	}
}

func TestDriverBindsSecretEnvironmentToCompilerOwnedFileSlot(t *testing.T) {
	bundle := validBundle()
	bundle.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint: "/usr/bin/tini", Argv: []string{"-s", "--", "node", "openclaw.mjs", "gateway"},
		SecretEnvironment: []cloudorchestrator.OCIServiceSecretEnvironmentV1{{SlotID: "model-token", EnvironmentKey: "OPENAI_API_KEY"}},
		RunAs:             &cloudorchestrator.OCIServiceRunAsV1{UID: 1000, GID: 1000},
		Capabilities:      []cloudorchestrator.OCIServiceCapability{cloudorchestrator.OCIServiceCapabilitySetGID, cloudorchestrator.OCIServiceCapabilitySetUID},
		SecretReadGID:     1000,
	}
	request := validRequest(bundle)
	request.Artifact.SecretTargets = []recipeexec.SecretTarget{{SlotID: "model-token", FileName: "model-token"}}
	request.SecretSlots = []cloudorchestrator.SecretSlotV1{{SlotID: "model-token", SecretRef: "secret_ref:model"}}
	directory := SecretStagingRoot + "/" + request.DeploymentID + "-" + request.Binding.ExecutionID
	request.Secrets = recipeexec.SecretDelivery{StagingDirectory: directory, Files: map[string]string{"model-token": directory + "/model-token"}}
	host := &testHost{}
	driver := NewDriver(testResolver{bundle: bundle}, host)
	driver.secretValidator = func(recipeexec.SecretDelivery) error { return nil }
	if err := driver.Execute(context.Background(), request, &testReporter{}); err != nil {
		t.Fatal(err)
	}
	if len(host.specs) != 1 || !reflect.DeepEqual(host.specs[0].SecretEnvironment, []ContainerSecretEnvironment{{EnvironmentKey: "OPENAI_API_KEY", FilePath: "/run/secrets/model-token"}}) || !reflect.DeepEqual(host.specs[0].RuntimeProfile, bundle.RuntimeProfile) {
		t.Fatalf("container spec=%#v", host.specs)
	}

	tampered := request
	tampered.Artifact.RuntimeProfile = cloudorchestrator.CloneOCIServiceRuntimeProfileV1(request.Artifact.RuntimeProfile)
	tampered.Artifact.RuntimeProfile.SecretEnvironment[0].EnvironmentKey = "ATTACKER_TOKEN"
	tamperedHost := &testHost{}
	if err := NewDriver(testResolver{bundle: bundle}, tamperedHost).Execute(context.Background(), tampered, &testReporter{}); !recipeexec.IsPermanentExecutionFailure(err) || len(tamperedHost.calls) != 0 {
		t.Fatalf("tampered profile err=%v calls=%v", err, tamperedHost.calls)
	}
}

func validBundle() cloudorchestrator.OCIServiceBundleV1 {
	probe := func(port uint16, path string, body byte) cloudorchestrator.OCIServiceLoopbackProbeV1 {
		return cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: port, Path: path, ExpectedStatus: 200, BodySHA256: digest(string(body))}
	}
	return cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: digest("a"), ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/test-service@" + digest("a")), ImageDigest: digest("a"), ImageSizeBytes: 1024,
		Architecture: cloudorchestrator.ArchitectureAMD64,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{
			{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "install_oci_service", RootRequired: true, TimeoutSeconds: 90, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionStart, ActionID: "start_oci_service", RootRequired: true, TimeoutSeconds: 30, CheckpointSequence: cloudorchestrator.OCIServiceStartCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionStop, ActionID: "stop_oci_service", RootRequired: true, TimeoutSeconds: 30, CheckpointSequence: cloudorchestrator.OCIServiceStopCheckpointSequenceV1()},
			{Kind: cloudorchestrator.CompiledRecipeActionRestart, ActionID: "restart_oci_service", RootRequired: true, TimeoutSeconds: 60, CheckpointSequence: cloudorchestrator.OCIServiceRestartCheckpointSequenceV1()},
		},
		Health:               cloudorchestrator.OCIServiceHealthV1{Liveness: probe(8080, "/live", 'b'), Readiness: probe(8080, "/ready", 'c'), Semantic: probe(8081, "/semantic", 'd')},
		HealthContractDigest: digest("e"), LifecycleContractDigest: digest("f"),
	}
}

func validRequest(bundle cloudorchestrator.OCIServiceBundleV1) recipeexec.ActionRequest {
	actionIDs := make([]string, len(bundle.Actions))
	for index, action := range bundle.Actions {
		actionIDs[index] = action.ActionID
	}
	return recipeexec.ActionRequest{
		Binding: recipeexec.Binding{ExecutionID: "execution-oci-0001", ManifestDigest: digest("1")}, Artifact: recipeexec.Bundle{ArtifactDigest: bundle.ArtifactDigest, ActionIDs: actionIDs, RuntimeProfile: cloudorchestrator.CloneOCIServiceRuntimeProfileV1(bundle.RuntimeProfile)},
		DeploymentID: "deployment-oci-0001", ActionID: "install_oci_service", RootRequired: true, Timeout: 90 * time.Second,
	}
}

func requestForAction(bundle cloudorchestrator.OCIServiceBundleV1, kind cloudorchestrator.CompiledRecipeActionKind) recipeexec.ActionRequest {
	request := validRequest(bundle)
	action, ok := bundle.Action(kind)
	if !ok {
		panic("missing test action " + string(kind))
	}
	request.Binding.ExecutionID = "execution-" + string(kind) + "-0001"
	manifestCharacter := map[cloudorchestrator.CompiledRecipeActionKind]string{
		cloudorchestrator.CompiledRecipeActionInstall: "2",
		cloudorchestrator.CompiledRecipeActionStart:   "3",
		cloudorchestrator.CompiledRecipeActionStop:    "4",
		cloudorchestrator.CompiledRecipeActionRestart: "5",
	}[kind]
	request.Binding.ManifestDigest = digest(manifestCharacter)
	request.ActionID = action.ActionID
	request.Timeout = time.Duration(action.TimeoutSeconds) * time.Second
	return request
}

func digest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
