package ociservice

import (
	"context"
	"errors"
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
func (host *testHost) VerifyPinnedImage(_ context.Context, digest string) error {
	host.calls = append(host.calls, "verify:"+digest)
	return nil
}
func (host *testHost) EnsureContainer(_ context.Context, spec ContainerSpec) error {
	host.calls = append(host.calls, "ensure:"+spec.Name)
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
	if len(resumedHost.specs) != 0 || !reflect.DeepEqual(resumedReporter.checkpoints, []string{CheckpointContainerStarted, CheckpointHealthVerified}) {
		t.Fatalf("resume repeated create: calls=%v checkpoints=%v", resumedHost.calls, resumedReporter.checkpoints)
	}
	if !strings.HasPrefix(resumedHost.calls[0], "start:"+created.Name) {
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

func validBundle() cloudorchestrator.OCIServiceBundleV1 {
	probe := func(port uint16, path string, body byte) cloudorchestrator.OCIServiceLoopbackProbeV1 {
		return cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: port, Path: path, ExpectedStatus: 200, BodySHA256: digest(string(body))}
	}
	return cloudorchestrator.OCIServiceBundleV1{
		SchemaVersion: cloudorchestrator.OCIServiceBundleV1Schema, ArtifactDigest: digest("a"), ImageDigest: digest("a"), ImageSizeBytes: 1024,
		Architecture:         cloudorchestrator.ArchitectureAMD64,
		Actions:              []cloudorchestrator.CompiledRecipeActionV1{{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "install_oci_service", RootRequired: true, TimeoutSeconds: 90, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()}},
		Health:               cloudorchestrator.OCIServiceHealthV1{Liveness: probe(8080, "/live", 'b'), Readiness: probe(8080, "/ready", 'c'), Semantic: probe(8081, "/semantic", 'd')},
		HealthContractDigest: digest("e"), LifecycleContractDigest: digest("f"),
	}
}

func validRequest(bundle cloudorchestrator.OCIServiceBundleV1) recipeexec.ActionRequest {
	return recipeexec.ActionRequest{
		Binding: recipeexec.Binding{ExecutionID: "execution-oci-0001", ManifestDigest: digest("1")}, Artifact: recipeexec.Bundle{ArtifactDigest: bundle.ArtifactDigest, ActionIDs: []string{"install_oci_service"}},
		DeploymentID: "deployment-oci-0001", ActionID: "install_oci_service", RootRequired: true, Timeout: 90 * time.Second,
	}
}

func digest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
