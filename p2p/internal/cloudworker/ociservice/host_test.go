package ociservice

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

type testExitError int

func (value testExitError) Error() string { return "exit" }
func (value testExitError) ExitCode() int { return int(value) }

type runnerCall struct {
	executable string
	arguments  []string
	contextErr error
}

type captureRunner struct {
	calls []runnerCall
	run   func([]string) ([]byte, error)
}

func (runner *captureRunner) Run(ctx context.Context, executable string, arguments []string) ([]byte, error) {
	runner.calls = append(runner.calls, runnerCall{executable: executable, arguments: append([]string(nil), arguments...), contextErr: ctx.Err()})
	if runner.run != nil {
		return runner.run(arguments)
	}
	return nil, nil
}

func TestPodmanHostUsesPreloadedPinnedImageWithoutPull(t *testing.T) {
	imageDigest := digest("a")
	source := cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/test-service@" + imageDigest)
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		if len(arguments) > 2 && arguments[1] == "inspect" {
			return []byte(imageDigest), nil
		}
		return nil, nil
	}}
	if err := newPodmanHost(0, runner).EnsurePinnedImage(context.Background(), source, imageDigest); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 2 || strings.Contains(strings.Join(runner.calls[0].arguments, " "), "pull") || strings.Contains(strings.Join(runner.calls[1].arguments, " "), "pull") {
		t.Fatalf("preloaded image commands=%#v", runner.calls)
	}
}

func TestPodmanHostPullsOnlyPinnedPublicReferenceAndVerifiesRepoDigest(t *testing.T) {
	imageDigest := digest("a")
	source := cloudorchestrator.OCIImageSourceReferenceV1("quay.io/dirextalk/knowledge-node@" + imageDigest)
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		switch {
		case len(arguments) > 1 && arguments[1] == "exists":
			return nil, testExitError(1)
		case len(arguments) > 1 && arguments[1] == "pull":
			return nil, nil
		case len(arguments) > 2 && arguments[1] == "inspect" && strings.Contains(arguments[2], ".Digest"):
			return []byte(imageDigest), nil
		case len(arguments) > 2 && arguments[1] == "inspect" && strings.Contains(arguments[2], ".RepoDigests"):
			return []byte(string(source) + "\n"), nil
		default:
			return nil, errors.New("unexpected podman command")
		}
	}}
	if err := newPodmanHost(0, runner).EnsurePinnedImage(context.Background(), source, imageDigest); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 4 {
		t.Fatalf("pull commands=%#v", runner.calls)
	}
	pull := strings.Join(runner.calls[1].arguments, " ")
	if pull != "image pull --tls-verify=true -- "+string(source) || strings.Contains(pull, "--creds") || strings.Contains(pull, "--authfile") {
		t.Fatalf("unsafe pull arguments=%q", pull)
	}
	canceledContext, cancel := context.WithCancel(context.Background())
	runner.run = func(arguments []string) ([]byte, error) {
		switch {
		case len(arguments) > 1 && arguments[1] == "exists":
			return nil, testExitError(1)
		case len(arguments) > 1 && arguments[1] == "pull":
			return nil, nil
		case len(arguments) > 2 && arguments[1] == "inspect" && strings.Contains(arguments[2], ".Digest"):
			return []byte(imageDigest), nil
		case len(arguments) > 2 && arguments[1] == "inspect" && strings.Contains(arguments[2], ".RepoDigests"):
			cancel()
			return []byte("quay.io/dirextalk/other@" + imageDigest), nil
		default:
			return nil, errors.New("unexpected podman command")
		}
	}
	if err := newPodmanHost(0, runner).EnsurePinnedImage(canceledContext, source, imageDigest); !errors.Is(err, ErrDescriptorMismatch) {
		t.Fatalf("repo digest mismatch=%v", err)
	}
	lastCall := runner.calls[len(runner.calls)-1]
	last := lastCall.arguments
	if strings.Join(last, " ") != "image rm --force -- "+imageDigest {
		t.Fatalf("mismatched pulled image was retained: %#v", runner.calls)
	}
	if lastCall.contextErr != nil {
		t.Fatalf("cleanup reused canceled operation context: %v", lastCall.contextErr)
	}
}

func TestPodmanHostConstructsOnlyFixedShellFreeCreateArguments(t *testing.T) {
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		if len(arguments) >= 2 && arguments[0] == "container" && arguments[1] == "exists" {
			return nil, testExitError(1)
		}
		return nil, nil
	}}
	host := newPodmanHost(0, runner)
	spec := ContainerSpec{Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080, 8081}}
	if err := host.EnsureContainer(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 2 || runner.calls[1].executable != podmanPath {
		t.Fatalf("calls=%#v", runner.calls)
	}
	arguments := runner.calls[1].arguments
	joined := strings.Join(arguments, " ")
	for _, forbidden := range []string{"/bin/sh", "bash", " -c ", "--privileged", "--network=host", "docker.sock", "--volume"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("unsafe podman arguments: %s", joined)
		}
	}
	for _, required := range []string{"container create", "--network=bridge", "--read-only", "--cap-drop=all", "--security-opt=no-new-privileges", "--publish=127.0.0.1:8080:8080/tcp", "-- " + spec.ImageDigest} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %q in %s", required, joined)
		}
	}
}

func TestPodmanHostExistingContainerMustMatchDeterministicBinding(t *testing.T) {
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		if len(arguments) > 1 && arguments[1] == "inspect" {
			return []byte(digest("f")), nil
		}
		return nil, nil
	}}
	host := newPodmanHost(0, runner)
	err := host.EnsureContainer(context.Background(), ContainerSpec{Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080}})
	if !errors.Is(err, ErrContainerBinding) || len(runner.calls) != 2 {
		t.Fatalf("err=%v calls=%#v", err, runner.calls)
	}
}

func TestCreateArgumentsAcceptOnlyFixedSecretRootAndReadonlyTargets(t *testing.T) {
	stableDirectory := ServiceSecretRoot + "/" + strings.Repeat("1", 64)
	spec := ContainerSpec{
		Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080},
		RuntimeProfile: &cloudorchestrator.OCIServiceRuntimeProfileV1{Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "MODEL_TOKEN_FILE", Value: "/run/secrets/token"}}},
		SecretMounts:   []SecretMount{{StagedSource: SecretStagingRoot + "/deployment-execution/token", StableSource: stableDirectory + "/token", Target: "/run/secrets/token"}},
	}
	if err := validateContainerSpec(spec); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(createArguments(spec), " ")
	if strings.Contains(joined, "--env-file") || strings.Contains(joined, SecretStagingRoot) || !strings.Contains(joined, "source="+stableDirectory+",target=/run/secrets,readonly=true") || strings.Contains(joined, "docker.sock") {
		t.Fatalf("secret arguments=%s", joined)
	}
	outside := spec
	outside.SecretMounts = []SecretMount{{StagedSource: "/tmp/token", StableSource: stableDirectory + "/token", Target: "/run/secrets/token"}}
	if validateContainerSpec(outside) == nil {
		t.Fatal("outside secret root accepted")
	}
	unbound := spec
	unbound.RuntimeProfile = &cloudorchestrator.OCIServiceRuntimeProfileV1{Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "MODEL_TOKEN_FILE", Value: "/run/secrets/missing"}}}
	if validateContainerSpec(unbound) == nil {
		t.Fatal("unbound *_FILE environment accepted")
	}
}

func TestCreateArgumentsRunVerifiedOpenClawProfileThroughMeasuredInit(t *testing.T) {
	stableDirectory := ServiceSecretRoot + "/" + strings.Repeat("1", 64)
	profile := &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint:  "/usr/bin/tini",
		Argv:        []string{"-s", "--", "node", "openclaw.mjs", "gateway", "--bind", "lan", "--port", "18789", "--allow-unconfigured"},
		Environment: []cloudorchestrator.OCIServiceEnvironmentV1{{Name: "OPENCLAW_DISABLE_BONJOUR", Value: "1"}},
		SecretEnvironment: []cloudorchestrator.OCIServiceSecretEnvironmentV1{{
			SlotID: "model_token", EnvironmentKey: "OPENAI_API_KEY",
		}, {SlotID: "gateway_token", EnvironmentKey: "OPENCLAW_GATEWAY_TOKEN"}},
		RunAs:         &cloudorchestrator.OCIServiceRunAsV1{UID: 1000, GID: 1000},
		Tmpfs:         []cloudorchestrator.OCIServiceTmpfsV1{{ContainerTarget: "/tmp", SizeBytes: 64 << 20, Mode: 0o1777}},
		Capabilities:  []cloudorchestrator.OCIServiceCapability{cloudorchestrator.OCIServiceCapabilitySetGID, cloudorchestrator.OCIServiceCapabilitySetUID},
		SecretReadGID: 1000,
	}
	spec := ContainerSpec{
		Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: "sha256:165b4992f1b4b74ffdd7a02c887ba006f9f5dc951eca420eef573a8b233b543f", LoopbackPorts: []uint16{18789},
		RuntimeProfile: profile,
		StorageMounts:  []StorageMount{{Source: StorageRoot + "/" + strings.Repeat("2", 64) + "/volumes/" + strings.Repeat("3", 64), Target: "/home/node/.openclaw", OwnerUID: 1000, OwnerGID: 1000, DirectoryMode: 0o700}},
		SecretMounts: []SecretMount{
			{StagedSource: SecretStagingRoot + "/deployment-execution/gateway-token", StableSource: stableDirectory + "/gateway-token", Target: "/run/secrets/gateway-token"},
			{StagedSource: SecretStagingRoot + "/deployment-execution/model-token", StableSource: stableDirectory + "/model-token", Target: "/run/secrets/model-token"},
		},
		SecretEnvironment: []ContainerSecretEnvironment{{EnvironmentKey: "OPENAI_API_KEY", FilePath: "/run/secrets/model-token"}, {EnvironmentKey: "OPENCLAW_GATEWAY_TOKEN", FilePath: "/run/secrets/gateway-token"}},
	}
	if err := validateContainerSpec(spec); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(createArgumentsWithInit(spec, "/opt/dirextalk/cloud-worker"), " ")
	for _, required := range []string{
		"--read-only", "--cap-drop=all", "--cap-add=SETGID", "--cap-add=SETUID", "--security-opt=no-new-privileges",
		"--user=0:0", "source=/opt/dirextalk/cloud-worker,target=/run/dirextalk/container-init,readonly=true,relabel=shared", "--entrypoint=/run/dirextalk/container-init",
		"container-init --run-as=1000:1000 --secret-env=OPENAI_API_KEY=/run/secrets/model-token --secret-env=OPENCLAW_GATEWAY_TOKEN=/run/secrets/gateway-token -- /usr/bin/tini -s -- node openclaw.mjs gateway --bind lan --port 18789 --allow-unconfigured",
		"--env=OPENCLAW_DISABLE_BONJOUR=1", "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=67108864,mode=1777",
		"target=/home/node/.openclaw,readonly=false",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %q in %s", required, joined)
		}
	}
	for _, forbidden := range []string{"--env-file", "--env=OPENAI_API_KEY", "--env=OPENCLAW_GATEWAY_TOKEN", "sk-", "--privileged", "--user=1000:1000", SecretStagingRoot} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("forbidden %q in %s", forbidden, joined)
		}
	}
}

func TestCreateArgumentsPreserveVerifiedHermesRootInit(t *testing.T) {
	profile := &cloudorchestrator.OCIServiceRuntimeProfileV1{
		Entrypoint: "/init", Argv: []string{"/opt/hermes/docker/main-wrapper.sh", "gateway", "run"},
		Tmpfs: []cloudorchestrator.OCIServiceTmpfsV1{
			{ContainerTarget: "/run", SizeBytes: 64 << 20, Mode: 0o755},
			{ContainerTarget: "/tmp", SizeBytes: 256 << 20, Mode: 0o1777},
		},
		Capabilities: []cloudorchestrator.OCIServiceCapability{
			cloudorchestrator.OCIServiceCapabilityChown,
			cloudorchestrator.OCIServiceCapabilityDACOverride,
			cloudorchestrator.OCIServiceCapabilityFOwner,
			cloudorchestrator.OCIServiceCapabilitySetGID,
			cloudorchestrator.OCIServiceCapabilitySetUID,
		},
	}
	spec := ContainerSpec{
		Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: "sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a", LoopbackPorts: []uint16{8080}, RuntimeProfile: profile,
		StorageMounts: []StorageMount{{Source: StorageRoot + "/" + strings.Repeat("2", 64) + "/volumes/" + strings.Repeat("3", 64), Target: "/opt/data", OwnerUID: 10000, OwnerGID: 10000, DirectoryMode: 0o700}},
	}
	if err := validateContainerSpec(spec); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(createArguments(spec), " ")
	for _, required := range []string{"--entrypoint=/init", "--cap-drop=all", "--cap-add=CHOWN", "--cap-add=DAC_OVERRIDE", "--cap-add=FOWNER", "--cap-add=SETGID", "--cap-add=SETUID", "--tmpfs=/run:rw,noexec,nosuid,nodev,size=67108864,mode=755", "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=268435456,mode=1777", "-- sha256:3db34ce19adfa080736a2a3feb0316dbcccc588faa9afe7fd8ae1c03b4f1a53a /opt/hermes/docker/main-wrapper.sh gateway run"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("missing %q in %s", required, joined)
		}
	}
	if strings.Contains(joined, "/bin/sh") || strings.Contains(joined, "--privileged") || strings.Contains(joined, "--user=") {
		t.Fatalf("unsafe Hermes arguments=%s", joined)
	}
}

func TestPodmanHostRefreshesStableSecretsBeforeContainerCreate(t *testing.T) {
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		if len(arguments) >= 2 && arguments[0] == "container" && arguments[1] == "exists" {
			return nil, testExitError(1)
		}
		return nil, nil
	}}
	host := newPodmanHost(0, runner)
	refreshes := 0
	host.refreshSecrets = func(spec ContainerSpec) error {
		refreshes++
		if len(spec.SecretMounts) != 1 || spec.SecretMounts[0].StagedSource == spec.SecretMounts[0].StableSource {
			return errors.New("secret binding drift")
		}
		return nil
	}
	stableDirectory := ServiceSecretRoot + "/" + strings.Repeat("1", 64)
	spec := ContainerSpec{Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080}, SecretMounts: []SecretMount{{StagedSource: SecretStagingRoot + "/deployment-execution/token", StableSource: stableDirectory + "/token", Target: "/run/secrets/token"}}}
	if err := host.EnsureContainer(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if refreshes != 1 || strings.Contains(strings.Join(runner.calls[1].arguments, " "), SecretStagingRoot) {
		t.Fatalf("refreshes=%d calls=%#v", refreshes, runner.calls)
	}
	if err := host.RefreshServiceSecrets(context.Background(), spec); err != nil || refreshes != 2 || len(runner.calls) != 2 {
		t.Fatalf("explicit refresh err=%v refreshes=%d podman calls=%d", err, refreshes, len(runner.calls))
	}
}

func TestPodmanHostCreatesFixedStorageDirectoriesAndReadonlyMountArguments(t *testing.T) {
	runner := &captureRunner{run: func(arguments []string) ([]byte, error) {
		if len(arguments) >= 2 && arguments[0] == "container" && arguments[1] == "exists" {
			return nil, testExitError(1)
		}
		return nil, nil
	}}
	host := newPodmanHost(0, runner)
	ensured := false
	host.ensureStorage = func(spec ContainerSpec) error {
		ensured = len(spec.StorageMounts) == 2
		return nil
	}
	deployment := strings.Repeat("1", 64)
	volume := strings.Repeat("2", 64)
	data := strings.Repeat("3", 64)
	spec := ContainerSpec{
		Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080},
		StorageMounts: []StorageMount{
			{Source: StorageRoot + "/" + deployment + "/data/" + data, Target: "/opt/service/knowledge", ReadOnly: true},
			{Source: StorageRoot + "/" + deployment + "/volumes/" + volume, Target: "/var/lib/service", ReadOnly: false},
		},
	}
	if err := host.EnsureContainer(context.Background(), spec); err != nil {
		t.Fatal(err)
	}
	if !ensured || len(runner.calls) != 2 {
		t.Fatalf("ensured=%v calls=%#v", ensured, runner.calls)
	}
	joined := strings.Join(runner.calls[1].arguments, " ")
	if !strings.Contains(joined, "source="+spec.StorageMounts[0].Source+",target=/opt/service/knowledge,readonly=true") ||
		!strings.Contains(joined, "source="+spec.StorageMounts[1].Source+",target=/var/lib/service,readonly=false") {
		t.Fatalf("storage arguments=%s", joined)
	}
	conflict := spec
	conflict.StorageMounts = append([]StorageMount(nil), spec.StorageMounts...)
	conflict.StorageMounts[1].Target = conflict.StorageMounts[0].Target
	if validateContainerSpec(conflict) == nil {
		t.Fatal("duplicate storage target accepted")
	}
}
