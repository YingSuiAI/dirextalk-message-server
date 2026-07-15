package ociservice

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type testExitError int

func (value testExitError) Error() string { return "exit" }
func (value testExitError) ExitCode() int { return int(value) }

type runnerCall struct {
	executable string
	arguments  []string
}

type captureRunner struct {
	calls []runnerCall
	run   func([]string) ([]byte, error)
}

func (runner *captureRunner) Run(_ context.Context, executable string, arguments []string) ([]byte, error) {
	runner.calls = append(runner.calls, runnerCall{executable: executable, arguments: append([]string(nil), arguments...)})
	if runner.run != nil {
		return runner.run(arguments)
	}
	return nil, nil
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
	spec := ContainerSpec{
		Name: "dtx-0123456789abcdef01234567", BindingDigest: digest("b"), ImageDigest: digest("a"), LoopbackPorts: []uint16{8080},
		SecretMounts: []SecretMount{{Source: SecretStagingRoot + "/deployment-execution/token", Target: "/run/secrets/token"}},
	}
	if err := validateContainerSpec(spec); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(createArguments(spec), " ")
	if strings.Contains(joined, "--env-file") || !strings.Contains(joined, "target=/run/secrets/token,readonly=true") || strings.Contains(joined, "docker.sock") {
		t.Fatalf("secret arguments=%s", joined)
	}
	outside := spec
	outside.SecretMounts = []SecretMount{{Source: "/tmp/token", Target: "/run/secrets/token"}}
	if validateContainerSpec(outside) == nil {
		t.Fatal("outside secret root accepted")
	}
}
