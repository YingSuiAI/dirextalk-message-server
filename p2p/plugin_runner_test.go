package p2p

import "testing"

func TestNewEnvironmentPluginRunnerDefaultsToNoop(t *testing.T) {
	t.Setenv("P2P_PLUGIN_DOCKER_ENABLED", "")

	if _, ok := newEnvironmentPluginRunner().(noopPluginRunner); !ok {
		t.Fatalf("expected noop plugin runner when docker runner is disabled")
	}
}

func TestNewEnvironmentPluginRunnerUsesDockerWhenEnabled(t *testing.T) {
	t.Setenv("P2P_PLUGIN_DOCKER_ENABLED", "true")
	t.Setenv("P2P_PLUGIN_DOCKER_BIN", "docker-test")
	t.Setenv("P2P_PLUGIN_DOCKER_NETWORK", "dirextalk-p2p_default")

	runner, ok := newEnvironmentPluginRunner().(dockerPluginRunner)
	if !ok {
		t.Fatalf("expected docker plugin runner when enabled")
	}
	if runner.binary != "docker-test" {
		t.Fatalf("expected configured docker binary, got %q", runner.binary)
	}
	if runner.network != "dirextalk-p2p_default" {
		t.Fatalf("expected configured docker network, got %q", runner.network)
	}
}

func TestValidateOfficialPluginOperationAllowsDirextalkImageWithoutDigest(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "install",
		PluginID: "io.dirextalk.agent",
		Image:    "docker.io/dirextalk/agent-plugin:latest",
	}

	if err := validateOfficialPluginOperation(op); err != nil {
		t.Fatalf("expected official dirextalk image without digest to pass, got %v", err)
	}
}

func TestValidateOfficialPluginOperationRejectsNonDirextalkImage(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "install",
		PluginID: "io.dirextalk.agent",
		Image:    "docker.io/example/agent-plugin:latest",
	}

	if err := validateOfficialPluginOperation(op); err == nil {
		t.Fatalf("expected non-dirextalk image to fail")
	}
}

func TestValidateOfficialPluginOperationRejectsInvalidOptionalDigest(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "install",
		PluginID: "io.dirextalk.agent",
		Image:    "dirextalk/agent-plugin:latest",
		Digest:   "latest",
	}

	if err := validateOfficialPluginOperation(op); err == nil {
		t.Fatalf("expected invalid digest to fail")
	}
}

func TestValidateOfficialPluginOperationRejectsPrivilegedMountForNonOpsPlugin(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "enable",
		PluginID: "io.dirextalk.agent",
		Image:    "docker.io/dirextalk/agent-plugin:latest",
		Volumes:  []string{"/var/run/docker.sock:/var/run/docker.sock"},
	}

	if err := validateOfficialPluginOperation(op); err == nil {
		t.Fatalf("expected non-ops plugin privileged mount to fail")
	}
}

func TestValidateOfficialPluginOperationRejectsAgentDataVolume(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "enable",
		PluginID: "io.dirextalk.agent",
		Image:    "docker.io/dirextalk/agent-plugin:latest",
		Volumes:  []string{"dirextalk_agent_data:/var/lib/dirextalk-agent"},
	}

	if err := validateOfficialPluginOperation(op); err == nil {
		t.Fatalf("expected agent data volume to be rejected")
	}
}

func TestValidateOfficialPluginOperationAllowsOpsPrivilegedMounts(t *testing.T) {
	op := PluginRunnerOperation{
		Action:   "enable",
		PluginID: "io.dirextalk.ops",
		Image:    "docker.io/dirextalk/ops-plugin:latest",
		Volumes: []string{
			"/var/run/docker.sock:/var/run/docker.sock",
			"dirextalk_ops_backups:/var/lib/dirextalk-ops",
		},
	}

	if err := validateOfficialPluginOperation(op); err != nil {
		t.Fatalf("expected ops privileged mounts to pass, got %v", err)
	}
}

func TestPluginImageReferenceUsesDigestOnlyWhenPresent(t *testing.T) {
	if got := pluginImageReference(" docker.io/dirextalk/agent-plugin:latest ", ""); got != "docker.io/dirextalk/agent-plugin:latest" {
		t.Fatalf("expected tag image reference, got %q", got)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := pluginImageReference("dirextalk/agent-plugin:latest", digest); got != "dirextalk/agent-plugin:latest@"+digest {
		t.Fatalf("expected digest image reference, got %q", got)
	}
}
