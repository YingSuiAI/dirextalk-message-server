package p2p

import "testing"

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

func TestPluginImageReferenceUsesDigestOnlyWhenPresent(t *testing.T) {
	if got := pluginImageReference(" docker.io/dirextalk/agent-plugin:latest ", ""); got != "docker.io/dirextalk/agent-plugin:latest" {
		t.Fatalf("expected tag image reference, got %q", got)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if got := pluginImageReference("dirextalk/agent-plugin:latest", digest); got != "dirextalk/agent-plugin:latest@"+digest {
		t.Fatalf("expected digest image reference, got %q", got)
	}
}
