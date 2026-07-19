package main

import (
	"io"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/workerimage"
)

func TestParseBuildOptionsDynamicArtifactsIsExplicitAndDefaultOff(t *testing.T) {
	base := []string{
		"--artifact", "artifact.tar",
		"--region", "us-east-1",
		"--base-ami", "ami-0abcdef0123456789",
		"--subnet", "subnet-0abcdef0123456789",
		"--security-group", "sg-0abcdef0123456789",
		"--bucket", "dirextalk-worker-artifacts",
		"--key", "worker/v1.2.0-stage-t.1/artifact.tar",
		"--version", "v1.2.0-stage-t.1",
		"--oci-source", "ghcr.io/dirextalk/worker-fixture@sha256:2222222222222222222222222222222222222222222222222222222222222222",
		"--output", "image-manifest.json",
	}
	staticOptions, err := parseBuildOptions(base, io.Discard)
	if err != nil {
		t.Fatalf("parse static options: %v", err)
	}
	if staticOptions.config.DynamicRecipeArtifacts {
		t.Fatal("dynamic artifacts must remain default-off")
	}
	dynamicOptions, err := parseBuildOptions(append(append([]string{}, base...), "--dynamic-artifacts"), io.Discard)
	if err != nil {
		t.Fatalf("parse dynamic options: %v", err)
	}
	if !dynamicOptions.config.DynamicRecipeArtifacts {
		t.Fatal("--dynamic-artifacts was not propagated to BuildConfig")
	}
	if workerimage.RecipeArtifactDynamic != "dynamic" {
		t.Fatal("unexpected public dynamic mode value")
	}
}
