package cloudorchestrator_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestExecutionProbeArtifactsUseDeterministicCBORDigests(t *testing.T) {
	manifest := validExecutionProbeManifest(t)
	input := validNoInput(manifest)

	for _, artifact := range []struct {
		name      string
		canonical func() ([]byte, error)
		digest    func() (string, error)
	}{
		{name: "execution manifest", canonical: manifest.CanonicalExecutionProbeManifestCBOR, digest: manifest.Digest},
		{name: "no input", canonical: input.CanonicalNoInputCBOR, digest: input.Digest},
	} {
		t.Run(artifact.name, func(t *testing.T) {
			firstCBOR, err := artifact.canonical()
			if err != nil {
				t.Fatalf("canonical CBOR error = %v", err)
			}
			secondCBOR, err := artifact.canonical()
			if err != nil {
				t.Fatalf("second canonical CBOR error = %v", err)
			}
			if !bytes.Equal(firstCBOR, secondCBOR) {
				t.Fatal("same artifact did not produce stable canonical CBOR")
			}
			firstDigest, err := artifact.digest()
			if err != nil {
				t.Fatalf("Digest() error = %v", err)
			}
			secondDigest, err := artifact.digest()
			if err != nil {
				t.Fatalf("second Digest() error = %v", err)
			}
			if firstDigest != secondDigest || !validArtifactDigest(firstDigest) {
				t.Fatalf("Digest() = %q, want stable lowercase sha256 digest", firstDigest)
			}
		})
	}
}

func TestExecutionProbeManifestDigestBindsEveryMutableReference(t *testing.T) {
	manifest := validExecutionProbeManifest(t)
	baseline, err := manifest.Digest()
	if err != nil {
		t.Fatalf("Digest() error = %v", err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.ExecutionProbeManifestV1)
	}{
		{name: "deployment", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) {
			value.DeploymentID = "deployment-execution-probe-2"
		}},
		{name: "plan", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.PlanID = "plan-2" }},
		{name: "plan hash", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.PlanHash = artifactDigest('b') }},
		{name: "plan revision", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.PlanRevision++ }},
		{name: "recipe", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.RecipeDigest = artifactDigest('c') }},
		{name: "worker resource manifest", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) {
			value.WorkerResourceManifestDigest = artifactDigest('d')
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := manifest
			test.mutate(&changed)
			digest, err := changed.Digest()
			if err != nil {
				t.Fatalf("changed Digest() error = %v", err)
			}
			if digest == baseline {
				t.Fatalf("%s did not change execution manifest digest", test.name)
			}
		})
	}

	input := validNoInput(manifest)
	inputDigest, err := input.Digest()
	if err != nil {
		t.Fatalf("NoInputV1.Digest() error = %v", err)
	}
	input.DeploymentID = "deployment-execution-probe-2"
	changedInputDigest, err := input.Digest()
	if err != nil {
		t.Fatalf("changed NoInputV1.Digest() error = %v", err)
	}
	if changedInputDigest == inputDigest {
		t.Fatal("deployment binding did not change no-input digest")
	}
}

func TestExecutionProbeArtifactsRejectInvalidAndNonArtifactDigests(t *testing.T) {
	manifest := validExecutionProbeManifest(t)
	input := validNoInput(manifest)
	for _, test := range []struct {
		name   string
		mutate func(*cloudorchestrator.ExecutionProbeManifestV1)
	}{
		{name: "schema", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) {
			value.SchemaVersion = cloudorchestrator.SchemaVersionV1
		}},
		{name: "deployment identifier", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.DeploymentID = "https://worker.invalid" }},
		{name: "plan hash", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.PlanHash = "not-a-digest" }},
		{name: "plan revision", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.PlanRevision = 0 }},
		{name: "recipe digest", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.RecipeDigest = "not-a-digest" }},
		{name: "worker resource digest", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) {
			value.WorkerResourceManifestDigest = "not-a-digest"
		}},
		{name: "task kind", mutate: func(value *cloudorchestrator.ExecutionProbeManifestV1) { value.TaskKind = "command" }},
	} {
		t.Run("manifest "+test.name, func(t *testing.T) {
			changed := manifest
			test.mutate(&changed)
			if err := changed.Validate(); err == nil {
				t.Fatal("Validate() accepted an invalid execution probe manifest")
			}
		})
	}

	input.SchemaVersion = cloudorchestrator.SchemaVersionV1
	if err := input.Validate(); err == nil {
		t.Fatal("NoInputV1.Validate() accepted a non-input schema")
	}
	input = validNoInput(manifest)
	input.DeploymentID = "https://worker.invalid"
	if err := input.Validate(); err == nil {
		t.Fatal("NoInputV1.Validate() accepted a URL-shaped deployment identifier")
	}
	input = validNoInput(manifest)
	input.NoInput = false
	if err := input.Validate(); err == nil {
		t.Fatal("NoInputV1.Validate() accepted an input-bearing value")
	}
	input = validNoInput(manifest)
	input.TaskKind = "command"
	if err := input.Validate(); err == nil {
		t.Fatal("NoInputV1.Validate() accepted a non-probe task kind")
	}

	plan := validPlan(t, time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC))
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("PlanV1.Hash() error = %v", err)
	}
	for _, digest := range []string{"not-a-digest", planHash, plan.Recipe.Digest} {
		if err := manifest.VerifyDigest(digest); err == nil {
			t.Fatalf("ExecutionProbeManifestV1.VerifyDigest(%q) accepted a non-artifact digest", digest)
		}
		if err := validNoInput(manifest).VerifyDigest(digest); err == nil {
			t.Fatalf("NoInputV1.VerifyDigest(%q) accepted a non-artifact digest", digest)
		}
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	if err := manifest.VerifyDigest(manifestDigest); err != nil {
		t.Fatalf("manifest.VerifyDigest(own digest) error = %v", err)
	}
	input = validNoInput(manifest)
	if err := input.ValidateForManifest(manifest); err != nil {
		t.Fatalf("NoInputV1.ValidateForManifest() error = %v", err)
	}
	input.DeploymentID = "deployment-execution-probe-2"
	if err := input.ValidateForManifest(manifest); err == nil {
		t.Fatal("NoInputV1.ValidateForManifest() accepted a different deployment")
	}
}

func TestExecutionProbeArtifactsExposeOnlyDeSecretedBindings(t *testing.T) {
	manifest := validExecutionProbeManifest(t)
	input := validNoInput(manifest)
	assertJSONFields(t, manifest, []string{
		"schema_version", "deployment_id", "plan_id", "plan_hash", "plan_revision", "recipe_digest", "worker_resource_manifest_digest", "task_kind",
	})
	assertJSONFields(t, input, []string{"schema_version", "deployment_id", "task_kind", "no_input"})
}

func validExecutionProbeManifest(t *testing.T) cloudorchestrator.ExecutionProbeManifestV1 {
	t.Helper()
	plan := validPlan(t, time.Date(2026, time.July, 15, 10, 0, 0, 0, time.UTC))
	planHash, err := plan.Hash()
	if err != nil {
		t.Fatalf("PlanV1.Hash() error = %v", err)
	}
	return cloudorchestrator.ExecutionProbeManifestV1{
		SchemaVersion:                cloudorchestrator.ExecutionProbeManifestV1Schema,
		DeploymentID:                 "deployment-execution-probe-1",
		PlanID:                       plan.PlanID,
		PlanHash:                     planHash,
		PlanRevision:                 plan.Revision,
		RecipeDigest:                 plan.Recipe.Digest,
		WorkerResourceManifestDigest: artifactDigest('a'),
		TaskKind:                     cloudorchestrator.ExecutionProbeTaskKind,
	}
}

func validNoInput(manifest cloudorchestrator.ExecutionProbeManifestV1) cloudorchestrator.NoInputV1 {
	return cloudorchestrator.NoInputV1{
		SchemaVersion: cloudorchestrator.NoInputV1Schema,
		DeploymentID:  manifest.DeploymentID,
		TaskKind:      cloudorchestrator.ExecutionProbeTaskKind,
		NoInput:       true,
	}
}

func artifactDigest(character rune) string {
	return "sha256:" + strings.Repeat(string(character), 64)
}

func validArtifactDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || value != strings.ToLower(value) {
		return false
	}
	for _, character := range strings.TrimPrefix(value, "sha256:") {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}

func assertJSONFields(t *testing.T, value any, want []string) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(fields) != len(want) {
		t.Fatalf("JSON fields = %v, want %v", fields, want)
	}
	for _, name := range want {
		if _, found := fields[name]; !found {
			t.Fatalf("JSON artifact has no %q binding: %s", name, encoded)
		}
	}
}
