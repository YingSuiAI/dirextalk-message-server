package recipeexec_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestRecipeTaskClaimResponseIsClosedAndManifestBound(t *testing.T) {
	manifest := validManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: sha256('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	raw, err := json.Marshal(recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest})
	if err != nil {
		t.Fatal(err)
	}
	response, err := recipeexec.ParseTaskClaimResponseV1(raw, 7)
	if err != nil || response.Task == nil || response.Manifest == nil {
		t.Fatalf("ParseTaskClaimResponseV1() = (%#v, %v)", response, err)
	}

	for _, invalid := range [][]byte{
		[]byte(`{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"none","lease_epoch":7,"secret":"not-allowed"}`),
		[]byte(`{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"none","lease_epoch":7,"manifest":null}`),
		append(raw[:len(raw)-1], []byte(`,"url":"https://example.invalid"}`)...),
	} {
		if _, err := recipeexec.ParseTaskClaimResponseV1(invalid, 7); err == nil {
			t.Fatalf("accepted closed-contract violation: %s", invalid)
		}
	}

	wrong := manifest
	wrong.ActionID = "restart-service"
	wrongRaw, err := json.Marshal(recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &wrong})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recipeexec.ParseTaskClaimResponseV1(wrongRaw, 7); err == nil {
		t.Fatal("accepted a task bound to another manifest")
	}
}

func TestRecipeTaskClaimResponseAcceptsOnlyTransientPinnedArtifactAccess(t *testing.T) {
	manifest := validManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: sha256('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	access := recipeexec.ArtifactAccessV1{Method: "GET", URL: "https://artifacts.example.invalid/object?versionId=version-0001&X-Amz-Signature=secret", ExpiresAt: "2026-07-16T10:10:00.000Z", VersionID: "version-0001", MediaType: recipeexec.RecipeArtifactMediaTypeV1, SizeBytes: 4096, ArchiveSHA256: strings.Repeat("a", 64)}
	raw, err := json.Marshal(recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest, ArtifactAccess: &access})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := recipeexec.ParseTaskClaimResponseV1(raw, 7)
	if err != nil || parsed.ArtifactAccess == nil || *parsed.ArtifactAccess != access {
		t.Fatalf("artifact claim = %#v, error = %v", parsed, err)
	}

	pending := []byte(`{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"artifact_pending","lease_epoch":7}`)
	if parsed, err := recipeexec.ParseTaskClaimResponseV1(pending, 7); err != nil || parsed.Task != nil || parsed.Manifest != nil || parsed.ArtifactAccess != nil {
		t.Fatalf("artifact pending = %#v, error = %v", parsed, err)
	}

	for _, mutate := range []func(*recipeexec.ArtifactAccessV1){
		func(value *recipeexec.ArtifactAccessV1) { value.Method = "POST" },
		func(value *recipeexec.ArtifactAccessV1) {
			value.URL = "http://artifacts.example.invalid/object?versionId=version-0001"
		},
		func(value *recipeexec.ArtifactAccessV1) {
			value.URL = "https://user:pass@artifacts.example.invalid/object?versionId=version-0001"
		},
		func(value *recipeexec.ArtifactAccessV1) {
			value.URL = "https://artifacts.example.invalid/object?versionId=another-version"
		},
		func(value *recipeexec.ArtifactAccessV1) {
			value.URL = "https://artifacts.example.invalid/object?versionId=version-0001&versionId=version-0001"
		},
		func(value *recipeexec.ArtifactAccessV1) { value.ExpiresAt = "tomorrow" },
		func(value *recipeexec.ArtifactAccessV1) { value.VersionID = "version id" },
		func(value *recipeexec.ArtifactAccessV1) { value.MediaType = "application/octet-stream" },
		func(value *recipeexec.ArtifactAccessV1) { value.SizeBytes = 256<<20 + 1 },
		func(value *recipeexec.ArtifactAccessV1) { value.ArchiveSHA256 = "sha256:" + strings.Repeat("a", 64) },
	} {
		invalid := access
		mutate(&invalid)
		raw, _ := json.Marshal(recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest, ArtifactAccess: &invalid})
		if _, err := recipeexec.ParseTaskClaimResponseV1(raw, 7); err == nil {
			t.Fatalf("accepted invalid artifact access: %#v", invalid)
		}
	}

	for _, invalid := range [][]byte{
		[]byte(`{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"artifact_pending","lease_epoch":7,"artifact_access":null}`),
		[]byte(`{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"none","lease_epoch":7,"artifact_access":null}`),
	} {
		if _, err := recipeexec.ParseTaskClaimResponseV1(invalid, 7); err == nil {
			t.Fatalf("accepted artifact access outside a claimed response: %s", invalid)
		}
	}
}
