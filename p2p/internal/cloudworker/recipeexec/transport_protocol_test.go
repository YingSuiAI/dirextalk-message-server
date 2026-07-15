package recipeexec_test

import (
	"encoding/json"
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
