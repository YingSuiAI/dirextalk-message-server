package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

var _ RecipeTaskRepository = (*DynamoWorkerTaskStore)(nil)

func TestDynamoRecipeTaskIssueIsAtomicAndRecordKindIsolated(t *testing.T) {
	task := recipeTaskStoreFixture(t)
	client := &fakeWorkerTaskDynamo{tasks: map[string]map[string]dynamodbtypes.AttributeValue{}}
	store := mustWorkerTaskStore(t, client)
	receipt := Record{ConnectionID: task.ConnectionID, CommandID: "command-recipe-issue-0001", RequestSHA256: task.RequestSHA256, ExpectedGeneration: 1, NodeCounter: 5, Action: contract.ActionWorkerRecipeTaskIssue, ResultJSON: []byte(`{"result":"safe"}`)}
	_, stored, created, err := store.IssueRecipeTask(t.Context(), receipt, task)
	if err != nil || !created || !sameRecipeTaskBinding(stored, task) {
		t.Fatalf("IssueRecipeTask() = (%#v,%t,%v)", stored, created, err)
	}
	if client.lastTransact == nil || len(client.lastTransact.TransactItems) != 3 || client.lastTransact.TransactItems[0].Update == nil || client.lastTransact.TransactItems[1].Put == nil || client.lastTransact.TransactItems[2].Put == nil {
		t.Fatalf("issue transaction = %#v", client.lastTransact)
	}
	item := client.lastTransact.TransactItems[2].Put.Item
	if kind, ok := item["record_kind"].(*dynamodbtypes.AttributeValueMemberS); !ok || kind.Value != recipeTaskRecordKind {
		t.Fatalf("record kind = %#v", item["record_kind"])
	}
	for _, forbidden := range []string{"command", "url", "path", "log", "access_token", "token_sha256", "event_json"} {
		if _, found := item[forbidden]; found {
			t.Fatalf("stored recipe task contains %s", forbidden)
		}
	}
	if _, err := workerTaskFromItem(item); Code(err) != "worker_task_store_invalid" {
		t.Fatalf("execution probe parser accepted recipe record: %v", err)
	}
}

func TestDynamoRecipeTaskClaimSkipsExecutionProbeAndReturnsManifest(t *testing.T) {
	recipe := recipeTaskStoreFixture(t)
	recipe.LeaseEpoch = 1
	probe := workerTaskFixture()
	client := &fakeWorkerTaskDynamo{session: activeWorkerSessionItem(), tasks: map[string]map[string]dynamodbtypes.AttributeValue{recipe.DeploymentID + "/" + recipe.TaskID: recipeTaskItem(recipe), probe.DeploymentID + "/" + probe.TaskID: workerTaskItem(probe)}}
	store := mustWorkerTaskStore(t, client)
	claimed, found, advanced, err := store.ClaimRecipeTask(t.Context(), workerAuthorizationFixture())
	if err != nil || !found || advanced || claimed.TaskID != recipe.TaskID || len(claimed.ManifestJSON) == 0 {
		t.Fatalf("ClaimRecipeTask() = (%#v,%t,%t,%v)", claimed, found, advanced, err)
	}
}

func TestDynamoRecipeTaskEventCASReconcilesResponseLossAndExactReplay(t *testing.T) {
	task := recipeTaskStoreFixture(t)
	task.LeaseEpoch = 1
	auth := workerAuthorizationFixture()
	event := RecipeTaskEvent{TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 1, Sequence: 1, Status: "running", Checkpoint: "artifact_verified", EvidenceDigest: task.RecipeExecutionManifestDigest, OccurredAt: "2026-07-14T12:02:00.000Z", EventSHA256: strings.Repeat("e", 64)}
	client := &fakeWorkerTaskDynamo{session: activeWorkerSessionItem(), tasks: map[string]map[string]dynamodbtypes.AttributeValue{task.DeploymentID + "/" + task.TaskID: recipeTaskItem(task)}}
	client.transact = func(input *dynamodb.TransactWriteItemsInput) error {
		item := client.tasks[task.DeploymentID+"/"+task.TaskID]
		item["status"] = &dynamodbtypes.AttributeValueMemberS{Value: "running"}
		item["last_sequence"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
		item["last_event_sha256"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}
		item["last_checkpoint"] = &dynamodbtypes.AttributeValueMemberS{Value: event.Checkpoint}
		item["evidence_digest"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EvidenceDigest}
		item["updated_at"] = &dynamodbtypes.AttributeValueMemberS{Value: auth.Now}
		return errors.New("response lost")
	}
	store := mustWorkerTaskStore(t, client)
	stored, idempotent, err := store.RecordRecipeTaskEvent(t.Context(), auth, event)
	if err != nil || !idempotent || stored.LastCheckpoint != "artifact_verified" {
		t.Fatalf("RecordRecipeTaskEvent()=(%#v,%t,%v)", stored, idempotent, err)
	}
	client.transact = func(*dynamodb.TransactWriteItemsInput) error { t.Fatal("exact replay wrote again"); return nil }
	if _, idempotent, err = store.RecordRecipeTaskEvent(t.Context(), auth, event); err != nil || !idempotent {
		t.Fatalf("exact replay=(%t,%v)", idempotent, err)
	}
}

func recipeTaskStoreFixture(t *testing.T) RecipeTaskRecord {
	t.Helper()
	manifest := contract.RecipeExecutionManifestV1{SchemaVersion: contract.RecipeExecutionManifestSchema, ExecutionID: "execution-0001", DeploymentID: "deployment-0001", PlanID: "plan-00000001", PlanHash: "sha256:" + strings.Repeat("1", 64), PlanRevision: 1, RecipeDigest: "sha256:" + strings.Repeat("2", 64), WorkerResourceManifestDigest: "sha256:" + strings.Repeat("3", 64), ArtifactDigest: "sha256:" + strings.Repeat("4", 64), ActionID: "install_service", RootRequired: true, TimeoutSeconds: 900, CheckpointSequence: []string{"artifact_verified", "install_complete", "health_verified"}}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := manifest.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return RecipeTaskRecord{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "recipe-task-0001", RequestSHA256: strings.Repeat("a", 64), BootstrapSessionID: "bootstrap-session-0001", ExpectedInstanceID: "i-0123456789abcdef0", ExecutionID: manifest.ExecutionID, TaskKind: contract.RecipeTaskKindExecution, RecipeExecutionManifestDigest: digest, InputDigest: "sha256:" + strings.Repeat("b", 64), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), ManifestJSON: raw, Status: "queued", Attempt: 1, CreatedAt: "2026-07-14T12:01:00.000Z", UpdatedAt: "2026-07-14T12:01:00.000Z"}
}
