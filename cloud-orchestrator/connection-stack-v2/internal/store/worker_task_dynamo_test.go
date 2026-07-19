package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var _ WorkerTaskRepository = (*DynamoWorkerTaskStore)(nil)

type fakeWorkerTaskDynamo struct {
	session      map[string]dynamodbtypes.AttributeValue
	receipts     map[string]map[string]dynamodbtypes.AttributeValue
	tasks        map[string]map[string]dynamodbtypes.AttributeValue
	transact     func(*dynamodb.TransactWriteItemsInput) error
	queryCalls   int
	lastTransact *dynamodb.TransactWriteItemsInput
}

func (f *fakeWorkerTaskDynamo) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if *input.TableName == "worker-sessions" {
		return &dynamodb.GetItemOutput{Item: cloneDynamoItem(f.session)}, nil
	}
	if *input.TableName == "receipts" {
		connection := input.Key["connection_id"].(*dynamodbtypes.AttributeValueMemberS).Value
		command := input.Key["command_id"].(*dynamodbtypes.AttributeValueMemberS).Value
		return &dynamodb.GetItemOutput{Item: cloneDynamoItem(f.receipts[connection+"/"+command])}, nil
	}
	deployment := input.Key["deployment_id"].(*dynamodbtypes.AttributeValueMemberS).Value
	task := input.Key["task_id"].(*dynamodbtypes.AttributeValueMemberS).Value
	return &dynamodb.GetItemOutput{Item: cloneDynamoItem(f.tasks[deployment+"/"+task])}, nil
}

func TestDynamoWorkerTaskIssueAtomicallyFencesReceiptAndTask(t *testing.T) {
	task := workerTaskFixture()
	receipt := Record{ConnectionID: task.ConnectionID, CommandID: "command-task-0001", RequestSHA256: task.RequestSHA256,
		ExpectedGeneration: 1, NodeCounter: 7, Action: "worker.task.issue", ResultJSON: []byte(`{"status":"worker_task_issued"}`)}
	client := &fakeWorkerTaskDynamo{receipts: map[string]map[string]dynamodbtypes.AttributeValue{}, tasks: map[string]map[string]dynamodbtypes.AttributeValue{}}
	client.transact = func(input *dynamodb.TransactWriteItemsInput) error {
		if len(input.TransactItems) != 3 || input.TransactItems[0].Update == nil || input.TransactItems[1].Put == nil || input.TransactItems[2].Put == nil {
			t.Fatalf("atomic issue transaction = %#v", input.TransactItems)
		}
		client.receipts[receipt.ConnectionID+"/"+receipt.CommandID] = cloneDynamoItem(input.TransactItems[1].Put.Item)
		client.tasks[task.DeploymentID+"/"+task.TaskID] = cloneDynamoItem(input.TransactItems[2].Put.Item)
		return errors.New("response lost")
	}
	store := mustWorkerTaskStore(t, client)
	storedReceipt, storedTask, created, err := store.IssueWorkerTask(t.Context(), receipt, task)
	if err != nil || created || !storedReceipt.SameIdentity(receipt) || !sameWorkerTaskBinding(storedTask, task) {
		t.Fatalf("IssueWorkerTask() = (%#v, %#v, %t, %v)", storedReceipt, storedTask, created, err)
	}
}

func (f *fakeWorkerTaskDynamo) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.queryCalls++
	deployment := input.ExpressionAttributeValues[":deployment_id"].(*dynamodbtypes.AttributeValueMemberS).Value
	output := &dynamodb.QueryOutput{}
	for key, item := range f.tasks {
		if len(key) > len(deployment) && key[:len(deployment)+1] == deployment+"/" {
			output.Items = append(output.Items, cloneDynamoItem(item))
		}
	}
	return output, nil
}

func (f *fakeWorkerTaskDynamo) TransactWriteItems(_ context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.lastTransact = input
	if f.transact != nil {
		return nil, f.transact(input)
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func TestDynamoWorkerTaskStoreReconcilesIssueResponseLoss(t *testing.T) {
	task := workerTaskFixture()
	client := &fakeWorkerTaskDynamo{tasks: map[string]map[string]dynamodbtypes.AttributeValue{}}
	client.transact = func(input *dynamodb.TransactWriteItemsInput) error {
		client.tasks[task.DeploymentID+"/"+task.TaskID] = cloneDynamoItem(input.TransactItems[0].Put.Item)
		return errors.New("response lost")
	}
	store := mustWorkerTaskStore(t, client)
	stored, created, err := store.EnsureWorkerTask(t.Context(), task)
	if err != nil || created || !sameWorkerTaskBinding(stored, task) {
		t.Fatalf("EnsureWorkerTask() = (%#v, %t, %v)", stored, created, err)
	}
	for _, forbidden := range []string{"access_token", "token_sha256", "event_json", "last_event_json"} {
		if _, found := client.tasks[task.DeploymentID+"/"+task.TaskID][forbidden]; found {
			t.Fatalf("stored task contains %s", forbidden)
		}
	}
}

func TestDynamoWorkerTaskStoreClaimAuthenticatesAndReconcilesResponseLoss(t *testing.T) {
	task := workerTaskFixture()
	auth := workerAuthorizationFixture()
	client := &fakeWorkerTaskDynamo{session: activeWorkerSessionItem(), tasks: map[string]map[string]dynamodbtypes.AttributeValue{task.DeploymentID + "/" + task.TaskID: workerTaskItem(task)}}
	client.transact = func(input *dynamodb.TransactWriteItemsInput) error {
		if len(input.TransactItems) != 2 || input.TransactItems[0].ConditionCheck == nil || input.TransactItems[1].Update == nil {
			t.Fatalf("claim transaction = %#v", input.TransactItems)
		}
		item := client.tasks[task.DeploymentID+"/"+task.TaskID]
		item["lease_epoch"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
		item["updated_at"] = &dynamodbtypes.AttributeValueMemberS{Value: auth.Now}
		return errors.New("response lost")
	}
	store := mustWorkerTaskStore(t, client)
	claimed, found, committed, err := store.ClaimWorkerTask(t.Context(), auth)
	if err != nil || !found || !committed || claimed.LeaseEpoch != 1 {
		t.Fatalf("ClaimWorkerTask() = (%#v, %t, %t, %v)", claimed, found, committed, err)
	}

	bad := auth
	bad.TokenSHA256 = string(make([]byte, 64))
	client.queryCalls = 0
	if _, _, _, err := store.ClaimWorkerTask(t.Context(), bad); Code(err) != "worker_task_unauthorized" || client.queryCalls != 0 {
		t.Fatalf("unauthorized claim = %v, query calls %d", err, client.queryCalls)
	}
}

func TestDynamoWorkerTaskStoreTaskEventIsExactAndResponseLossSafe(t *testing.T) {
	task := workerTaskFixture()
	task.LeaseEpoch = 1
	auth := workerAuthorizationFixture()
	event := WorkerTaskEvent{TaskID: task.TaskID, Attempt: 1, LeaseEpoch: 1, Sequence: 1, Status: "running", Checkpoint: "execution_manifest_received", EvidenceDigest: task.ExecutionManifestDigest, OccurredAt: "2026-07-14T12:02:00.000Z", EventSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
	client := &fakeWorkerTaskDynamo{session: activeWorkerSessionItem(), tasks: map[string]map[string]dynamodbtypes.AttributeValue{task.DeploymentID + "/" + task.TaskID: workerTaskItem(task)}}
	client.transact = func(input *dynamodb.TransactWriteItemsInput) error {
		item := client.tasks[task.DeploymentID+"/"+task.TaskID]
		item["status"] = &dynamodbtypes.AttributeValueMemberS{Value: event.Status}
		item["last_sequence"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
		item["last_event_sha256"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}
		item["checkpoint"] = &dynamodbtypes.AttributeValueMemberS{Value: event.Checkpoint}
		item["evidence_digest"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EvidenceDigest}
		item["updated_at"] = &dynamodbtypes.AttributeValueMemberS{Value: auth.Now}
		return errors.New("response lost")
	}
	store := mustWorkerTaskStore(t, client)
	stored, idempotent, err := store.RecordWorkerTaskEvent(t.Context(), auth, event)
	if err != nil || !idempotent || stored.LastSequence != 1 {
		t.Fatalf("RecordWorkerTaskEvent() = (%#v, %t, %v)", stored, idempotent, err)
	}
	client.transact = func(*dynamodb.TransactWriteItemsInput) error {
		t.Fatal("exact replay attempted another write")
		return nil
	}
	if _, idempotent, err := store.RecordWorkerTaskEvent(t.Context(), auth, event); err != nil || !idempotent {
		t.Fatalf("exact replay = (%t, %v)", idempotent, err)
	}
	conflict := event
	conflict.EventSHA256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if _, _, err := store.RecordWorkerTaskEvent(t.Context(), auth, conflict); Code(err) != "worker_task_event_invalid" {
		t.Fatalf("same-sequence conflict = %v", err)
	}
}

func TestDynamoRepositoryWorkerHeartbeatAuthenticatesHashAndReconcilesResponseLoss(t *testing.T) {
	event := WorkerSessionEvent{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", BootstrapSessionID: "bootstrap-session-0001", ExpectedInstanceID: "i-0123456789abcdef0", LeaseEpoch: 1, Sequence: 1, TokenSHA256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", EventSHA256: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", OccurredAt: "2026-07-14T12:02:00.000Z", Now: "2026-07-14T12:01:30.000Z"}
	storedItem := activeWorkerSessionItem()
	storedItem["last_sequence"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
	storedItem["last_event_at"] = &dynamodbtypes.AttributeValueMemberS{Value: event.OccurredAt}
	storedItem["last_event_sha256"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}
	client := &fakeDynamo{transactErr: errors.New("response lost")}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if *input.TableName == "worker-sessions" {
			return &dynamodb.GetItemOutput{Item: storedItem}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)
	stored, idempotent, err := repository.RecordWorkerSessionEvent(t.Context(), event)
	if err != nil || !idempotent || stored.LastEventSHA256 != event.EventSHA256 {
		t.Fatalf("RecordWorkerSessionEvent() = (%#v, %t, %v)", stored, idempotent, err)
	}
	update := client.transactInput.TransactItems[0].Update
	if update == nil || update.ConditionExpression == nil || !containsAll(*update.ConditionExpression, "token_sha256 = :token_sha256", "lease_expires_at > :now", "last_sequence = :previous_sequence") {
		t.Fatalf("heartbeat condition = %#v", update)
	}
}

func containsAll(value string, fragments ...string) bool {
	for _, fragment := range fragments {
		if !strings.Contains(value, fragment) {
			return false
		}
	}
	return true
}

func workerTaskFixture() WorkerTaskRecord {
	return WorkerTaskRecord{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "worker-task-0001", RequestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BootstrapSessionID: "bootstrap-session-0001", ExpectedInstanceID: "i-0123456789abcdef0", TaskKind: "execution_probe", ExecutionManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", InputDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Status: "queued", Attempt: 1, CreatedAt: "2026-07-14T12:01:00.000Z", UpdatedAt: "2026-07-14T12:01:00.000Z"}
}

func workerAuthorizationFixture() WorkerLeaseAuthorization {
	return WorkerLeaseAuthorization{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", BootstrapSessionID: "bootstrap-session-0001", ExpectedInstanceID: "i-0123456789abcdef0", LeaseEpoch: 1, TokenSHA256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Now: "2026-07-14T12:01:30.000Z"}
}

func activeWorkerSessionItem() map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: "bootstrap-session-0001"}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: "connection-0001"}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: "deployment-0001"}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, "worker_image_digest": &dynamodbtypes.AttributeValueMemberS{Value: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, "artifact_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}, "bootstrap_endpoint": &dynamodbtypes.AttributeValueMemberS{Value: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/worker-sessions"}, "expected_ami_id": &dynamodbtypes.AttributeValueMemberS{Value: "ami-0123456789abcdef0"}, "expected_instance_type": &dynamodbtypes.AttributeValueMemberS{Value: "m7i.xlarge"}, "expected_architecture": &dynamodbtypes.AttributeValueMemberS{Value: "x86_64"}, "expected_vpc_id": &dynamodbtypes.AttributeValueMemberS{Value: "vpc-0123456789abcdef0"}, "expected_subnet_id": &dynamodbtypes.AttributeValueMemberS{Value: "subnet-0123456789abcdef0"}, "expected_availability_zone": &dynamodbtypes.AttributeValueMemberS{Value: "us-east-1a"}, "expected_security_group_id": &dynamodbtypes.AttributeValueMemberS{Value: "sg-0123456789abcdef0"}, "expected_instance_id": &dynamodbtypes.AttributeValueMemberS{Value: "i-0123456789abcdef0"}, "state": &dynamodbtypes.AttributeValueMemberS{Value: "active"}, "expires_at": &dynamodbtypes.AttributeValueMemberS{Value: "2026-07-14T12:10:00.000Z"}, "lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: "1"}, "lease_expires_at": &dynamodbtypes.AttributeValueMemberS{Value: "2026-07-14T12:05:00.000Z"}, "token_sha256": &dynamodbtypes.AttributeValueMemberS{Value: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}, "last_sequence": &dynamodbtypes.AttributeValueMemberN{Value: "0"}, "ttl_epoch_seconds": &dynamodbtypes.AttributeValueMemberN{Value: "1784030700"}}
}

func mustWorkerTaskStore(t *testing.T, client WorkerTaskDynamoAPI) *DynamoWorkerTaskStore {
	t.Helper()
	store, err := NewDynamoWorkerTaskStore(client, "receipts", "counters", "worker-sessions", "worker-tasks")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func cloneDynamoItem(item map[string]dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
	if item == nil {
		return nil
	}
	copy := make(map[string]dynamodbtypes.AttributeValue, len(item))
	for key, value := range item {
		copy[key] = value
	}
	return copy
}
