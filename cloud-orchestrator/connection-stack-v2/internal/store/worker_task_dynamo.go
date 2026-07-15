package store

import (
	"context"
	"regexp"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

const workerTaskClaimRetryLimit = 4

type WorkerTaskRecord struct {
	ConnectionID            string
	DeploymentID            string
	TaskID                  string
	RequestSHA256           string
	BootstrapSessionID      string
	ExpectedInstanceID      string
	TaskKind                string
	ExecutionManifestDigest string
	InputDigest             string
	Status                  string
	Attempt                 int64
	LeaseEpoch              int64
	LastSequence            int64
	Checkpoint              string
	ErrorCode               string
	EvidenceDigest          string
	LastEventSHA256         string
	CreatedAt               string
	UpdatedAt               string
}

type WorkerLeaseAuthorization struct {
	ConnectionID       string
	DeploymentID       string
	BootstrapSessionID string
	ExpectedInstanceID string
	LeaseEpoch         int64
	TokenSHA256        string
	Now                string
}

type WorkerTaskEvent struct {
	TaskID         string
	Attempt        int64
	LeaseEpoch     int64
	Sequence       int64
	Status         string
	Checkpoint     string
	ErrorCode      string
	EvidenceDigest string
	OccurredAt     string
	EventSHA256    string
}

type WorkerTaskDynamoAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type WorkerTaskRepository interface {
	LookupWorkerTask(ctx context.Context, deploymentID, taskID string) (WorkerTaskRecord, bool, error)
	IssueWorkerTask(ctx context.Context, receipt Record, task WorkerTaskRecord) (storedReceipt Record, storedTask WorkerTaskRecord, created bool, err error)
	EnsureWorkerTask(ctx context.Context, task WorkerTaskRecord) (stored WorkerTaskRecord, created bool, err error)
	ClaimWorkerTask(ctx context.Context, authorization WorkerLeaseAuthorization) (task WorkerTaskRecord, found bool, advanced bool, err error)
	RecordWorkerTaskEvent(ctx context.Context, authorization WorkerLeaseAuthorization, event WorkerTaskEvent) (stored WorkerTaskRecord, idempotent bool, err error)
}

type DynamoWorkerTaskStore struct {
	client              WorkerTaskDynamoAPI
	receiptsTable       string
	countersTable       string
	workerSessionsTable string
	workerTasksTable    string
}

func NewDynamoWorkerTaskStore(client WorkerTaskDynamoAPI, receiptsTable, countersTable, workerSessionsTable, workerTasksTable string) (*DynamoWorkerTaskStore, error) {
	if client == nil || !validTableName(receiptsTable) || !validTableName(countersTable) || !validTableName(workerSessionsTable) ||
		!validTableName(workerTasksTable) || !uniqueStrings(receiptsTable, countersTable, workerSessionsTable, workerTasksTable) {
		return nil, NewError("worker_task_store_invalid")
	}
	return &DynamoWorkerTaskStore{client: client, receiptsTable: receiptsTable, countersTable: countersTable,
		workerSessionsTable: workerSessionsTable, workerTasksTable: workerTasksTable}, nil
}

func (s *DynamoWorkerTaskStore) LookupWorkerTask(ctx context.Context, deploymentID, taskID string) (WorkerTaskRecord, bool, error) {
	if s == nil || s.client == nil || !contract.ValidID(deploymentID) || !contract.ValidID(taskID) {
		return WorkerTaskRecord{}, false, NewError("worker_task_invalid")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.workerTasksTable, ConsistentRead: boolPtr(true), Key: workerTaskKey(deploymentID, taskID)})
	if err != nil {
		return WorkerTaskRecord{}, false, NewError("worker_task_unavailable")
	}
	if len(output.Item) == 0 {
		return WorkerTaskRecord{}, false, nil
	}
	task, parseErr := workerTaskFromItem(output.Item)
	if parseErr != nil || task.DeploymentID != deploymentID || task.TaskID != taskID {
		return WorkerTaskRecord{}, false, NewError("worker_task_store_invalid")
	}
	return task, true, nil
}

// IssueWorkerTask atomically fences the signed node counter, records its
// durable receipt, and reserves the deterministic task key. A successful
// receipt can therefore never exist without the exact task it describes.
func (s *DynamoWorkerTaskStore) IssueWorkerTask(ctx context.Context, receipt Record, task WorkerTaskRecord) (Record, WorkerTaskRecord, bool, error) {
	if s == nil || s.client == nil || validateRecord(receipt) != nil || receipt.Action != contract.ActionWorkerTaskIssue ||
		!validNewWorkerTask(task) || task.ConnectionID != receipt.ConnectionID || task.RequestSHA256 != receipt.RequestSHA256 {
		return Record{}, WorkerTaskRecord{}, false, NewError("worker_task_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable,
			Key:                       map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: receipt.ConnectionID}},
			UpdateExpression:          stringPtr("SET last_node_counter = :node_counter"),
			ConditionExpression:       stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(receipt.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, Item: recordItemForStore(receipt),
			ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)")}},
		{Put: &dynamodbtypes.Put{TableName: &s.workerTasksTable, Item: workerTaskItem(task),
			ConditionExpression: stringPtr("attribute_not_exists(deployment_id) AND attribute_not_exists(task_id)")}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if writeErr == nil {
		return cloneRecord(receipt), task, true, nil
	}
	storedReceipt, receiptFound, receiptErr := s.lookupWorkerTaskReceipt(ctx, receipt.ConnectionID, receipt.CommandID)
	if receiptErr != nil {
		return Record{}, WorkerTaskRecord{}, false, receiptErr
	}
	storedTask, taskFound, taskErr := s.LookupWorkerTask(ctx, task.DeploymentID, task.TaskID)
	if taskErr != nil {
		return Record{}, WorkerTaskRecord{}, false, taskErr
	}
	if receiptFound && taskFound && storedReceipt.SameIdentity(receipt) && sameWorkerTaskBinding(storedTask, task) {
		return storedReceipt, storedTask, false, nil
	}
	if receiptFound && !storedReceipt.SameIdentity(receipt) {
		return Record{}, WorkerTaskRecord{}, false, NewError("command_id_conflict")
	}
	if taskFound && !sameWorkerTaskBinding(storedTask, task) {
		return Record{}, WorkerTaskRecord{}, false, NewError("worker_task_conflict")
	}
	if receiptFound || taskFound {
		return Record{}, WorkerTaskRecord{}, false, NewError("worker_task_store_invalid")
	}
	return Record{}, WorkerTaskRecord{}, false, NewError("worker_task_unavailable")
}

func (s *DynamoWorkerTaskStore) lookupWorkerTaskReceipt(ctx context.Context, connectionID, commandID string) (Record, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.receiptsTable, ConsistentRead: boolPtr(true),
		Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: commandID}}})
	if err != nil {
		return Record{}, false, NewError("worker_task_unavailable")
	}
	if len(output.Item) == 0 {
		return Record{}, false, nil
	}
	record, parseErr := recordFromItem(output.Item)
	if parseErr != nil {
		return Record{}, false, parseErr
	}
	return record, true, nil
}

// EnsureWorkerTask is response-loss safe: every failed write is reconciled by
// a strong read of the deterministic (deployment_id, task_id) key.
func (s *DynamoWorkerTaskStore) EnsureWorkerTask(ctx context.Context, task WorkerTaskRecord) (WorkerTaskRecord, bool, error) {
	if !validNewWorkerTask(task) {
		return WorkerTaskRecord{}, false, NewError("worker_task_invalid")
	}
	put := &dynamodbtypes.Put{TableName: &s.workerTasksTable, Item: workerTaskItem(task), ConditionExpression: stringPtr("attribute_not_exists(deployment_id) AND attribute_not_exists(task_id)")}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []dynamodbtypes.TransactWriteItem{{Put: put}}})
	if writeErr == nil {
		return task, true, nil
	}
	existing, found, readErr := s.LookupWorkerTask(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return WorkerTaskRecord{}, false, readErr
	}
	if found && sameWorkerTaskBinding(existing, task) {
		return existing, false, nil
	}
	if found {
		return WorkerTaskRecord{}, false, NewError("worker_task_conflict")
	}
	return WorkerTaskRecord{}, false, NewError("worker_task_unavailable")
}

func (s *DynamoWorkerTaskStore) ClaimWorkerTask(ctx context.Context, authorization WorkerLeaseAuthorization) (WorkerTaskRecord, bool, bool, error) {
	session, err := s.authorizeWorker(ctx, authorization)
	if err != nil {
		return WorkerTaskRecord{}, false, false, err
	}
	for attempt := 0; attempt < workerTaskClaimRetryLimit; attempt++ {
		tasks, queryErr := s.queryWorkerTasks(ctx, authorization.DeploymentID)
		if queryErr != nil {
			return WorkerTaskRecord{}, false, false, queryErr
		}
		var candidate *WorkerTaskRecord
		for index := range tasks {
			if tasks[index].Status == "queued" || tasks[index].Status == "running" {
				candidate = &tasks[index]
				break
			}
		}
		if candidate == nil {
			return WorkerTaskRecord{}, false, false, nil
		}
		if !taskBindsWorker(*candidate, session) || candidate.LeaseEpoch > authorization.LeaseEpoch {
			return WorkerTaskRecord{}, false, false, NewError("worker_task_unauthorized")
		}
		if candidate.LeaseEpoch == authorization.LeaseEpoch {
			return *candidate, true, false, nil
		}
		created, claimErr := s.advanceWorkerTaskClaim(ctx, *candidate, authorization)
		if claimErr == nil {
			return created, true, true, nil
		}
		if Code(claimErr) != "worker_task_claim_race" {
			return WorkerTaskRecord{}, false, false, claimErr
		}
	}
	return WorkerTaskRecord{}, false, false, NewError("worker_task_unavailable")
}

func (s *DynamoWorkerTaskStore) RecordWorkerTaskEvent(ctx context.Context, authorization WorkerLeaseAuthorization, event WorkerTaskEvent) (WorkerTaskRecord, bool, error) {
	session, err := s.authorizeWorker(ctx, authorization)
	if err != nil {
		return WorkerTaskRecord{}, false, err
	}
	task, found, err := s.LookupWorkerTask(ctx, authorization.DeploymentID, event.TaskID)
	if err != nil || !found {
		if err != nil {
			return WorkerTaskRecord{}, false, err
		}
		return WorkerTaskRecord{}, false, NewError("worker_task_not_found")
	}
	if !taskBindsWorker(task, session) || task.LeaseEpoch != authorization.LeaseEpoch {
		return WorkerTaskRecord{}, false, NewError("worker_task_event_invalid")
	}
	if task.Attempt == event.Attempt && task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	if !validWorkerTaskEvent(task, event) {
		return WorkerTaskRecord{}, false, NewError("worker_task_event_invalid")
	}
	values, updateExpression, stateCondition := workerTaskEventUpdate(task, event, authorization.Now)
	items := []dynamodbtypes.TransactWriteItem{
		{ConditionCheck: workerSessionCondition(s.workerSessionsTable, authorization)},
		{Update: &dynamodbtypes.Update{TableName: &s.workerTasksTable, Key: workerTaskKey(task.DeploymentID, task.TaskID), ConditionExpression: stringPtr("connection_id = :connection_id AND bootstrap_session_id = :bootstrap_session_id AND expected_instance_id = :instance_id AND " + stateCondition + " AND attempt = :attempt AND lease_epoch = :lease_epoch AND last_sequence = :previous_sequence"), UpdateExpression: stringPtr(updateExpression), ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, storedFound, readErr := s.LookupWorkerTask(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return WorkerTaskRecord{}, false, readErr
	}
	if storedFound && sameWorkerTaskLease(stored, task, authorization.LeaseEpoch) && stored.LastSequence == event.Sequence && stored.LastEventSHA256 == event.EventSHA256 {
		return stored, writeErr != nil, nil
	}
	if writeErr != nil {
		return WorkerTaskRecord{}, false, NewError("worker_task_event_conflict")
	}
	return WorkerTaskRecord{}, false, NewError("worker_task_store_invalid")
}

func (s *DynamoWorkerTaskStore) advanceWorkerTaskClaim(ctx context.Context, task WorkerTaskRecord, authorization WorkerLeaseAuthorization) (WorkerTaskRecord, error) {
	values := map[string]dynamodbtypes.AttributeValue{
		":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ConnectionID}, ":bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.BootstrapSessionID}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ExpectedInstanceID},
		":status": &dynamodbtypes.AttributeValueMemberS{Value: task.Status}, ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.Attempt, 10)}, ":previous_lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LeaseEpoch, 10)}, ":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(authorization.LeaseEpoch, 10)}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: authorization.Now},
	}
	updateExpression := "SET lease_epoch = :lease_epoch, updated_at = :now"
	advanceAttempt := task.LeaseEpoch > 0 && task.Status == "running"
	if advanceAttempt {
		updateExpression = "SET attempt = attempt + :one, lease_epoch = :lease_epoch, updated_at = :now"
		values[":one"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
	}
	items := []dynamodbtypes.TransactWriteItem{
		{ConditionCheck: workerSessionCondition(s.workerSessionsTable, authorization)},
		{Update: &dynamodbtypes.Update{TableName: &s.workerTasksTable, Key: workerTaskKey(task.DeploymentID, task.TaskID), ConditionExpression: stringPtr("connection_id = :connection_id AND bootstrap_session_id = :bootstrap_session_id AND expected_instance_id = :instance_id AND #status = :status AND attempt = :attempt AND lease_epoch = :previous_lease_epoch"), UpdateExpression: stringPtr(updateExpression), ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, found, readErr := s.LookupWorkerTask(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return WorkerTaskRecord{}, readErr
	}
	expectedAttempt := task.Attempt
	if advanceAttempt {
		expectedAttempt++
	}
	if found && sameWorkerTaskBinding(stored, task) && stored.LeaseEpoch == authorization.LeaseEpoch && stored.Attempt == expectedAttempt {
		return stored, nil
	}
	if writeErr != nil {
		return WorkerTaskRecord{}, NewError("worker_task_claim_race")
	}
	return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
}

func (s *DynamoWorkerTaskStore) authorizeWorker(ctx context.Context, authorization WorkerLeaseAuthorization) (WorkerSession, error) {
	if !validWorkerAuthorization(authorization) {
		return WorkerSession{}, NewError("worker_task_unauthorized")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.workerSessionsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.BootstrapSessionID}}})
	if err != nil {
		return WorkerSession{}, NewError("worker_task_unavailable")
	}
	session, parseErr := workerSessionFromItem(output.Item)
	if parseErr != nil || session.State != "active" || session.ConnectionID != authorization.ConnectionID || session.DeploymentID != authorization.DeploymentID || session.BootstrapSessionID != authorization.BootstrapSessionID || session.ExpectedInstanceID != authorization.ExpectedInstanceID || session.LeaseEpoch != authorization.LeaseEpoch || session.TokenSHA256 != authorization.TokenSHA256 || session.LeaseExpiresAt <= authorization.Now {
		return WorkerSession{}, NewError("worker_task_unauthorized")
	}
	return session, nil
}

func workerSessionCondition(table string, authorization WorkerLeaseAuthorization) *dynamodbtypes.ConditionCheck {
	return &dynamodbtypes.ConditionCheck{TableName: &table, Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.BootstrapSessionID}}, ConditionExpression: stringPtr("#state = :active AND connection_id = :connection_id AND deployment_id = :deployment_id AND expected_instance_id = :instance_id AND lease_epoch = :lease_epoch AND token_sha256 = :token_sha256 AND lease_expires_at > :now"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
		":active": &dynamodbtypes.AttributeValueMemberS{Value: "active"}, ":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ConnectionID}, ":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.DeploymentID}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ExpectedInstanceID}, ":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(authorization.LeaseEpoch, 10)}, ":token_sha256": &dynamodbtypes.AttributeValueMemberS{Value: authorization.TokenSHA256}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: authorization.Now},
	}}
}

func (s *DynamoWorkerTaskStore) queryWorkerTasks(ctx context.Context, deploymentID string) ([]WorkerTaskRecord, error) {
	var tasks []WorkerTaskRecord
	var cursor map[string]dynamodbtypes.AttributeValue
	for pages := 0; ; pages++ {
		if pages >= 100 {
			return nil, NewError("worker_task_unavailable")
		}
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{TableName: &s.workerTasksTable, ConsistentRead: boolPtr(true), KeyConditionExpression: stringPtr("deployment_id = :deployment_id"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}}, ExclusiveStartKey: cursor})
		if err != nil {
			return nil, NewError("worker_task_unavailable")
		}
		for _, item := range output.Items {
			task, parseErr := workerTaskFromItem(item)
			if parseErr != nil || task.DeploymentID != deploymentID {
				return nil, NewError("worker_task_store_invalid")
			}
			tasks = append(tasks, task)
		}
		if len(output.LastEvaluatedKey) == 0 {
			break
		}
		cursor = output.LastEvaluatedKey
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].TaskID < tasks[j].TaskID })
	return tasks, nil
}

func workerTaskEventUpdate(task WorkerTaskRecord, event WorkerTaskEvent, now string) (map[string]dynamodbtypes.AttributeValue, string, string) {
	values := map[string]dynamodbtypes.AttributeValue{
		":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ConnectionID}, ":bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: task.BootstrapSessionID}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExpectedInstanceID}, ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Attempt, 10)}, ":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.LeaseEpoch, 10)}, ":previous_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Sequence-1, 10)}, ":sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Sequence, 10)}, ":event_status": &dynamodbtypes.AttributeValueMemberS{Value: event.Status}, ":event_sha256": &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: now},
	}
	sets := "SET #status = :event_status, last_sequence = :sequence, last_event_sha256 = :event_sha256, updated_at = :now"
	removes := ""
	for name, value := range map[string]string{"checkpoint": event.Checkpoint, "error_code": event.ErrorCode, "evidence_digest": event.EvidenceDigest} {
		if value == "" {
			if removes == "" {
				removes = " REMOVE " + name
			} else {
				removes += ", " + name
			}
		} else {
			sets += ", " + name + " = :" + name
			values[":"+name] = &dynamodbtypes.AttributeValueMemberS{Value: value}
		}
	}
	state := "(#status = :queued OR #status = :running)"
	if event.Status == "running" {
		state = "#status = :queued"
		values[":queued"] = &dynamodbtypes.AttributeValueMemberS{Value: "queued"}
	}
	if event.Status == "succeeded" {
		state = "#status = :running"
		values[":running"] = &dynamodbtypes.AttributeValueMemberS{Value: "running"}
	}
	if event.Status == "failed" || event.Status == "interrupted" {
		values[":queued"] = &dynamodbtypes.AttributeValueMemberS{Value: "queued"}
		values[":running"] = &dynamodbtypes.AttributeValueMemberS{Value: "running"}
	}
	return values, sets + removes, state
}

func workerTaskKey(deploymentID, taskID string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: taskID}}
}

func workerTaskItem(task WorkerTaskRecord) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: task.DeploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: task.TaskID}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ConnectionID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: task.RequestSHA256}, "bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: task.BootstrapSessionID}, "expected_instance_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExpectedInstanceID}, "task_kind": &dynamodbtypes.AttributeValueMemberS{Value: task.TaskKind}, "execution_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.ExecutionManifestDigest}, "input_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.InputDigest}, "status": &dynamodbtypes.AttributeValueMemberS{Value: task.Status}, "attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.Attempt, 10)}, "lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LeaseEpoch, 10)}, "last_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LastSequence, 10)}, "created_at": &dynamodbtypes.AttributeValueMemberS{Value: task.CreatedAt}, "updated_at": &dynamodbtypes.AttributeValueMemberS{Value: task.UpdatedAt}}
}

func workerTaskFromItem(item map[string]dynamodbtypes.AttributeValue) (WorkerTaskRecord, error) {
	allowed := map[string]bool{"deployment_id": true, "task_id": true, "connection_id": true, "request_sha256": true, "bootstrap_session_id": true, "expected_instance_id": true, "task_kind": true, "execution_manifest_digest": true, "input_digest": true, "status": true, "attempt": true, "lease_epoch": true, "last_sequence": true, "checkpoint": true, "error_code": true, "evidence_digest": true, "last_event_sha256": true, "created_at": true, "updated_at": true}
	for name := range item {
		if !allowed[name] {
			return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
		}
	}
	var task WorkerTaskRecord
	stringsByName := map[string]*string{"deployment_id": &task.DeploymentID, "task_id": &task.TaskID, "connection_id": &task.ConnectionID, "request_sha256": &task.RequestSHA256, "bootstrap_session_id": &task.BootstrapSessionID, "expected_instance_id": &task.ExpectedInstanceID, "task_kind": &task.TaskKind, "execution_manifest_digest": &task.ExecutionManifestDigest, "input_digest": &task.InputDigest, "status": &task.Status, "created_at": &task.CreatedAt, "updated_at": &task.UpdatedAt}
	for name, target := range stringsByName {
		value, err := stringAttribute(item, name)
		if err != nil {
			return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
		}
		*target = value
	}
	var err error
	task.Attempt, err = numberAttribute(item, "attempt", false)
	if err != nil {
		return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
	}
	task.LeaseEpoch, err = numberAttribute(item, "lease_epoch", true)
	if err != nil {
		return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
	}
	task.LastSequence, err = numberAttribute(item, "last_sequence", true)
	if err != nil {
		return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
	}
	for name, target := range map[string]*string{"checkpoint": &task.Checkpoint, "error_code": &task.ErrorCode, "evidence_digest": &task.EvidenceDigest, "last_event_sha256": &task.LastEventSHA256} {
		if value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS); ok {
			*target = value.Value
		}
	}
	if !validStoredWorkerTask(task) {
		return WorkerTaskRecord{}, NewError("worker_task_store_invalid")
	}
	return task, nil
}

func validNewWorkerTask(task WorkerTaskRecord) bool {
	return validStoredWorkerTask(task) && task.Status == "queued" && task.Attempt == 1 && task.LeaseEpoch == 0 && task.LastSequence == 0 && task.Checkpoint == "" && task.ErrorCode == "" && task.EvidenceDigest == "" && task.LastEventSHA256 == "" && task.CreatedAt == task.UpdatedAt
}

func validStoredWorkerTask(task WorkerTaskRecord) bool {
	if !contract.ValidConnectionID(task.ConnectionID) || !contract.ValidID(task.DeploymentID) || !contract.ValidID(task.TaskID) || !validSHA256(task.RequestSHA256) || !contract.ValidID(task.BootstrapSessionID) || !workerInstancePattern.MatchString(task.ExpectedInstanceID) || task.TaskKind != "execution_probe" || !workerNamedDigestPattern.MatchString(task.ExecutionManifestDigest) || !workerNamedDigestPattern.MatchString(task.InputDigest) || task.Attempt < 1 || task.LeaseEpoch < 0 || task.LastSequence < 0 || !canonicalWorkerEventInstant(task.CreatedAt) || !canonicalWorkerEventInstant(task.UpdatedAt) || task.UpdatedAt < task.CreatedAt {
		return false
	}
	if (task.Checkpoint != "" && !workerTaskCodePattern.MatchString(task.Checkpoint)) || (task.ErrorCode != "" && !workerTaskCodePattern.MatchString(task.ErrorCode)) || (task.EvidenceDigest != "" && !workerNamedDigestPattern.MatchString(task.EvidenceDigest)) || (task.LastSequence == 0) != (task.LastEventSHA256 == "") {
		return false
	}
	switch task.Status {
	case "queued":
		return task.Attempt == 1 && task.LastSequence == 0 && task.Checkpoint == "" && task.ErrorCode == "" && task.EvidenceDigest == ""
	case "running":
		return task.LastSequence > 0 && task.Checkpoint == "execution_manifest_received" && task.ErrorCode == "" && task.EvidenceDigest == task.ExecutionManifestDigest
	case "succeeded":
		return task.LastSequence > 0 && task.Checkpoint == "task_transport_verified" && task.ErrorCode == "" && task.EvidenceDigest == task.ExecutionManifestDigest
	case "failed", "interrupted":
		return task.LastSequence > 0 && task.Checkpoint == "" && task.ErrorCode != "" && task.EvidenceDigest == ""
	default:
		return false
	}
}

func validWorkerAuthorization(value WorkerLeaseAuthorization) bool {
	return contract.ValidConnectionID(value.ConnectionID) && contract.ValidID(value.DeploymentID) && contract.ValidID(value.BootstrapSessionID) && workerInstancePattern.MatchString(value.ExpectedInstanceID) && value.LeaseEpoch > 0 && validSHA256(value.TokenSHA256) && canonicalWorkerEventInstant(value.Now)
}

func validWorkerTaskEvent(task WorkerTaskRecord, event WorkerTaskEvent) bool {
	if event.TaskID != task.TaskID || event.Attempt != task.Attempt || event.LeaseEpoch != task.LeaseEpoch || event.Sequence < 1 || event.Sequence != task.LastSequence+1 || !canonicalWorkerEventInstant(event.OccurredAt) || !validSHA256(event.EventSHA256) || (event.Checkpoint != "" && !workerTaskCodePattern.MatchString(event.Checkpoint)) || (event.ErrorCode != "" && !workerTaskCodePattern.MatchString(event.ErrorCode)) {
		return false
	}
	switch event.Status {
	case "running":
		return task.Status == "queued" && event.Checkpoint == "execution_manifest_received" && event.ErrorCode == "" && event.EvidenceDigest == task.ExecutionManifestDigest
	case "succeeded":
		return task.Status == "running" && event.Checkpoint == "task_transport_verified" && event.ErrorCode == "" && event.EvidenceDigest == task.ExecutionManifestDigest
	case "failed", "interrupted":
		return (task.Status == "queued" || task.Status == "running") && event.Checkpoint == "" && event.ErrorCode != "" && event.EvidenceDigest == ""
	default:
		return false
	}
}

func sameWorkerTaskBinding(left, right WorkerTaskRecord) bool {
	return left.ConnectionID == right.ConnectionID && left.DeploymentID == right.DeploymentID && left.TaskID == right.TaskID && left.RequestSHA256 == right.RequestSHA256 && left.BootstrapSessionID == right.BootstrapSessionID && left.ExpectedInstanceID == right.ExpectedInstanceID && left.TaskKind == right.TaskKind && left.ExecutionManifestDigest == right.ExecutionManifestDigest && left.InputDigest == right.InputDigest
}

func sameWorkerTaskLease(stored, previous WorkerTaskRecord, leaseEpoch int64) bool {
	return sameWorkerTaskBinding(stored, previous) && stored.Attempt == previous.Attempt && stored.LeaseEpoch == leaseEpoch
}
func taskBindsWorker(task WorkerTaskRecord, session WorkerSession) bool {
	return task.ConnectionID == session.ConnectionID && task.DeploymentID == session.DeploymentID && task.BootstrapSessionID == session.BootstrapSessionID && task.ExpectedInstanceID == session.ExpectedInstanceID
}

var workerTaskCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)
