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

const recipeTaskRecordKind = "recipe_execution_v1"

var recipeExecutionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type RecipeTaskRecord struct {
	ConnectionID                  string
	DeploymentID                  string
	TaskID                        string
	RequestSHA256                 string
	BootstrapSessionID            string
	ExpectedInstanceID            string
	ExecutionID                   string
	TaskKind                      string
	RecipeExecutionManifestDigest string
	InputDigest                   string
	CheckpointSequence            []string
	ManifestJSON                  []byte
	Status                        string
	Attempt                       int64
	LeaseEpoch                    int64
	LastSequence                  int64
	LastCheckpoint                string
	ErrorCode                     string
	EvidenceDigest                string
	LastEventSHA256               string
	CreatedAt                     string
	UpdatedAt                     string
}

type RecipeTaskEvent struct {
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

type RecipeTaskRepository interface {
	LookupRecipeTask(ctx context.Context, deploymentID, taskID string) (RecipeTaskRecord, bool, error)
	IssueRecipeTask(ctx context.Context, receipt Record, task RecipeTaskRecord) (storedReceipt Record, storedTask RecipeTaskRecord, created bool, err error)
	ClaimRecipeTask(ctx context.Context, authorization WorkerLeaseAuthorization) (task RecipeTaskRecord, found bool, advanced bool, err error)
	RecordRecipeTaskEvent(ctx context.Context, authorization WorkerLeaseAuthorization, event RecipeTaskEvent) (stored RecipeTaskRecord, idempotent bool, err error)
}

func (s *DynamoWorkerTaskStore) LookupRecipeTask(ctx context.Context, deploymentID, taskID string) (RecipeTaskRecord, bool, error) {
	if s == nil || s.client == nil || !contract.ValidID(deploymentID) || !recipeTaskIDPatternStore.MatchString(taskID) {
		return RecipeTaskRecord{}, false, NewError("recipe_task_invalid")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.workerTasksTable, ConsistentRead: boolPtr(true), Key: workerTaskKey(deploymentID, taskID)})
	if err != nil {
		return RecipeTaskRecord{}, false, NewError("recipe_task_unavailable")
	}
	if len(output.Item) == 0 {
		return RecipeTaskRecord{}, false, nil
	}
	task, parseErr := recipeTaskFromItem(output.Item)
	if parseErr != nil || task.DeploymentID != deploymentID || task.TaskID != taskID {
		return RecipeTaskRecord{}, false, NewError("recipe_task_store_invalid")
	}
	return task, true, nil
}

func (s *DynamoWorkerTaskStore) IssueRecipeTask(ctx context.Context, receipt Record, task RecipeTaskRecord) (Record, RecipeTaskRecord, bool, error) {
	if s == nil || s.client == nil || validateRecord(receipt) != nil || receipt.Action != contract.ActionWorkerRecipeTaskIssue ||
		!validNewRecipeTask(task) || task.ConnectionID != receipt.ConnectionID || task.RequestSHA256 != receipt.RequestSHA256 {
		return Record{}, RecipeTaskRecord{}, false, NewError("recipe_task_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: receipt.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(receipt.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, Item: recordItemForStore(receipt), ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)")}},
		{Put: &dynamodbtypes.Put{TableName: &s.workerTasksTable, Item: recipeTaskItem(task), ConditionExpression: stringPtr("attribute_not_exists(deployment_id) AND attribute_not_exists(task_id)")}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if writeErr == nil {
		return cloneRecord(receipt), cloneRecipeTask(task), true, nil
	}
	storedReceipt, receiptFound, receiptErr := s.lookupWorkerTaskReceipt(ctx, receipt.ConnectionID, receipt.CommandID)
	if receiptErr != nil {
		return Record{}, RecipeTaskRecord{}, false, receiptErr
	}
	storedTask, taskFound, taskErr := s.LookupRecipeTask(ctx, task.DeploymentID, task.TaskID)
	if taskErr != nil {
		return Record{}, RecipeTaskRecord{}, false, taskErr
	}
	if receiptFound && taskFound && storedReceipt.SameIdentity(receipt) && sameRecipeTaskBinding(storedTask, task) {
		return storedReceipt, storedTask, false, nil
	}
	if receiptFound && !storedReceipt.SameIdentity(receipt) {
		return Record{}, RecipeTaskRecord{}, false, NewError("command_id_conflict")
	}
	if taskFound && !sameRecipeTaskBinding(storedTask, task) {
		return Record{}, RecipeTaskRecord{}, false, NewError("recipe_task_conflict")
	}
	if receiptFound || taskFound {
		return Record{}, RecipeTaskRecord{}, false, NewError("recipe_task_store_invalid")
	}
	return Record{}, RecipeTaskRecord{}, false, NewError("recipe_task_unavailable")
}

func (s *DynamoWorkerTaskStore) ClaimRecipeTask(ctx context.Context, authorization WorkerLeaseAuthorization) (RecipeTaskRecord, bool, bool, error) {
	session, err := s.authorizeWorker(ctx, authorization)
	if err != nil {
		return RecipeTaskRecord{}, false, false, err
	}
	for attempt := 0; attempt < workerTaskClaimRetryLimit; attempt++ {
		tasks, queryErr := s.queryRecipeTasks(ctx, authorization.DeploymentID)
		if queryErr != nil {
			return RecipeTaskRecord{}, false, false, queryErr
		}
		var candidate *RecipeTaskRecord
		for index := range tasks {
			if tasks[index].Status == "queued" || tasks[index].Status == "running" {
				candidate = &tasks[index]
				break
			}
		}
		if candidate == nil {
			return RecipeTaskRecord{}, false, false, nil
		}
		if !recipeTaskBindsWorker(*candidate, session) || candidate.LeaseEpoch > authorization.LeaseEpoch {
			return RecipeTaskRecord{}, false, false, NewError("recipe_task_unauthorized")
		}
		if candidate.LeaseEpoch == authorization.LeaseEpoch {
			return cloneRecipeTask(*candidate), true, false, nil
		}
		claimed, claimErr := s.advanceRecipeTaskClaim(ctx, *candidate, authorization)
		if claimErr == nil {
			return claimed, true, true, nil
		}
		if Code(claimErr) != "recipe_task_claim_race" {
			return RecipeTaskRecord{}, false, false, claimErr
		}
	}
	return RecipeTaskRecord{}, false, false, NewError("recipe_task_unavailable")
}

func (s *DynamoWorkerTaskStore) RecordRecipeTaskEvent(ctx context.Context, authorization WorkerLeaseAuthorization, event RecipeTaskEvent) (RecipeTaskRecord, bool, error) {
	session, err := s.authorizeWorker(ctx, authorization)
	if err != nil {
		return RecipeTaskRecord{}, false, err
	}
	task, found, err := s.LookupRecipeTask(ctx, authorization.DeploymentID, event.TaskID)
	if err != nil || !found {
		if err != nil {
			return RecipeTaskRecord{}, false, err
		}
		return RecipeTaskRecord{}, false, NewError("recipe_task_not_found")
	}
	if !recipeTaskBindsWorker(task, session) || task.LeaseEpoch != authorization.LeaseEpoch {
		return RecipeTaskRecord{}, false, NewError("recipe_task_event_invalid")
	}
	if task.Attempt == event.Attempt && task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	if !validRecipeTaskEvent(task, event) {
		return RecipeTaskRecord{}, false, NewError("recipe_task_event_invalid")
	}
	values, updateExpression, stateCondition := recipeTaskEventUpdate(task, event, authorization.Now)
	items := []dynamodbtypes.TransactWriteItem{
		{ConditionCheck: workerSessionCondition(s.workerSessionsTable, authorization)},
		{Update: &dynamodbtypes.Update{TableName: &s.workerTasksTable, Key: workerTaskKey(task.DeploymentID, task.TaskID),
			ConditionExpression: stringPtr("record_kind = :record_kind AND connection_id = :connection_id AND bootstrap_session_id = :bootstrap_session_id AND expected_instance_id = :instance_id AND execution_id = :execution_id AND recipe_execution_manifest_digest = :manifest_digest AND checkpoint_sequence = :checkpoint_sequence AND " + stateCondition + " AND attempt = :attempt AND lease_epoch = :lease_epoch AND last_sequence = :previous_sequence"),
			UpdateExpression:    updateExpression, ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, storedFound, readErr := s.LookupRecipeTask(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return RecipeTaskRecord{}, false, readErr
	}
	if storedFound && sameRecipeTaskLease(stored, task, authorization.LeaseEpoch) && stored.LastSequence == event.Sequence && stored.LastEventSHA256 == event.EventSHA256 {
		return stored, writeErr != nil, nil
	}
	if writeErr != nil {
		return RecipeTaskRecord{}, false, NewError("recipe_task_event_conflict")
	}
	return RecipeTaskRecord{}, false, NewError("recipe_task_store_invalid")
}

func (s *DynamoWorkerTaskStore) advanceRecipeTaskClaim(ctx context.Context, task RecipeTaskRecord, authorization WorkerLeaseAuthorization) (RecipeTaskRecord, error) {
	values := map[string]dynamodbtypes.AttributeValue{
		":record_kind": &dynamodbtypes.AttributeValueMemberS{Value: recipeTaskRecordKind}, ":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ConnectionID}, ":bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.BootstrapSessionID}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: authorization.ExpectedInstanceID}, ":execution_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExecutionID}, ":manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.RecipeExecutionManifestDigest}, ":checkpoint_sequence": recipeCheckpointAttribute(task.CheckpointSequence), ":status": &dynamodbtypes.AttributeValueMemberS{Value: task.Status}, ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.Attempt, 10)}, ":previous_lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LeaseEpoch, 10)}, ":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(authorization.LeaseEpoch, 10)}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: authorization.Now},
	}
	advanceAttempt := task.LeaseEpoch > 0 && task.Status == "running"
	updateExpression := "SET lease_epoch = :lease_epoch, updated_at = :now"
	if advanceAttempt {
		values[":one"] = &dynamodbtypes.AttributeValueMemberN{Value: "1"}
		updateExpression = "SET attempt = attempt + :one, lease_epoch = :lease_epoch, updated_at = :now"
	}
	condition := "record_kind = :record_kind AND connection_id = :connection_id AND bootstrap_session_id = :bootstrap_session_id AND expected_instance_id = :instance_id AND execution_id = :execution_id AND recipe_execution_manifest_digest = :manifest_digest AND checkpoint_sequence = :checkpoint_sequence AND #status = :status AND attempt = :attempt AND lease_epoch = :previous_lease_epoch"
	items := []dynamodbtypes.TransactWriteItem{{ConditionCheck: workerSessionCondition(s.workerSessionsTable, authorization)}, {Update: &dynamodbtypes.Update{TableName: &s.workerTasksTable, Key: workerTaskKey(task.DeploymentID, task.TaskID), ConditionExpression: stringPtr(condition), UpdateExpression: stringPtr(updateExpression), ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}}}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, found, readErr := s.LookupRecipeTask(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return RecipeTaskRecord{}, readErr
	}
	expectedAttempt := task.Attempt
	if advanceAttempt {
		expectedAttempt++
	}
	if found && sameRecipeTaskBinding(stored, task) && stored.LeaseEpoch == authorization.LeaseEpoch && stored.Attempt == expectedAttempt {
		return stored, nil
	}
	if writeErr != nil {
		return RecipeTaskRecord{}, NewError("recipe_task_claim_race")
	}
	return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
}

func (s *DynamoWorkerTaskStore) queryRecipeTasks(ctx context.Context, deploymentID string) ([]RecipeTaskRecord, error) {
	var tasks []RecipeTaskRecord
	var cursor map[string]dynamodbtypes.AttributeValue
	for pages := 0; ; pages++ {
		if pages >= 100 {
			return nil, NewError("recipe_task_unavailable")
		}
		output, err := s.client.Query(ctx, &dynamodb.QueryInput{TableName: &s.workerTasksTable, ConsistentRead: boolPtr(true), KeyConditionExpression: stringPtr("deployment_id = :deployment_id"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}}, ExclusiveStartKey: cursor})
		if err != nil {
			return nil, NewError("recipe_task_unavailable")
		}
		for _, item := range output.Items {
			if kind, ok := item["record_kind"].(*dynamodbtypes.AttributeValueMemberS); !ok || kind.Value != recipeTaskRecordKind {
				continue
			}
			task, parseErr := recipeTaskFromItem(item)
			if parseErr != nil || task.DeploymentID != deploymentID {
				return nil, NewError("recipe_task_store_invalid")
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

func recipeTaskItem(task RecipeTaskRecord) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"record_kind": &dynamodbtypes.AttributeValueMemberS{Value: recipeTaskRecordKind}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: task.DeploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: task.TaskID}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ConnectionID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: task.RequestSHA256}, "bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: task.BootstrapSessionID}, "expected_instance_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExpectedInstanceID}, "execution_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExecutionID}, "task_kind": &dynamodbtypes.AttributeValueMemberS{Value: task.TaskKind}, "recipe_execution_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.RecipeExecutionManifestDigest}, "input_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.InputDigest}, "checkpoint_sequence": recipeCheckpointAttribute(task.CheckpointSequence), "manifest_json": &dynamodbtypes.AttributeValueMemberS{Value: string(task.ManifestJSON)}, "status": &dynamodbtypes.AttributeValueMemberS{Value: task.Status}, "attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.Attempt, 10)}, "lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LeaseEpoch, 10)}, "last_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(task.LastSequence, 10)}, "created_at": &dynamodbtypes.AttributeValueMemberS{Value: task.CreatedAt}, "updated_at": &dynamodbtypes.AttributeValueMemberS{Value: task.UpdatedAt},
	}
}

func recipeTaskFromItem(item map[string]dynamodbtypes.AttributeValue) (RecipeTaskRecord, error) {
	allowed := map[string]bool{"record_kind": true, "deployment_id": true, "task_id": true, "connection_id": true, "request_sha256": true, "bootstrap_session_id": true, "expected_instance_id": true, "execution_id": true, "task_kind": true, "recipe_execution_manifest_digest": true, "input_digest": true, "checkpoint_sequence": true, "manifest_json": true, "status": true, "attempt": true, "lease_epoch": true, "last_sequence": true, "last_checkpoint": true, "error_code": true, "evidence_digest": true, "last_event_sha256": true, "created_at": true, "updated_at": true}
	for name := range item {
		if !allowed[name] {
			return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
		}
	}
	var task RecipeTaskRecord
	values := map[string]*string{"deployment_id": &task.DeploymentID, "task_id": &task.TaskID, "connection_id": &task.ConnectionID, "request_sha256": &task.RequestSHA256, "bootstrap_session_id": &task.BootstrapSessionID, "expected_instance_id": &task.ExpectedInstanceID, "execution_id": &task.ExecutionID, "task_kind": &task.TaskKind, "recipe_execution_manifest_digest": &task.RecipeExecutionManifestDigest, "input_digest": &task.InputDigest, "status": &task.Status, "created_at": &task.CreatedAt, "updated_at": &task.UpdatedAt}
	for name, target := range values {
		value, err := stringAttribute(item, name)
		if err != nil {
			return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
		}
		*target = value
	}
	kind, err := stringAttribute(item, "record_kind")
	if err != nil || kind != recipeTaskRecordKind {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	checkpointList, ok := item["checkpoint_sequence"].(*dynamodbtypes.AttributeValueMemberL)
	if !ok || len(checkpointList.Value) == 0 || len(checkpointList.Value) > 32 {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	for _, raw := range checkpointList.Value {
		value, ok := raw.(*dynamodbtypes.AttributeValueMemberS)
		if !ok {
			return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
		}
		task.CheckpointSequence = append(task.CheckpointSequence, value.Value)
	}
	manifestJSON, manifestErr := stringAttribute(item, "manifest_json")
	if manifestErr != nil {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	task.ManifestJSON = []byte(manifestJSON)
	task.Attempt, err = numberAttribute(item, "attempt", false)
	if err != nil {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	task.LeaseEpoch, err = numberAttribute(item, "lease_epoch", true)
	if err != nil {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	task.LastSequence, err = numberAttribute(item, "last_sequence", true)
	if err != nil {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	for name, target := range map[string]*string{"last_checkpoint": &task.LastCheckpoint, "error_code": &task.ErrorCode, "evidence_digest": &task.EvidenceDigest, "last_event_sha256": &task.LastEventSHA256} {
		if value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS); ok {
			*target = value.Value
		}
	}
	if !validStoredRecipeTask(task) {
		return RecipeTaskRecord{}, NewError("recipe_task_store_invalid")
	}
	return task, nil
}

func recipeTaskEventUpdate(task RecipeTaskRecord, event RecipeTaskEvent, now string) (map[string]dynamodbtypes.AttributeValue, *string, string) {
	values := map[string]dynamodbtypes.AttributeValue{
		":record_kind": &dynamodbtypes.AttributeValueMemberS{Value: recipeTaskRecordKind}, ":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ConnectionID}, ":bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: task.BootstrapSessionID}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExpectedInstanceID}, ":execution_id": &dynamodbtypes.AttributeValueMemberS{Value: task.ExecutionID}, ":manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: task.RecipeExecutionManifestDigest}, ":checkpoint_sequence": recipeCheckpointAttribute(task.CheckpointSequence), ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Attempt, 10)}, ":lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.LeaseEpoch, 10)}, ":previous_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Sequence-1, 10)}, ":sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Sequence, 10)}, ":event_status": &dynamodbtypes.AttributeValueMemberS{Value: event.Status}, ":event_sha256": &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: now},
	}
	update := "SET #status = :event_status, last_sequence = :sequence, last_event_sha256 = :event_sha256, updated_at = :now"
	remove := ""
	if event.Checkpoint != "" {
		values[":last_checkpoint"] = &dynamodbtypes.AttributeValueMemberS{Value: event.Checkpoint}
		update += ", last_checkpoint = :last_checkpoint"
	}
	if event.ErrorCode != "" {
		values[":error_code"] = &dynamodbtypes.AttributeValueMemberS{Value: event.ErrorCode}
		update += ", error_code = :error_code"
	} else {
		remove = " REMOVE error_code"
	}
	if event.EvidenceDigest != "" {
		values[":evidence_digest"] = &dynamodbtypes.AttributeValueMemberS{Value: event.EvidenceDigest}
		update += ", evidence_digest = :evidence_digest"
	} else {
		if remove == "" {
			remove = " REMOVE evidence_digest"
		} else {
			remove += ", evidence_digest"
		}
	}
	state := "(#status = :queued OR #status = :running)"
	values[":queued"] = &dynamodbtypes.AttributeValueMemberS{Value: "queued"}
	values[":running"] = &dynamodbtypes.AttributeValueMemberS{Value: "running"}
	return values, stringPtr(update + remove), state
}

func validNewRecipeTask(task RecipeTaskRecord) bool {
	return validStoredRecipeTask(task) && task.Status == "queued" && task.Attempt == 1 && task.LeaseEpoch == 0 && task.LastSequence == 0 && task.LastCheckpoint == "" && task.ErrorCode == "" && task.EvidenceDigest == "" && task.LastEventSHA256 == "" && task.CreatedAt == task.UpdatedAt
}

func validStoredRecipeTask(task RecipeTaskRecord) bool {
	if !contract.ValidConnectionID(task.ConnectionID) || !contract.ValidID(task.DeploymentID) || !recipeTaskIDPatternStore.MatchString(task.TaskID) || !validSHA256(task.RequestSHA256) || !contract.ValidID(task.BootstrapSessionID) || !workerInstancePattern.MatchString(task.ExpectedInstanceID) || !recipeExecutionIDPattern.MatchString(task.ExecutionID) || task.TaskKind != contract.RecipeTaskKindExecution || !workerNamedDigestPattern.MatchString(task.RecipeExecutionManifestDigest) || !workerNamedDigestPattern.MatchString(task.InputDigest) || !validRecipeCheckpointSequence(task.CheckpointSequence) || task.Attempt < 1 || task.LeaseEpoch < 0 || task.LastSequence < 0 || !canonicalWorkerEventInstant(task.CreatedAt) || !canonicalWorkerEventInstant(task.UpdatedAt) || task.UpdatedAt < task.CreatedAt || (task.LastCheckpoint != "" && recipeCheckpointIndexStore(task.CheckpointSequence, task.LastCheckpoint) < 0) || (task.ErrorCode != "" && !workerTaskCodePattern.MatchString(task.ErrorCode)) || (task.EvidenceDigest != "" && !workerNamedDigestPattern.MatchString(task.EvidenceDigest)) || (task.LastSequence == 0) != (task.LastEventSHA256 == "") {
		return false
	}
	manifest, err := contract.ParseRecipeExecutionManifestJSON(task.ManifestJSON)
	digest, digestErr := manifest.Digest()
	if err != nil || digestErr != nil || digest != task.RecipeExecutionManifestDigest || manifest.ExecutionID != task.ExecutionID || manifest.DeploymentID != task.DeploymentID || !sameRecipeCheckpoints(manifest.CheckpointSequence, task.CheckpointSequence) {
		return false
	}
	switch task.Status {
	case "queued":
		return task.Attempt == 1 && task.LastSequence == 0 && task.LastCheckpoint == "" && task.ErrorCode == "" && task.EvidenceDigest == ""
	case "running":
		return task.LastSequence > 0 && task.LastCheckpoint != "" && task.ErrorCode == "" && task.EvidenceDigest == task.RecipeExecutionManifestDigest
	case "succeeded":
		return task.LastSequence > 0 && task.LastCheckpoint == task.CheckpointSequence[len(task.CheckpointSequence)-1] && task.ErrorCode == "" && task.EvidenceDigest == task.RecipeExecutionManifestDigest
	case "failed", "interrupted":
		return task.LastSequence > 0 && task.ErrorCode != "" && task.EvidenceDigest == ""
	default:
		return false
	}
}

func validRecipeTaskEvent(task RecipeTaskRecord, event RecipeTaskEvent) bool {
	if event.TaskID != task.TaskID || event.Attempt != task.Attempt || event.LeaseEpoch != task.LeaseEpoch || event.Sequence != task.LastSequence+1 || !canonicalWorkerEventInstant(event.OccurredAt) || !validSHA256(event.EventSHA256) || (event.Checkpoint != "" && !workerTaskCodePattern.MatchString(event.Checkpoint)) || (event.ErrorCode != "" && !workerTaskCodePattern.MatchString(event.ErrorCode)) {
		return false
	}
	next := recipeCheckpointIndexStore(task.CheckpointSequence, task.LastCheckpoint) + 1
	switch event.Status {
	case "running":
		return next >= 0 && next < len(task.CheckpointSequence)-1 && event.Checkpoint == task.CheckpointSequence[next] && event.ErrorCode == "" && event.EvidenceDigest == task.RecipeExecutionManifestDigest
	case "succeeded":
		return next == len(task.CheckpointSequence)-1 && event.Checkpoint == task.CheckpointSequence[next] && event.ErrorCode == "" && event.EvidenceDigest == task.RecipeExecutionManifestDigest
	case "failed", "interrupted":
		return event.Checkpoint == "" && event.ErrorCode != "" && event.EvidenceDigest == ""
	default:
		return false
	}
}

func recipeCheckpointAttribute(checkpoints []string) dynamodbtypes.AttributeValue {
	values := make([]dynamodbtypes.AttributeValue, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		values = append(values, &dynamodbtypes.AttributeValueMemberS{Value: checkpoint})
	}
	return &dynamodbtypes.AttributeValueMemberL{Value: values}
}

func validRecipeCheckpointSequence(checkpoints []string) bool {
	if len(checkpoints) == 0 || len(checkpoints) > 32 {
		return false
	}
	seen := make(map[string]struct{}, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if !workerTaskCodePattern.MatchString(checkpoint) {
			return false
		}
		if _, duplicate := seen[checkpoint]; duplicate {
			return false
		}
		seen[checkpoint] = struct{}{}
	}
	return true
}

func recipeCheckpointIndexStore(checkpoints []string, checkpoint string) int {
	if checkpoint == "" {
		return -1
	}
	for index, value := range checkpoints {
		if value == checkpoint {
			return index
		}
	}
	return -1
}

func sameRecipeTaskBinding(left, right RecipeTaskRecord) bool {
	return left.ConnectionID == right.ConnectionID && left.DeploymentID == right.DeploymentID && left.TaskID == right.TaskID && left.RequestSHA256 == right.RequestSHA256 && left.BootstrapSessionID == right.BootstrapSessionID && left.ExpectedInstanceID == right.ExpectedInstanceID && left.ExecutionID == right.ExecutionID && left.TaskKind == right.TaskKind && left.RecipeExecutionManifestDigest == right.RecipeExecutionManifestDigest && left.InputDigest == right.InputDigest && string(left.ManifestJSON) == string(right.ManifestJSON) && sameRecipeCheckpoints(left.CheckpointSequence, right.CheckpointSequence)
}

func sameRecipeCheckpoints(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func sameRecipeTaskLease(stored, previous RecipeTaskRecord, leaseEpoch int64) bool {
	return sameRecipeTaskBinding(stored, previous) && stored.Attempt == previous.Attempt && stored.LeaseEpoch == leaseEpoch
}
func recipeTaskBindsWorker(task RecipeTaskRecord, session WorkerSession) bool {
	return task.ConnectionID == session.ConnectionID && task.DeploymentID == session.DeploymentID && task.BootstrapSessionID == session.BootstrapSessionID && task.ExpectedInstanceID == session.ExpectedInstanceID
}
func cloneRecipeTask(task RecipeTaskRecord) RecipeTaskRecord {
	task.CheckpointSequence = append([]string(nil), task.CheckpointSequence...)
	task.ManifestJSON = append([]byte(nil), task.ManifestJSON...)
	return task
}

var recipeTaskIDPatternStore = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
