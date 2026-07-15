package store

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

type ServiceReadinessRecord struct {
	ConnectionID, DeploymentID, ServiceID, TaskID, RequestSHA256                    string
	BootstrapSessionID, ExpectedInstanceID, ExecutionID, ProbeKind                  string
	RecipeExecutionManifestDigest, InstallEvidenceDigest, SemanticExpectationDigest string
	Status, Checkpoint, ChallengeDigest, ChallengeExpiresAt                         string
	SemanticEvidenceDigest, StackObservationDigest, ErrorCode, LastEventSHA256      string
	Attempt, LeaseEpoch, LastSequence                                               int64
	CreatedAt, UpdatedAt                                                            string
}

type ServiceReadinessEvent struct {
	TaskID, Status, ChallengeDigest, SemanticEvidenceDigest, StackObservationDigest string
	ErrorCode, OccurredAt, EventSHA256                                              string
	Attempt, LeaseEpoch, Sequence                                                   int64
}

type ServiceReadinessChallengeGrant struct {
	Digest, ExpiresAt string
}

type ServiceReadinessRepository interface {
	LookupServiceReadiness(context.Context, string, string) (ServiceReadinessRecord, bool, error)
	IssueServiceReadiness(context.Context, Record, ServiceReadinessRecord) (Record, ServiceReadinessRecord, bool, error)
	ClaimServiceReadiness(context.Context, WorkerLeaseAuthorization, ServiceReadinessChallengeGrant) (ServiceReadinessRecord, bool, error)
	RecordServiceReadinessEvent(context.Context, WorkerLeaseAuthorization, ServiceReadinessEvent) (ServiceReadinessRecord, bool, error)
}

type DynamoServiceReadinessStore struct {
	client                                                            WorkerTaskDynamoAPI
	receiptsTable, countersTable, workerSessionsTable, readinessTable string
}

func NewDynamoServiceReadinessStore(client WorkerTaskDynamoAPI, receipts, counters, sessions, readiness string) (*DynamoServiceReadinessStore, error) {
	if client == nil || !validTableName(receipts) || !validTableName(counters) || !validTableName(sessions) || !validTableName(readiness) || !uniqueStrings(receipts, counters, sessions, readiness) {
		return nil, NewError("service_readiness_store_invalid")
	}
	return &DynamoServiceReadinessStore{client: client, receiptsTable: receipts, countersTable: counters, workerSessionsTable: sessions, readinessTable: readiness}, nil
}

func readinessKey(deploymentID, taskID string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: taskID}}
}

func (s *DynamoServiceReadinessStore) LookupServiceReadiness(ctx context.Context, deploymentID, taskID string) (ServiceReadinessRecord, bool, error) {
	if s == nil || s.client == nil || !contract.ValidID(deploymentID) || !contract.ValidRecipeTaskID(taskID) {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_invalid")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.readinessTable, ConsistentRead: boolPtr(true), Key: readinessKey(deploymentID, taskID)})
	if err != nil {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_unavailable")
	}
	if len(output.Item) == 0 {
		return ServiceReadinessRecord{}, false, nil
	}
	record, err := readinessFromItem(output.Item)
	if err != nil || record.DeploymentID != deploymentID || record.TaskID != taskID {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_store_invalid")
	}
	return record, true, nil
}

func (s *DynamoServiceReadinessStore) IssueServiceReadiness(ctx context.Context, receipt Record, task ServiceReadinessRecord) (Record, ServiceReadinessRecord, bool, error) {
	if s == nil || validateRecord(receipt) != nil || receipt.Action != contract.ActionServiceReadinessIssue || !validNewReadiness(task) || task.ConnectionID != receipt.ConnectionID || task.RequestSHA256 != receipt.RequestSHA256 {
		return Record{}, ServiceReadinessRecord{}, false, NewError("service_readiness_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: receipt.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(receipt.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, Item: recordItemForStore(receipt), ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)")}},
		{Put: &dynamodbtypes.Put{TableName: &s.readinessTable, Item: readinessItem(task), ConditionExpression: stringPtr("attribute_not_exists(deployment_id) AND attribute_not_exists(task_id)")}},
	}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if writeErr == nil {
		return cloneRecord(receipt), task, true, nil
	}
	storedReceipt, receiptFound, receiptErr := s.lookupReceipt(ctx, receipt.ConnectionID, receipt.CommandID)
	storedTask, taskFound, taskErr := s.LookupServiceReadiness(ctx, task.DeploymentID, task.TaskID)
	if receiptErr != nil {
		return Record{}, ServiceReadinessRecord{}, false, receiptErr
	}
	if taskErr != nil {
		return Record{}, ServiceReadinessRecord{}, false, taskErr
	}
	if receiptFound && taskFound && storedReceipt.SameIdentity(receipt) && sameReadinessBinding(storedTask, task) {
		return storedReceipt, storedTask, false, nil
	}
	if receiptFound && !storedReceipt.SameIdentity(receipt) {
		return Record{}, ServiceReadinessRecord{}, false, NewError("command_id_conflict")
	}
	if taskFound && !sameReadinessBinding(storedTask, task) {
		return Record{}, ServiceReadinessRecord{}, false, NewError("service_readiness_conflict")
	}
	return Record{}, ServiceReadinessRecord{}, false, NewError("service_readiness_unavailable")
}

func (s *DynamoServiceReadinessStore) lookupReceipt(ctx context.Context, connectionID, commandID string) (Record, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.receiptsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: commandID}}})
	if err != nil {
		return Record{}, false, NewError("service_readiness_unavailable")
	}
	if len(output.Item) == 0 {
		return Record{}, false, nil
	}
	record, err := recordFromItem(output.Item)
	return record, err == nil, err
}

func (s *DynamoServiceReadinessStore) ClaimServiceReadiness(ctx context.Context, auth WorkerLeaseAuthorization, grant ServiceReadinessChallengeGrant) (ServiceReadinessRecord, bool, error) {
	if !validWorkerAuthorization(auth) || !workerNamedDigestPattern.MatchString(grant.Digest) || !canonicalWorkerEventInstant(grant.ExpiresAt) || grant.ExpiresAt <= auth.Now {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_unauthorized")
	}
	session, err := s.authorize(ctx, auth)
	if err != nil {
		return ServiceReadinessRecord{}, false, err
	}
	var candidate ServiceReadinessRecord
	var cursor map[string]dynamodbtypes.AttributeValue
	for pages := 0; pages < 100 && candidate.TaskID == ""; pages++ {
		output, queryErr := s.client.Query(ctx, &dynamodb.QueryInput{TableName: &s.readinessTable, ConsistentRead: boolPtr(true), KeyConditionExpression: stringPtr("deployment_id = :deployment_id"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: auth.DeploymentID}}, ExclusiveStartKey: cursor})
		if queryErr != nil {
			return ServiceReadinessRecord{}, false, NewError("service_readiness_unavailable")
		}
		for _, item := range output.Items {
			parsed, parseErr := readinessFromItem(item)
			if parseErr != nil {
				return ServiceReadinessRecord{}, false, NewError("service_readiness_store_invalid")
			}
			if parsed.Status == "queued" || (parsed.Status == "running" && parsed.LastSequence == 0) {
				candidate = parsed
				break
			}
		}
		cursor = output.LastEvaluatedKey
		if len(cursor) == 0 {
			break
		}
	}
	if candidate.TaskID == "" && len(cursor) != 0 {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_unavailable")
	}
	if candidate.TaskID == "" {
		return ServiceReadinessRecord{}, false, nil
	}
	if candidate.ConnectionID != session.ConnectionID || candidate.BootstrapSessionID != session.BootstrapSessionID || candidate.ExpectedInstanceID != session.ExpectedInstanceID {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_unauthorized")
	}
	values := map[string]dynamodbtypes.AttributeValue{
		":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: auth.ConnectionID}, ":session": &dynamodbtypes.AttributeValueMemberS{Value: auth.BootstrapSessionID}, ":instance": &dynamodbtypes.AttributeValueMemberS{Value: auth.ExpectedInstanceID}, ":current_status": &dynamodbtypes.AttributeValueMemberS{Value: candidate.Status}, ":running": &dynamodbtypes.AttributeValueMemberS{Value: "running"}, ":checkpoint": &dynamodbtypes.AttributeValueMemberS{Value: "challenge_issued"}, ":zero": &dynamodbtypes.AttributeValueMemberN{Value: "0"}, ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(candidate.Attempt, 10)}, ":next_attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(candidate.Attempt, 10)}, ":previous_lease": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(candidate.LeaseEpoch, 10)}, ":lease": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(auth.LeaseEpoch, 10)}, ":digest": &dynamodbtypes.AttributeValueMemberS{Value: grant.Digest}, ":expires": &dynamodbtypes.AttributeValueMemberS{Value: grant.ExpiresAt}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: auth.Now},
	}
	if candidate.LeaseEpoch > 0 && candidate.LeaseEpoch != auth.LeaseEpoch {
		values[":next_attempt"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(candidate.Attempt+1, 10)}
	}
	challengeCondition := "attribute_not_exists(challenge_digest)"
	if candidate.ChallengeDigest != "" {
		challengeCondition = "challenge_digest = :previous_digest"
		values[":previous_digest"] = &dynamodbtypes.AttributeValueMemberS{Value: candidate.ChallengeDigest}
	}
	items := []dynamodbtypes.TransactWriteItem{{ConditionCheck: workerSessionCondition(s.workerSessionsTable, auth)}, {Update: &dynamodbtypes.Update{TableName: &s.readinessTable, Key: readinessKey(candidate.DeploymentID, candidate.TaskID), ConditionExpression: stringPtr("connection_id = :connection_id AND bootstrap_session_id = :session AND expected_instance_id = :instance AND #status = :current_status AND attempt = :attempt AND lease_epoch = :previous_lease AND last_sequence = :zero AND " + challengeCondition), UpdateExpression: stringPtr("SET #status = :running, checkpoint = :checkpoint, attempt = :next_attempt, lease_epoch = :lease, challenge_digest = :digest, challenge_expires_at = :expires, updated_at = :now"), ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}}}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, found, readErr := s.LookupServiceReadiness(ctx, candidate.DeploymentID, candidate.TaskID)
	if readErr != nil {
		return ServiceReadinessRecord{}, false, readErr
	}
	expectedAttempt := candidate.Attempt
	if candidate.LeaseEpoch > 0 && candidate.LeaseEpoch != auth.LeaseEpoch {
		expectedAttempt++
	}
	if found && sameReadinessBinding(stored, candidate) && stored.LeaseEpoch == auth.LeaseEpoch && stored.Attempt == expectedAttempt && stored.ChallengeDigest == grant.Digest && stored.ChallengeExpiresAt == grant.ExpiresAt {
		return stored, true, nil
	}
	if writeErr != nil {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_claim_race")
	}
	return ServiceReadinessRecord{}, false, NewError("service_readiness_store_invalid")
}

func (s *DynamoServiceReadinessStore) RecordServiceReadinessEvent(ctx context.Context, auth WorkerLeaseAuthorization, event ServiceReadinessEvent) (ServiceReadinessRecord, bool, error) {
	if _, err := s.authorize(ctx, auth); err != nil {
		return ServiceReadinessRecord{}, false, err
	}
	task, found, err := s.LookupServiceReadiness(ctx, auth.DeploymentID, event.TaskID)
	if err != nil || !found {
		if err != nil {
			return ServiceReadinessRecord{}, false, err
		}
		return ServiceReadinessRecord{}, false, NewError("service_readiness_not_found")
	}
	if task.ConnectionID != auth.ConnectionID || task.BootstrapSessionID != auth.BootstrapSessionID || task.ExpectedInstanceID != auth.ExpectedInstanceID || task.LeaseEpoch != auth.LeaseEpoch {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_event_invalid")
	}
	if task.Attempt == event.Attempt && task.LastSequence == event.Sequence && task.LastEventSHA256 == event.EventSHA256 {
		return task, true, nil
	}
	if !validReadinessEvent(task, auth, event) {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_event_invalid")
	}
	values := map[string]dynamodbtypes.AttributeValue{
		":connection": &dynamodbtypes.AttributeValueMemberS{Value: auth.ConnectionID}, ":session": &dynamodbtypes.AttributeValueMemberS{Value: auth.BootstrapSessionID}, ":instance": &dynamodbtypes.AttributeValueMemberS{Value: auth.ExpectedInstanceID}, ":running": &dynamodbtypes.AttributeValueMemberS{Value: "running"}, ":attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.Attempt, 10)}, ":lease": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(event.LeaseEpoch, 10)}, ":zero": &dynamodbtypes.AttributeValueMemberN{Value: "0"}, ":one": &dynamodbtypes.AttributeValueMemberN{Value: "1"}, ":status": &dynamodbtypes.AttributeValueMemberS{Value: event.Status}, ":event": &dynamodbtypes.AttributeValueMemberS{Value: event.EventSHA256}, ":now": &dynamodbtypes.AttributeValueMemberS{Value: auth.Now},
	}
	update := "SET #status = :status, last_sequence = :one, last_event_sha256 = :event, updated_at = :now REMOVE challenge_expires_at"
	if event.Status == "succeeded" {
		values[":checkpoint"] = &dynamodbtypes.AttributeValueMemberS{Value: "readiness_verified"}
		values[":semantic"] = &dynamodbtypes.AttributeValueMemberS{Value: event.SemanticEvidenceDigest}
		values[":observation"] = &dynamodbtypes.AttributeValueMemberS{Value: event.StackObservationDigest}
		update = "SET #status = :status, checkpoint = :checkpoint, last_sequence = :one, semantic_evidence_digest = :semantic, stack_observation_digest = :observation, last_event_sha256 = :event, updated_at = :now REMOVE challenge_expires_at"
	} else {
		values[":error"] = &dynamodbtypes.AttributeValueMemberS{Value: event.ErrorCode}
		update = "SET #status = :status, error_code = :error, last_sequence = :one, last_event_sha256 = :event, updated_at = :now REMOVE checkpoint, challenge_digest, challenge_expires_at"
	}
	items := []dynamodbtypes.TransactWriteItem{{ConditionCheck: workerSessionCondition(s.workerSessionsTable, auth)}, {Update: &dynamodbtypes.Update{TableName: &s.readinessTable, Key: readinessKey(task.DeploymentID, task.TaskID), ConditionExpression: stringPtr("connection_id = :connection AND bootstrap_session_id = :session AND expected_instance_id = :instance AND #status = :running AND attempt = :attempt AND lease_epoch = :lease AND last_sequence = :zero"), UpdateExpression: stringPtr(update), ExpressionAttributeNames: map[string]string{"#status": "status"}, ExpressionAttributeValues: values}}}
	_, writeErr := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	stored, storedFound, readErr := s.LookupServiceReadiness(ctx, task.DeploymentID, task.TaskID)
	if readErr != nil {
		return ServiceReadinessRecord{}, false, readErr
	}
	if storedFound && sameReadinessBinding(stored, task) && stored.LastSequence == event.Sequence && stored.LastEventSHA256 == event.EventSHA256 {
		return stored, writeErr != nil, nil
	}
	if writeErr != nil {
		return ServiceReadinessRecord{}, false, NewError("service_readiness_event_conflict")
	}
	return ServiceReadinessRecord{}, false, NewError("service_readiness_store_invalid")
}

func (s *DynamoServiceReadinessStore) authorize(ctx context.Context, auth WorkerLeaseAuthorization) (WorkerSession, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.workerSessionsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: auth.BootstrapSessionID}}})
	if err != nil {
		return WorkerSession{}, NewError("service_readiness_unavailable")
	}
	session, parseErr := workerSessionFromItem(output.Item)
	if parseErr != nil || session.State != "active" || session.ConnectionID != auth.ConnectionID || session.DeploymentID != auth.DeploymentID || session.ExpectedInstanceID != auth.ExpectedInstanceID || session.LeaseEpoch != auth.LeaseEpoch || session.TokenSHA256 != auth.TokenSHA256 || session.LeaseExpiresAt <= auth.Now {
		return WorkerSession{}, NewError("service_readiness_unauthorized")
	}
	return session, nil
}

func readinessItem(r ServiceReadinessRecord) map[string]dynamodbtypes.AttributeValue {
	item := map[string]dynamodbtypes.AttributeValue{
		"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: r.TaskID}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "service_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ServiceID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, "bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BootstrapSessionID}, "expected_instance_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ExpectedInstanceID}, "execution_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ExecutionID}, "probe_kind": &dynamodbtypes.AttributeValueMemberS{Value: r.ProbeKind}, "recipe_execution_manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: r.RecipeExecutionManifestDigest}, "install_evidence_digest": &dynamodbtypes.AttributeValueMemberS{Value: r.InstallEvidenceDigest}, "semantic_expectation_digest": &dynamodbtypes.AttributeValueMemberS{Value: r.SemanticExpectationDigest}, "status": &dynamodbtypes.AttributeValueMemberS{Value: r.Status}, "attempt": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.Attempt, 10)}, "lease_epoch": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.LeaseEpoch, 10)}, "last_sequence": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.LastSequence, 10)}, "created_at": &dynamodbtypes.AttributeValueMemberS{Value: r.CreatedAt}, "updated_at": &dynamodbtypes.AttributeValueMemberS{Value: r.UpdatedAt},
	}
	return item
}

func readinessFromItem(item map[string]dynamodbtypes.AttributeValue) (ServiceReadinessRecord, error) {
	var r ServiceReadinessRecord
	var err error
	required := []*string{&r.ConnectionID, &r.DeploymentID, &r.ServiceID, &r.TaskID, &r.RequestSHA256, &r.BootstrapSessionID, &r.ExpectedInstanceID, &r.ExecutionID, &r.ProbeKind, &r.RecipeExecutionManifestDigest, &r.InstallEvidenceDigest, &r.SemanticExpectationDigest, &r.Status, &r.CreatedAt, &r.UpdatedAt}
	names := []string{"connection_id", "deployment_id", "service_id", "task_id", "request_sha256", "bootstrap_session_id", "expected_instance_id", "execution_id", "probe_kind", "recipe_execution_manifest_digest", "install_evidence_digest", "semantic_expectation_digest", "status", "created_at", "updated_at"}
	for i := range required {
		*required[i], err = stringAttribute(item, names[i])
		if err != nil {
			return r, NewError("service_readiness_store_invalid")
		}
	}
	r.Attempt, err = numberAttribute(item, "attempt", false)
	if err != nil {
		return r, NewError("service_readiness_store_invalid")
	}
	r.LeaseEpoch, err = numberAttribute(item, "lease_epoch", true)
	if err != nil {
		return r, NewError("service_readiness_store_invalid")
	}
	r.LastSequence, err = numberAttribute(item, "last_sequence", true)
	if err != nil {
		return r, NewError("service_readiness_store_invalid")
	}
	for name, target := range map[string]*string{"checkpoint": &r.Checkpoint, "challenge_digest": &r.ChallengeDigest, "challenge_expires_at": &r.ChallengeExpiresAt, "semantic_evidence_digest": &r.SemanticEvidenceDigest, "stack_observation_digest": &r.StackObservationDigest, "error_code": &r.ErrorCode, "last_event_sha256": &r.LastEventSHA256} {
		if value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS); ok {
			*target = value.Value
		}
	}
	if !validReadiness(r) {
		return r, NewError("service_readiness_store_invalid")
	}
	return r, nil
}

func validNewReadiness(r ServiceReadinessRecord) bool {
	return validReadiness(r) && r.Status == "queued" && r.Attempt == 1 && r.LeaseEpoch == 0 && r.LastSequence == 0 && r.ChallengeDigest == ""
}
func validReadiness(r ServiceReadinessRecord) bool {
	if !contract.ValidConnectionID(r.ConnectionID) || !contract.ValidID(r.DeploymentID) || !contract.ValidID(r.ServiceID) || !contract.ValidRecipeTaskID(r.TaskID) || !validSHA256(r.RequestSHA256) || !contract.ValidID(r.BootstrapSessionID) || !workerInstancePattern.MatchString(r.ExpectedInstanceID) || !contract.ValidID(r.ExecutionID) || r.ProbeKind != contract.ServiceReadinessProbeKind || !workerNamedDigestPattern.MatchString(r.RecipeExecutionManifestDigest) || !workerNamedDigestPattern.MatchString(r.InstallEvidenceDigest) || !workerNamedDigestPattern.MatchString(r.SemanticExpectationDigest) || r.Attempt < 1 || r.LeaseEpoch < 0 || r.LastSequence < 0 || !canonicalWorkerEventInstant(r.CreatedAt) || !canonicalWorkerEventInstant(r.UpdatedAt) {
		return false
	}
	if r.Status == "queued" && ((r.ChallengeDigest == "") != (r.ChallengeExpiresAt == "")) {
		return false
	}
	if r.Status == "queued" && r.ChallengeDigest != "" && (!workerNamedDigestPattern.MatchString(r.ChallengeDigest) || !canonicalWorkerEventInstant(r.ChallengeExpiresAt) || r.LeaseEpoch < 1) {
		return false
	}
	switch r.Status {
	case "queued":
		return r.LastSequence == 0 && r.Checkpoint == "" && r.SemanticEvidenceDigest == "" && r.StackObservationDigest == "" && r.ErrorCode == "" && r.LastEventSHA256 == ""
	case "running":
		return r.LastSequence == 0 && r.Checkpoint == "challenge_issued" && workerNamedDigestPattern.MatchString(r.ChallengeDigest) && canonicalWorkerEventInstant(r.ChallengeExpiresAt) && r.LeaseEpoch > 0 && r.SemanticEvidenceDigest == "" && r.StackObservationDigest == "" && r.ErrorCode == "" && r.LastEventSHA256 == ""
	case "succeeded":
		return r.LastSequence == 1 && r.Checkpoint == "readiness_verified" && workerNamedDigestPattern.MatchString(r.ChallengeDigest) && r.ChallengeExpiresAt == "" && r.SemanticEvidenceDigest == r.SemanticExpectationDigest && workerNamedDigestPattern.MatchString(r.StackObservationDigest) && r.ErrorCode == "" && validSHA256(r.LastEventSHA256)
	case "failed", "interrupted":
		return r.LastSequence == 1 && r.Checkpoint == "" && r.ChallengeDigest == "" && r.ChallengeExpiresAt == "" && workerTaskCodePattern.MatchString(r.ErrorCode) && r.SemanticEvidenceDigest == "" && r.StackObservationDigest == "" && validSHA256(r.LastEventSHA256)
	}
	return false
}
func sameReadinessBinding(a, b ServiceReadinessRecord) bool {
	return a.ConnectionID == b.ConnectionID && a.DeploymentID == b.DeploymentID && a.ServiceID == b.ServiceID && a.TaskID == b.TaskID && a.RequestSHA256 == b.RequestSHA256 && a.BootstrapSessionID == b.BootstrapSessionID && a.ExpectedInstanceID == b.ExpectedInstanceID && a.ExecutionID == b.ExecutionID && a.ProbeKind == b.ProbeKind && a.RecipeExecutionManifestDigest == b.RecipeExecutionManifestDigest && a.InstallEvidenceDigest == b.InstallEvidenceDigest && a.SemanticExpectationDigest == b.SemanticExpectationDigest
}
func validReadinessEvent(task ServiceReadinessRecord, auth WorkerLeaseAuthorization, event ServiceReadinessEvent) bool {
	if task.Status != "running" || task.Checkpoint != "challenge_issued" || event.TaskID != task.TaskID || event.Attempt != task.Attempt || event.LeaseEpoch != task.LeaseEpoch || event.Sequence != 1 || task.LastSequence != 0 || task.ChallengeDigest == "" || task.ChallengeExpiresAt <= auth.Now || !validSHA256(event.EventSHA256) {
		return false
	}
	if event.Status == "succeeded" {
		return event.ChallengeDigest == task.ChallengeDigest && event.SemanticEvidenceDigest == task.SemanticExpectationDigest && workerNamedDigestPattern.MatchString(event.StackObservationDigest) && event.ErrorCode == ""
	}
	return (event.Status == "failed" || event.Status == "interrupted") && event.ChallengeDigest == "" && event.SemanticEvidenceDigest == "" && event.StackObservationDigest == "" && workerTaskCodePattern.MatchString(event.ErrorCode)
}
