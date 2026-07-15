package store

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoArtifactStore struct {
	client                                       DynamoAPI
	receiptsTable, countersTable, artifactsTable string
}

func NewDynamoArtifactStore(client DynamoAPI, receipts, counters, artifacts string) (*DynamoArtifactStore, error) {
	if client == nil || !validTableName(receipts) || !validTableName(counters) || !validTableName(artifacts) || !uniqueStrings(receipts, counters, artifacts) {
		return nil, NewError("artifact_store_invalid")
	}
	return &DynamoArtifactStore{client: client, receiptsTable: receipts, countersTable: counters, artifactsTable: artifacts}, nil
}

func (s *DynamoArtifactStore) LookupArtifact(ctx context.Context, connectionID, deploymentID, taskID string) (ArtifactRecord, bool, error) {
	if s == nil || !contract.ValidConnectionID(connectionID) || !contract.ValidID(deploymentID) || !contract.ValidRecipeTaskID(taskID) {
		return ArtifactRecord{}, false, NewError("artifact_store_invalid")
	}
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.artifactsTable, ConsistentRead: boolPtr(true), Key: artifactKey(deploymentID, taskID)})
	if err != nil {
		return ArtifactRecord{}, false, NewError("artifact_store_unavailable")
	}
	if len(out.Item) == 0 {
		return ArtifactRecord{}, false, nil
	}
	record, err := artifactFromItem(out.Item)
	if err != nil || record.ConnectionID != connectionID {
		return ArtifactRecord{}, false, NewError("artifact_store_invalid")
	}
	return record, true, nil
}

func (s *DynamoArtifactStore) PrepareArtifact(ctx context.Context, receipt Record, artifact ArtifactRecord) (Record, ArtifactRecord, bool, error) {
	if validateArtifactMutation(receipt, artifact, "uploading") != nil {
		return Record{}, ArtifactRecord{}, false, NewError("artifact_store_invalid")
	}
	items := baseArtifactTransaction(s, receipt)
	items = append(items, dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{TableName: &s.artifactsTable, ConditionExpression: stringPtr("attribute_not_exists(deployment_id) AND attribute_not_exists(task_id)"), Item: artifactItem(artifact)}})
	if _, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items}); err == nil {
		return cloneRecord(receipt), artifact, true, nil
	} else {
		return s.reconcile(ctx, receipt, artifact, err)
	}
}

func (s *DynamoArtifactStore) CompleteArtifact(ctx context.Context, receipt Record, artifact ArtifactRecord) (Record, ArtifactRecord, bool, error) {
	if validateArtifactMutation(receipt, artifact, "verified") != nil || artifact.VersionID == "" || artifact.VerifiedAt == "" {
		return Record{}, ArtifactRecord{}, false, NewError("artifact_store_invalid")
	}
	items := baseArtifactTransaction(s, receipt)
	values := artifactItem(artifact)
	delete(values, "deployment_id")
	delete(values, "task_id")
	items = append(items, dynamodbtypes.TransactWriteItem{Update: &dynamodbtypes.Update{TableName: &s.artifactsTable, Key: artifactKey(artifact.Binding.DeploymentID, artifact.Binding.TaskID), UpdateExpression: stringPtr("SET #state=:verified, version_id=:version_id, verified_at=:verified_at REMOVE expires_at"), ConditionExpression: stringPtr("#state=:uploading AND connection_id=:connection_id AND execution_id=:execution_id AND recipe_digest=:recipe_digest AND artifact_digest=:artifact_digest AND manifest_digest=:manifest_digest AND archive_sha256=:archive_sha256 AND size_bytes=:size_bytes AND media_type=:media_type"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":verified": values["state"], ":uploading": &dynamodbtypes.AttributeValueMemberS{Value: "uploading"}, ":version_id": values["version_id"], ":verified_at": values["verified_at"], ":connection_id": values["connection_id"], ":execution_id": values["execution_id"], ":recipe_digest": values["recipe_digest"], ":artifact_digest": values["artifact_digest"], ":manifest_digest": values["manifest_digest"], ":archive_sha256": values["archive_sha256"], ":size_bytes": values["size_bytes"], ":media_type": values["media_type"]}}})
	if _, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items}); err == nil {
		return cloneRecord(receipt), artifact, true, nil
	} else {
		return s.reconcile(ctx, receipt, artifact, err)
	}
}

func baseArtifactTransaction(s *DynamoArtifactStore, r Record) []dynamodbtypes.TransactWriteItem {
	return []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter=:counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)"), Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: r.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, "expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}, "action": &dynamodbtypes.AttributeValueMemberS{Value: r.Action}, "result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(r.ResultJSON)}}}}}
}
func (s *DynamoArtifactStore) reconcile(ctx context.Context, r Record, a ArtifactRecord, cause error) (Record, ArtifactRecord, bool, error) {
	var canceled *dynamodbtypes.TransactionCanceledException
	if !errors.As(cause, &canceled) {
		return Record{}, ArtifactRecord{}, false, NewError("artifact_store_unavailable")
	}
	receipt, receiptFound, receiptErr := s.lookupArtifactReceipt(ctx, r.ConnectionID, r.CommandID)
	existing, found, err := s.LookupArtifact(ctx, a.ConnectionID, a.Binding.DeploymentID, a.Binding.TaskID)
	if receiptErr == nil && receiptFound && receipt.SameIdentity(r) && err == nil && found && existing.SameBinding(a) && existing.State == a.State && existing.VersionID == a.VersionID {
		return receipt, existing, false, nil
	}
	return Record{}, ArtifactRecord{}, false, NewError("artifact_put_conflict")
}

func (s *DynamoArtifactStore) lookupArtifactReceipt(ctx context.Context, connectionID, commandID string) (Record, bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.receiptsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: commandID}}})
	if err != nil {
		return Record{}, false, NewError("artifact_store_unavailable")
	}
	if len(out.Item) == 0 {
		return Record{}, false, nil
	}
	value, err := recordFromItem(out.Item)
	return value, err == nil, err
}
func validateArtifactMutation(r Record, a ArtifactRecord, state string) error {
	uploadingValid := state == "uploading" && a.VersionID == "" && a.VerifiedAt == "" && canonicalArtifactTime(a.ExpiresAt)
	verifiedValid := state == "verified" && a.VersionID != "" && len(a.VersionID) <= 1024 && a.ExpiresAt == "" && canonicalArtifactTime(a.VerifiedAt)
	if validateRecord(r) != nil || r.Action != contract.ActionArtifactPut || a.ConnectionID != r.ConnectionID || a.Binding.Validate() != nil || a.State != state || a.ObjectKey != a.Binding.ObjectKey() || (!uploadingValid && !verifiedValid) {
		return NewError("artifact_store_invalid")
	}
	return nil
}
func artifactKey(deploymentID, taskID string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: taskID}}
}
func artifactItem(a ArtifactRecord) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.DeploymentID}, "task_id": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.TaskID}, "connection_id": &dynamodbtypes.AttributeValueMemberS{Value: a.ConnectionID}, "execution_id": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.ExecutionID}, "recipe_digest": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.RecipeDigest}, "artifact_digest": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.ArtifactDigest}, "manifest_digest": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.ManifestDigest}, "archive_sha256": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.ArchiveSHA256}, "size_bytes": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(a.Binding.SizeBytes, 10)}, "media_type": &dynamodbtypes.AttributeValueMemberS{Value: a.Binding.MediaType}, "object_key": &dynamodbtypes.AttributeValueMemberS{Value: a.ObjectKey}, "version_id": &dynamodbtypes.AttributeValueMemberS{Value: a.VersionID}, "state": &dynamodbtypes.AttributeValueMemberS{Value: a.State}, "expires_at": &dynamodbtypes.AttributeValueMemberS{Value: a.ExpiresAt}, "verified_at": &dynamodbtypes.AttributeValueMemberS{Value: a.VerifiedAt}}
}
func artifactFromItem(i map[string]dynamodbtypes.AttributeValue) (ArtifactRecord, error) {
	get := func(k string) string { v, _ := stringAttribute(i, k); return v }
	size, err := numberAttribute(i, "size_bytes", false)
	a := ArtifactRecord{ConnectionID: get("connection_id"), Binding: contract.ArtifactBinding{DeploymentID: get("deployment_id"), TaskID: get("task_id"), ExecutionID: get("execution_id"), RecipeDigest: get("recipe_digest"), ArtifactDigest: get("artifact_digest"), ManifestDigest: get("manifest_digest"), ArchiveSHA256: get("archive_sha256"), SizeBytes: size, MediaType: get("media_type")}, ObjectKey: get("object_key"), VersionID: get("version_id"), State: get("state"), ExpiresAt: get("expires_at"), VerifiedAt: get("verified_at")}
	uploadingValid := a.State == "uploading" && a.ObjectKey != "" && a.VersionID == "" && a.VerifiedAt == "" && canonicalArtifactTime(a.ExpiresAt)
	verifiedValid := a.State == "verified" && a.ObjectKey != "" && a.VersionID != "" && a.ExpiresAt == "" && canonicalArtifactTime(a.VerifiedAt)
	if err != nil || a.Binding.Validate() != nil || (!uploadingValid && !verifiedValid) {
		return ArtifactRecord{}, NewError("artifact_store_invalid")
	}
	return a, nil
}

func canonicalArtifactTime(value string) bool {
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z", value)
	return err == nil && parsed.UTC().Format("2006-01-02T15:04:05.000Z") == value
}
