package store

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func (s *DynamoRepository) LookupServiceBackup(ctx context.Context, connectionID, backupID string) (ServiceBackupReservation, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.serviceBackupsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "backup_id": &dynamodbtypes.AttributeValueMemberS{Value: backupID}}})
	if err != nil {
		return ServiceBackupReservation{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return ServiceBackupReservation{}, false, nil
	}
	reservation, err := serviceBackupFromItem(output.Item)
	if err != nil || reservation.ConnectionID != connectionID || reservation.BackupID != backupID || validateServiceBackupReservation(reservation) != nil {
		return ServiceBackupReservation{}, false, NewError("service_backup_store_invalid")
	}
	return reservation, true, nil
}

func (s *DynamoRepository) ReserveServiceBackup(ctx context.Context, reservation ServiceBackupReservation) (ServiceBackupReservation, bool, error) {
	if validateServiceBackupReservation(reservation) != nil || reservation.State != "reserved" || len(reservation.ResultJSON) != 0 {
		return ServiceBackupReservation{}, false, NewError("service_backup_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(reservation.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.serviceBackupsTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(backup_id)"), Item: serviceBackupItem(reservation)}},
		{Put: serviceBackupApprovalUsePut(s.approvalUsesTable, reservation, "approval#"+reservation.ApprovalID)},
		{Put: serviceBackupApprovalUsePut(s.approvalUsesTable, reservation, "challenge#"+reservation.ChallengeID)},
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneServiceBackup(reservation), true, nil
	}
	if existing, found, lookupErr := s.LookupServiceBackup(ctx, reservation.ConnectionID, reservation.BackupID); lookupErr != nil {
		return ServiceBackupReservation{}, false, lookupErr
	} else if found {
		if !existing.SameIdentity(reservation) {
			return ServiceBackupReservation{}, false, NewError("service_backup_conflict")
		}
		return existing, false, nil
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "approval#"+reservation.ApprovalID); lookupErr != nil {
		return ServiceBackupReservation{}, false, lookupErr
	} else if used {
		return ServiceBackupReservation{}, false, NewError("approval_already_consumed")
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "challenge#"+reservation.ChallengeID); lookupErr != nil {
		return ServiceBackupReservation{}, false, lookupErr
	} else if used {
		return ServiceBackupReservation{}, false, NewError("challenge_already_consumed")
	}
	if counter, found, lookupErr := s.lookupCounter(ctx, reservation.ConnectionID); lookupErr != nil {
		return ServiceBackupReservation{}, false, lookupErr
	} else if found && reservation.NodeCounter <= counter {
		return ServiceBackupReservation{}, false, NewError("stale_node_counter")
	}
	return ServiceBackupReservation{}, false, NewError("service_backup_reservation_race")
}

func (s *DynamoRepository) FinalizeServiceBackup(ctx context.Context, reservation ServiceBackupReservation, receipt Record) (Record, bool, error) {
	if validateServiceBackupReservation(reservation) != nil || reservation.State != "reserved" || receipt.Action != contract.ActionServiceBackup || !reservation.SameIdentity(ServiceBackupReservation{ConnectionID: receipt.ConnectionID, BackupID: reservation.BackupID, ServiceID: reservation.ServiceID, DeploymentID: reservation.DeploymentID, CommandID: receipt.CommandID, RequestSHA256: receipt.RequestSHA256, ExpectedGeneration: receipt.ExpectedGeneration, NodeCounter: receipt.NodeCounter, ApprovalID: reservation.ApprovalID, ChallengeID: reservation.ChallengeID, SignerKeyID: reservation.SignerKeyID, RequestJSON: reservation.RequestJSON}) || validateRecord(receipt) != nil {
		return Record{}, false, NewError("service_backup_store_invalid")
	}
	var result contract.ServiceBackupResult
	var request contract.ServiceBackupRequest
	if json.Unmarshal(receipt.ResultJSON, &result) != nil || json.Unmarshal(reservation.RequestJSON, &request) != nil || result.Schema != contract.ServiceBackupResultSchema || result.Status != "backup_available" || result.Receipt.Schema != contract.ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != receipt.ConnectionID || result.Receipt.CommandID != receipt.CommandID || result.Receipt.RequestSHA256 != receipt.RequestSHA256 || result.Receipt.ExpectedGeneration != receipt.ExpectedGeneration || result.Receipt.NodeCounter != receipt.NodeCounter || result.Receipt.Action != contract.ActionServiceBackup || result.Backup.BackupID != request.BackupID || result.Backup.ServiceID != request.ServiceID || result.Backup.DeploymentID != request.DeploymentID || result.Backup.InstanceID != request.InstanceID || result.Backup.RetentionPolicy != request.RetentionPolicy || result.Backup.ImageID == "" || len(result.Backup.Snapshots) != len(request.VolumeIDs) {
		return Record{}, false, NewError("service_backup_store_invalid")
	}
	for index, snapshot := range result.Backup.Snapshots {
		if snapshot.VolumeID != request.VolumeIDs[index] || snapshot.SnapshotID == "" || snapshot.State != "completed" || !snapshot.Encrypted {
			return Record{}, false, NewError("service_backup_store_invalid")
		}
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.serviceBackupsTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "backup_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.BackupID}}, UpdateExpression: stringPtr("SET #state=:finalized,result_json=:result_json"), ConditionExpression: stringPtr("request_sha256=:request_sha256 AND #state=:reserved"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":finalized": &dynamodbtypes.AttributeValueMemberS{Value: "finalized"}, ":reserved": &dynamodbtypes.AttributeValueMemberS{Value: "reserved"}, ":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RequestSHA256}, ":result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(receipt.ResultJSON)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)"), Item: recordItemForStore(receipt)}},
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneRecord(receipt), true, nil
	}
	if existing, found, lookupErr := s.Lookup(ctx, receipt.ConnectionID, receipt.CommandID); lookupErr != nil {
		return Record{}, false, lookupErr
	} else if found {
		if !existing.SameIdentity(receipt) {
			return Record{}, false, NewError("command_id_conflict")
		}
		return existing, false, nil
	}
	return Record{}, false, NewError("service_backup_finalize_race")
}

func validateServiceBackupReservation(reservation ServiceBackupReservation) error {
	if !contract.ValidConnectionID(reservation.ConnectionID) || !contract.ValidID(reservation.BackupID) || !contract.ValidID(reservation.ServiceID) || !contract.ValidID(reservation.DeploymentID) || !contract.ValidID(reservation.CommandID) || !validSHA256(reservation.RequestSHA256) || reservation.ExpectedGeneration < 1 || reservation.NodeCounter < 0 || !contract.ValidID(reservation.ApprovalID) || !contract.ValidID(reservation.ChallengeID) || !contract.ValidNodeKeyID(reservation.SignerKeyID) || len(reservation.RequestJSON) == 0 || len(reservation.RequestJSON) > contract.MaxCommandBytes || reservation.State != "reserved" && reservation.State != "finalized" {
		return NewError("service_backup_store_invalid")
	}
	var request contract.ServiceBackupRequest
	if json.Unmarshal(reservation.RequestJSON, &request) != nil || request.Validate() != nil || request.BackupID != reservation.BackupID || request.ServiceID != reservation.ServiceID || request.DeploymentID != reservation.DeploymentID {
		return NewError("service_backup_store_invalid")
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, reservation.RequestJSON) || reservation.State == "reserved" && len(reservation.ResultJSON) != 0 || reservation.State == "finalized" && len(reservation.ResultJSON) == 0 {
		return NewError("service_backup_store_invalid")
	}
	return nil
}

func serviceBackupItem(r ServiceBackupReservation) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "backup_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BackupID}, "service_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ServiceID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: r.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, "expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}, "approval_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ApprovalID}, "challenge_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ChallengeID}, "signer_key_id": &dynamodbtypes.AttributeValueMemberS{Value: r.SignerKeyID}, "request_json": &dynamodbtypes.AttributeValueMemberS{Value: string(r.RequestJSON)}, "state": &dynamodbtypes.AttributeValueMemberS{Value: r.State}}
}

func serviceBackupFromItem(item map[string]dynamodbtypes.AttributeValue) (ServiceBackupReservation, error) {
	var r ServiceBackupReservation
	var err error
	for name, target := range map[string]*string{"connection_id": &r.ConnectionID, "backup_id": &r.BackupID, "service_id": &r.ServiceID, "deployment_id": &r.DeploymentID, "command_id": &r.CommandID, "request_sha256": &r.RequestSHA256, "approval_id": &r.ApprovalID, "challenge_id": &r.ChallengeID, "signer_key_id": &r.SignerKeyID, "state": &r.State} {
		if *target, err = stringAttribute(item, name); err != nil {
			return r, err
		}
	}
	if r.ExpectedGeneration, err = numberAttribute(item, "expected_generation", false); err != nil {
		return r, err
	}
	if r.NodeCounter, err = numberAttribute(item, "node_counter", true); err != nil {
		return r, err
	}
	raw, err := stringAttribute(item, "request_json")
	if err != nil {
		return r, err
	}
	r.RequestJSON = []byte(raw)
	if value, ok := item["result_json"].(*dynamodbtypes.AttributeValueMemberS); ok {
		r.ResultJSON = []byte(value.Value)
	}
	return r, nil
}

func serviceBackupApprovalUsePut(table string, r ServiceBackupReservation, useID string) *dynamodbtypes.Put {
	return &dynamodbtypes.Put{TableName: &table, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(use_id)"), Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: useID}, "backup_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BackupID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}}}
}

func cloneServiceBackup(r ServiceBackupReservation) ServiceBackupReservation {
	r.RequestJSON = append([]byte(nil), r.RequestJSON...)
	r.ResultJSON = append([]byte(nil), r.ResultJSON...)
	return r
}
