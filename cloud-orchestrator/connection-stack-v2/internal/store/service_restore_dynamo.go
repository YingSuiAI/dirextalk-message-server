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

func (s *DynamoRepository) LookupServiceRestore(ctx context.Context, connectionID, restoreID string) (ServiceRestoreReservation, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.serviceRestoresTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "restore_id": &dynamodbtypes.AttributeValueMemberS{Value: restoreID}}})
	if err != nil {
		return ServiceRestoreReservation{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return ServiceRestoreReservation{}, false, nil
	}
	reservation, err := serviceRestoreFromItem(output.Item)
	if err != nil || reservation.ConnectionID != connectionID || reservation.RestoreID != restoreID || validateServiceRestoreReservation(reservation) != nil {
		return ServiceRestoreReservation{}, false, NewError("service_restore_store_invalid")
	}
	return reservation, true, nil
}

func (s *DynamoRepository) ReserveServiceRestore(ctx context.Context, reservation ServiceRestoreReservation) (ServiceRestoreReservation, bool, error) {
	if validateServiceRestoreReservation(reservation) != nil || reservation.State != "reserved" || len(reservation.ResultJSON) != 0 {
		return ServiceRestoreReservation{}, false, NewError("service_restore_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(reservation.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.serviceRestoresTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(restore_id)"), Item: serviceRestoreItem(reservation)}},
		{Put: serviceRestoreApprovalUsePut(s.approvalUsesTable, reservation, "approval#"+reservation.ApprovalID)},
		{Put: serviceRestoreApprovalUsePut(s.approvalUsesTable, reservation, "challenge#"+reservation.ChallengeID)},
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneServiceRestore(reservation), true, nil
	}
	if existing, found, lookupErr := s.LookupServiceRestore(ctx, reservation.ConnectionID, reservation.RestoreID); lookupErr != nil {
		return ServiceRestoreReservation{}, false, lookupErr
	} else if found {
		if !existing.SameIdentity(reservation) {
			return ServiceRestoreReservation{}, false, NewError("service_restore_conflict")
		}
		return existing, false, nil
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "approval#"+reservation.ApprovalID); lookupErr != nil {
		return ServiceRestoreReservation{}, false, lookupErr
	} else if used {
		return ServiceRestoreReservation{}, false, NewError("approval_already_consumed")
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "challenge#"+reservation.ChallengeID); lookupErr != nil {
		return ServiceRestoreReservation{}, false, lookupErr
	} else if used {
		return ServiceRestoreReservation{}, false, NewError("challenge_already_consumed")
	}
	if counter, found, lookupErr := s.lookupCounter(ctx, reservation.ConnectionID); lookupErr != nil {
		return ServiceRestoreReservation{}, false, lookupErr
	} else if found && reservation.NodeCounter <= counter {
		return ServiceRestoreReservation{}, false, NewError("stale_node_counter")
	}
	return ServiceRestoreReservation{}, false, NewError("service_restore_reservation_race")
}

func (s *DynamoRepository) FinalizeServiceRestore(ctx context.Context, reservation ServiceRestoreReservation, receipt Record) (Record, bool, error) {
	if validateServiceRestoreReservation(reservation) != nil || reservation.State != "reserved" || receipt.Action != contract.ActionServiceRestore || !reservation.SameIdentity(ServiceRestoreReservation{ConnectionID: receipt.ConnectionID, RestoreID: reservation.RestoreID, ServiceID: reservation.ServiceID, DeploymentID: reservation.DeploymentID, BackupID: reservation.BackupID, CommandID: receipt.CommandID, RequestSHA256: receipt.RequestSHA256, ExpectedGeneration: receipt.ExpectedGeneration, NodeCounter: receipt.NodeCounter, ApprovalID: reservation.ApprovalID, ChallengeID: reservation.ChallengeID, SignerKeyID: reservation.SignerKeyID}) {
		return Record{}, false, NewError("service_restore_store_invalid")
	}
	var result contract.ServiceRestoreResult
	var request contract.ServiceRestoreRequest
	if json.Unmarshal(receipt.ResultJSON, &result) != nil || json.Unmarshal(reservation.RequestJSON, &request) != nil || result.Schema != contract.ServiceRestoreResultSchema || (result.Status != "aws_restore_applied" && result.Status != "aws_original_restored" && result.Status != "restore_blocked") || result.Receipt.Schema != contract.ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != receipt.ConnectionID || result.Receipt.CommandID != receipt.CommandID || result.Receipt.RequestSHA256 != receipt.RequestSHA256 || result.Receipt.ExpectedGeneration != receipt.ExpectedGeneration || result.Receipt.NodeCounter != receipt.NodeCounter || result.Receipt.Action != receipt.Action || contract.ValidateServiceRestoreEvidence(request, result.Restore) != nil {
		return Record{}, false, NewError("service_restore_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.serviceRestoresTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "restore_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RestoreID}}, UpdateExpression: stringPtr("SET #state=:finalized,result_json=:result_json"), ConditionExpression: stringPtr("request_sha256=:request_sha256 AND #state=:reserved"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":finalized": &dynamodbtypes.AttributeValueMemberS{Value: "finalized"}, ":reserved": &dynamodbtypes.AttributeValueMemberS{Value: "reserved"}, ":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RequestSHA256}, ":result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(receipt.ResultJSON)}}}},
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
	return Record{}, false, NewError("service_restore_finalize_race")
}

func validateServiceRestoreReservation(reservation ServiceRestoreReservation) error {
	if !contract.ValidConnectionID(reservation.ConnectionID) || !contract.ValidID(reservation.RestoreID) || !contract.ValidID(reservation.ServiceID) || !contract.ValidID(reservation.DeploymentID) || !contract.ValidID(reservation.BackupID) || !contract.ValidID(reservation.CommandID) || !validSHA256(reservation.RequestSHA256) || reservation.ExpectedGeneration < 1 || reservation.NodeCounter < 0 || !contract.ValidID(reservation.ApprovalID) || !contract.ValidID(reservation.ChallengeID) || !contract.ValidNodeKeyID(reservation.SignerKeyID) || len(reservation.RequestJSON) == 0 || len(reservation.RequestJSON) > contract.MaxCommandBytes || (reservation.State != "reserved" && reservation.State != "finalized") {
		return NewError("service_restore_store_invalid")
	}
	var request contract.ServiceRestoreRequest
	if json.Unmarshal(reservation.RequestJSON, &request) != nil || request.Validate() != nil || request.RestoreID != reservation.RestoreID || request.ServiceID != reservation.ServiceID || request.DeploymentID != reservation.DeploymentID || request.BackupID != reservation.BackupID {
		return NewError("service_restore_store_invalid")
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, reservation.RequestJSON) || reservation.State == "reserved" && len(reservation.ResultJSON) != 0 || reservation.State == "finalized" && len(reservation.ResultJSON) == 0 {
		return NewError("service_restore_store_invalid")
	}
	return nil
}

func serviceRestoreItem(r ServiceRestoreReservation) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "restore_id": &dynamodbtypes.AttributeValueMemberS{Value: r.RestoreID},
		"service_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ServiceID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, "backup_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BackupID},
		"command_id": &dynamodbtypes.AttributeValueMemberS{Value: r.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256},
		"expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)},
		"approval_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ApprovalID}, "challenge_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ChallengeID}, "signer_key_id": &dynamodbtypes.AttributeValueMemberS{Value: r.SignerKeyID},
		"request_json": &dynamodbtypes.AttributeValueMemberS{Value: string(r.RequestJSON)}, "state": &dynamodbtypes.AttributeValueMemberS{Value: r.State},
	}
}

func serviceRestoreFromItem(item map[string]dynamodbtypes.AttributeValue) (ServiceRestoreReservation, error) {
	var r ServiceRestoreReservation
	var err error
	for name, target := range map[string]*string{"connection_id": &r.ConnectionID, "restore_id": &r.RestoreID, "service_id": &r.ServiceID, "deployment_id": &r.DeploymentID, "backup_id": &r.BackupID, "command_id": &r.CommandID, "request_sha256": &r.RequestSHA256, "approval_id": &r.ApprovalID, "challenge_id": &r.ChallengeID, "signer_key_id": &r.SignerKeyID, "state": &r.State} {
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

func serviceRestoreApprovalUsePut(table string, r ServiceRestoreReservation, useID string) *dynamodbtypes.Put {
	return &dynamodbtypes.Put{TableName: &table, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(use_id)"), Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: useID}, "restore_id": &dynamodbtypes.AttributeValueMemberS{Value: r.RestoreID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}}}
}

func cloneServiceRestore(r ServiceRestoreReservation) ServiceRestoreReservation {
	r.RequestJSON = append([]byte(nil), r.RequestJSON...)
	r.ResultJSON = append([]byte(nil), r.ResultJSON...)
	return r
}
