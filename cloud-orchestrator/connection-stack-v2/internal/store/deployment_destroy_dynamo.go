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

func (s *DynamoRepository) LookupDeploymentDestroy(ctx context.Context, connectionID, deploymentID string) (DeploymentDestroyReservation, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.deploymentDestroyTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}}})
	if err != nil {
		return DeploymentDestroyReservation{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return DeploymentDestroyReservation{}, false, nil
	}
	reservation, err := deploymentDestroyFromItem(output.Item)
	if err != nil || reservation.ConnectionID != connectionID || reservation.DeploymentID != deploymentID || validateDeploymentDestroyReservation(reservation) != nil {
		return DeploymentDestroyReservation{}, false, NewError("deployment_destroy_store_invalid")
	}
	return reservation, true, nil
}

func (s *DynamoRepository) ReserveDeploymentDestroy(ctx context.Context, reservation DeploymentDestroyReservation) (DeploymentDestroyReservation, bool, error) {
	if validateDeploymentDestroyReservation(reservation) != nil || reservation.State != "reserved" || len(reservation.ResultJSON) != 0 {
		return DeploymentDestroyReservation{}, false, NewError("deployment_destroy_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(reservation.NodeCounter, 10)}}}},
		{Put: &dynamodbtypes.Put{TableName: &s.deploymentDestroyTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(deployment_id)"), Item: deploymentDestroyItem(reservation)}},
		{Put: destroyApprovalUsePut(s.approvalUsesTable, reservation, "approval#"+reservation.ApprovalID)},
		{Put: destroyApprovalUsePut(s.approvalUsesTable, reservation, "challenge#"+reservation.ChallengeID)},
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneDeploymentDestroy(reservation), true, nil
	}
	if existing, found, lookupErr := s.LookupDeploymentDestroy(ctx, reservation.ConnectionID, reservation.DeploymentID); lookupErr != nil {
		return DeploymentDestroyReservation{}, false, lookupErr
	} else if found {
		if !existing.SameIdentity(reservation) {
			return DeploymentDestroyReservation{}, false, NewError("deployment_destroy_conflict")
		}
		return existing, false, nil
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "approval#"+reservation.ApprovalID); lookupErr != nil {
		return DeploymentDestroyReservation{}, false, lookupErr
	} else if used {
		return DeploymentDestroyReservation{}, false, NewError("approval_already_consumed")
	}
	if used, lookupErr := s.lookupApprovalUse(ctx, reservation.ConnectionID, "challenge#"+reservation.ChallengeID); lookupErr != nil {
		return DeploymentDestroyReservation{}, false, lookupErr
	} else if used {
		return DeploymentDestroyReservation{}, false, NewError("challenge_already_consumed")
	}
	if counter, found, lookupErr := s.lookupCounter(ctx, reservation.ConnectionID); lookupErr != nil {
		return DeploymentDestroyReservation{}, false, lookupErr
	} else if found && reservation.NodeCounter <= counter {
		return DeploymentDestroyReservation{}, false, NewError("stale_node_counter")
	}
	return DeploymentDestroyReservation{}, false, NewError("deployment_destroy_reservation_race")
}

func (s *DynamoRepository) FinalizeDeploymentDestroy(ctx context.Context, reservation DeploymentDestroyReservation, receipt Record) (Record, bool, error) {
	if validateDeploymentDestroyReservation(reservation) != nil || reservation.State != "reserved" || receipt.Action != contract.ActionDeploymentDestroy || !reservation.SameIdentity(DeploymentDestroyReservation{ConnectionID: receipt.ConnectionID, DeploymentID: reservation.DeploymentID, ServiceID: reservation.ServiceID, CommandID: receipt.CommandID, RequestSHA256: receipt.RequestSHA256, ExpectedGeneration: receipt.ExpectedGeneration, NodeCounter: receipt.NodeCounter, ApprovalID: reservation.ApprovalID, ChallengeID: reservation.ChallengeID, SignerKeyID: reservation.SignerKeyID, RequestJSON: reservation.RequestJSON}) || validateRecord(receipt) != nil {
		return Record{}, false, NewError("deployment_destroy_store_invalid")
	}
	var result contract.DeploymentDestroyResult
	var request contract.DeploymentDestroyRequest
	if json.Unmarshal(receipt.ResultJSON, &result) != nil || json.Unmarshal(reservation.RequestJSON, &request) != nil ||
		result.Schema != contract.DeploymentDestroyResultSchema || result.Status != "verified_destroyed" ||
		result.Receipt.Schema != contract.ReceiptSchema || result.Receipt.Disposition != "committed" ||
		result.Receipt.ConnectionID != receipt.ConnectionID || result.Receipt.CommandID != receipt.CommandID || result.Receipt.RequestSHA256 != receipt.RequestSHA256 || result.Receipt.ExpectedGeneration != receipt.ExpectedGeneration || result.Receipt.NodeCounter != receipt.NodeCounter || result.Receipt.Action != contract.ActionDeploymentDestroy ||
		result.Deployment.DeploymentID != request.DeploymentID || result.Deployment.InstanceID != request.InstanceID || !sameDestroyIDs(result.Deployment.VolumeIDs, request.VolumeIDs) || !sameDestroyIDs(result.Deployment.NetworkInterfaceIDs, request.NetworkInterfaceIDs) || !sameDestroyIDs(result.Deployment.SecretRefs, request.SecretRefs) {
		return Record{}, false, NewError("deployment_destroy_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{TableName: &s.deploymentDestroyTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.DeploymentID}}, UpdateExpression: stringPtr("SET #state=:finalized,result_json=:result_json"), ConditionExpression: stringPtr("request_sha256=:request_sha256 AND #state=:reserved"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":finalized": &dynamodbtypes.AttributeValueMemberS{Value: "finalized"}, ":reserved": &dynamodbtypes.AttributeValueMemberS{Value: "reserved"}, ":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RequestSHA256}, ":result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(receipt.ResultJSON)}}}},
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
	return Record{}, false, NewError("deployment_destroy_finalize_race")
}

func validateDeploymentDestroyReservation(reservation DeploymentDestroyReservation) error {
	if !contract.ValidConnectionID(reservation.ConnectionID) || !contract.ValidID(reservation.DeploymentID) || (reservation.ServiceID != "" && !contract.ValidID(reservation.ServiceID)) || !contract.ValidID(reservation.CommandID) || !validSHA256(reservation.RequestSHA256) || reservation.ExpectedGeneration < 1 || reservation.NodeCounter < 0 || !contract.ValidID(reservation.ApprovalID) || !contract.ValidID(reservation.ChallengeID) || !contract.ValidNodeKeyID(reservation.SignerKeyID) || len(reservation.RequestJSON) == 0 || len(reservation.RequestJSON) > contract.MaxCommandBytes || (reservation.State != "reserved" && reservation.State != "finalized") {
		return NewError("deployment_destroy_store_invalid")
	}
	var request contract.DeploymentDestroyRequest
	if json.Unmarshal(reservation.RequestJSON, &request) != nil || request.Validate() != nil || request.DeploymentID != reservation.DeploymentID || request.ServiceID != reservation.ServiceID {
		return NewError("deployment_destroy_store_invalid")
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, reservation.RequestJSON) {
		return NewError("deployment_destroy_store_invalid")
	}
	if reservation.State == "reserved" && len(reservation.ResultJSON) != 0 || reservation.State == "finalized" && len(reservation.ResultJSON) == 0 {
		return NewError("deployment_destroy_store_invalid")
	}
	return nil
}

func deploymentDestroyItem(reservation DeploymentDestroyReservation) map[string]dynamodbtypes.AttributeValue {
	item := map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.DeploymentID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RequestSHA256}, "expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(reservation.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(reservation.NodeCounter, 10)}, "approval_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ApprovalID}, "challenge_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ChallengeID}, "signer_key_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.SignerKeyID}, "request_json": &dynamodbtypes.AttributeValueMemberS{Value: string(reservation.RequestJSON)}, "state": &dynamodbtypes.AttributeValueMemberS{Value: reservation.State}}
	if reservation.ServiceID != "" {
		item["service_id"] = &dynamodbtypes.AttributeValueMemberS{Value: reservation.ServiceID}
	}
	return item
}

func deploymentDestroyFromItem(item map[string]dynamodbtypes.AttributeValue) (DeploymentDestroyReservation, error) {
	var reservation DeploymentDestroyReservation
	var err error
	if reservation.ConnectionID, err = stringAttribute(item, "connection_id"); err != nil {
		return reservation, err
	}
	if reservation.DeploymentID, err = stringAttribute(item, "deployment_id"); err != nil {
		return reservation, err
	}
	if _, present := item["service_id"]; present {
		if reservation.ServiceID, err = stringAttribute(item, "service_id"); err != nil {
			return reservation, err
		}
	}
	if reservation.CommandID, err = stringAttribute(item, "command_id"); err != nil {
		return reservation, err
	}
	if reservation.RequestSHA256, err = stringAttribute(item, "request_sha256"); err != nil {
		return reservation, err
	}
	if reservation.ExpectedGeneration, err = numberAttribute(item, "expected_generation", false); err != nil {
		return reservation, err
	}
	if reservation.NodeCounter, err = numberAttribute(item, "node_counter", true); err != nil {
		return reservation, err
	}
	if reservation.ApprovalID, err = stringAttribute(item, "approval_id"); err != nil {
		return reservation, err
	}
	if reservation.ChallengeID, err = stringAttribute(item, "challenge_id"); err != nil {
		return reservation, err
	}
	if reservation.SignerKeyID, err = stringAttribute(item, "signer_key_id"); err != nil {
		return reservation, err
	}
	requestJSON, err := stringAttribute(item, "request_json")
	if err != nil {
		return reservation, err
	}
	reservation.RequestJSON = []byte(requestJSON)
	if reservation.State, err = stringAttribute(item, "state"); err != nil {
		return reservation, err
	}
	if raw, ok := item["result_json"].(*dynamodbtypes.AttributeValueMemberS); ok {
		reservation.ResultJSON = []byte(raw.Value)
	}
	return reservation, nil
}

func destroyApprovalUsePut(table string, reservation DeploymentDestroyReservation, useID string) *dynamodbtypes.Put {
	return &dynamodbtypes.Put{TableName: &table, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(use_id)"), Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: useID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.DeploymentID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: reservation.RequestSHA256}}}
}

func cloneDeploymentDestroy(reservation DeploymentDestroyReservation) DeploymentDestroyReservation {
	reservation.RequestJSON = append([]byte(nil), reservation.RequestJSON...)
	reservation.ResultJSON = append([]byte(nil), reservation.ResultJSON...)
	return reservation
}

func sameDestroyIDs(left, right []string) bool {
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
