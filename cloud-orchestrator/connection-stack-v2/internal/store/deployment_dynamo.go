package store

import (
	"context"
	"regexp"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

var clientTokenPattern = regexp.MustCompile(`^dtx-[0-9a-f]{60}$`)

func (s *DynamoRepository) LookupIssuedQuote(ctx context.Context, connectionID, quoteID string) (IssuedQuote, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.issuedQuotesTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "quote_id": &dynamodbtypes.AttributeValueMemberS{Value: quoteID}}})
	if err != nil {
		return IssuedQuote{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return IssuedQuote{}, false, nil
	}
	q := IssuedQuote{}
	if q.ConnectionID, err = stringAttribute(output.Item, "connection_id"); err != nil {
		return IssuedQuote{}, false, err
	}
	if q.QuoteID, err = stringAttribute(output.Item, "quote_id"); err != nil {
		return IssuedQuote{}, false, err
	}
	if q.PlanDigest, err = stringAttribute(output.Item, "plan_digest"); err != nil {
		return IssuedQuote{}, false, err
	}
	if q.CommandID, err = stringAttribute(output.Item, "command_id"); err != nil {
		return IssuedQuote{}, false, err
	}
	if q.RequestSHA256, err = stringAttribute(output.Item, "request_sha256"); err != nil {
		return IssuedQuote{}, false, err
	}
	if q.ValidUntil, err = stringAttribute(output.Item, "valid_until"); err != nil {
		return IssuedQuote{}, false, err
	}
	raw, err := stringAttribute(output.Item, "quote_json")
	if err != nil {
		return IssuedQuote{}, false, err
	}
	q.QuoteJSON = []byte(raw)
	if !contract.ValidConnectionID(connectionID) || q.ConnectionID != connectionID || q.QuoteID != quoteID || validateIssuedQuote(Record{ConnectionID: q.ConnectionID, CommandID: q.CommandID, RequestSHA256: q.RequestSHA256, Action: contract.ActionQuoteRequest}, q) != nil {
		return IssuedQuote{}, false, NewError("issued_quote_invalid")
	}
	return q, true, nil
}

func (s *DynamoRepository) LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.deploymentReservationsTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}}})
	if err != nil {
		return DeploymentReservation{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return DeploymentReservation{}, false, nil
	}
	r, err := deploymentFromItem(output.Item)
	if err != nil || r.ConnectionID != connectionID || r.DeploymentID != deploymentID {
		return DeploymentReservation{}, false, NewError("deployment_store_invalid")
	}
	session, found, err := s.LookupWorkerSession(ctx, r.BootstrapSessionID)
	if err != nil {
		return DeploymentReservation{}, false, err
	}
	if !found {
		return DeploymentReservation{}, false, NewError("deployment_store_invalid")
	}
	r.WorkerSession = session
	if validateDeploymentReservation(r) != nil {
		return DeploymentReservation{}, false, NewError("deployment_store_invalid")
	}
	return r, true, nil
}

func (s *DynamoRepository) ReserveDeployment(ctx context.Context, r DeploymentReservation) (DeploymentReservation, bool, error) {
	if validateDeploymentReservation(r) != nil {
		return DeploymentReservation{}, false, NewError("deployment_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{TableName: &s.countersTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}}, UpdateExpression: stringPtr("SET last_node_counter = :node_counter"), ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}}}}, {Put: &dynamodbtypes.Put{TableName: &s.deploymentReservationsTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(deployment_id)"), Item: deploymentItem(r)}}, {Put: approvalUsePut(s.approvalUsesTable, r, "approval#"+r.ApprovalID)}, {Put: approvalUsePut(s.approvalUsesTable, r, "challenge#"+r.ChallengeID)}, {Put: &dynamodbtypes.Put{TableName: &s.workerSessionsTable, ConditionExpression: stringPtr("attribute_not_exists(bootstrap_session_id)"), Item: workerSessionItem(r.WorkerSession)}}}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneDeployment(r), true, nil
	}
	return s.reconcileDeploymentReservation(ctx, r, err)
}

func (s *DynamoRepository) reconcileDeploymentReservation(ctx context.Context, r DeploymentReservation, _ error) (DeploymentReservation, bool, error) {
	if existing, found, err := s.LookupDeployment(ctx, r.ConnectionID, r.DeploymentID); err != nil {
		return DeploymentReservation{}, false, err
	} else if found {
		if !existing.SameIdentity(r) {
			return DeploymentReservation{}, false, NewError("deployment_id_conflict")
		}
		return existing, false, nil
	}
	if used, err := s.lookupApprovalUse(ctx, r.ConnectionID, "approval#"+r.ApprovalID); err != nil {
		return DeploymentReservation{}, false, err
	} else if used {
		return DeploymentReservation{}, false, NewError("approval_already_consumed")
	}
	if used, err := s.lookupApprovalUse(ctx, r.ConnectionID, "challenge#"+r.ChallengeID); err != nil {
		return DeploymentReservation{}, false, err
	} else if used {
		return DeploymentReservation{}, false, NewError("challenge_already_consumed")
	}
	if counter, found, err := s.lookupCounter(ctx, r.ConnectionID); err != nil {
		return DeploymentReservation{}, false, err
	} else if found && r.NodeCounter <= counter {
		return DeploymentReservation{}, false, NewError("stale_node_counter")
	}
	return DeploymentReservation{}, false, NewError("deployment_reservation_race")
}

func (s *DynamoRepository) FinalizeDeployment(ctx context.Context, r DeploymentReservation, receipt Record) (Record, bool, error) {
	if validateDeploymentReservation(r) != nil || receipt.Action != contract.ActionDeploymentCreate || !r.SameIdentity(DeploymentReservation{ConnectionID: receipt.ConnectionID, DeploymentID: r.DeploymentID, CommandID: receipt.CommandID, RequestSHA256: receipt.RequestSHA256, ExpectedGeneration: receipt.ExpectedGeneration, NodeCounter: receipt.NodeCounter, ApprovalID: r.ApprovalID, ChallengeID: r.ChallengeID, SignerKeyID: r.SignerKeyID, QuoteID: r.QuoteID, ClientToken: r.ClientToken, BootstrapSessionID: r.BootstrapSessionID, WorkerSession: r.WorkerSession, SpecJSON: r.SpecJSON}) || validateRecord(receipt) != nil {
		return Record{}, false, NewError("deployment_store_invalid")
	}
	instanceID, resultErr := deploymentInstanceID(receipt.ResultJSON)
	if resultErr != nil {
		return Record{}, false, NewError("deployment_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{{Update: &dynamodbtypes.Update{TableName: &s.deploymentReservationsTable, Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}}, UpdateExpression: stringPtr("SET #state = :finalized, result_json = :result_json"), ConditionExpression: stringPtr("request_sha256 = :request_sha256 AND #state = :reserved"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":finalized": &dynamodbtypes.AttributeValueMemberS{Value: "finalized"}, ":reserved": &dynamodbtypes.AttributeValueMemberS{Value: "reserved"}, ":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, ":result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(receipt.ResultJSON)}}}}, {Update: &dynamodbtypes.Update{TableName: &s.workerSessionsTable, Key: map[string]dynamodbtypes.AttributeValue{"bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BootstrapSessionID}}, UpdateExpression: stringPtr("SET #state = :bound, expected_instance_id = :instance_id"), ConditionExpression: stringPtr("connection_id = :connection_id AND deployment_id = :deployment_id AND request_sha256 = :request_sha256 AND #state = :issued"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":bound": &dynamodbtypes.AttributeValueMemberS{Value: "bound"}, ":issued": &dynamodbtypes.AttributeValueMemberS{Value: "issued"}, ":instance_id": &dynamodbtypes.AttributeValueMemberS{Value: instanceID}, ":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, ":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, ":request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}}}}, {Put: &dynamodbtypes.Put{TableName: &s.receiptsTable, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)"), Item: recordItemForStore(receipt)}}}
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
	return Record{}, false, NewError("deployment_finalize_race")
}

func (s *DynamoRepository) lookupApprovalUse(ctx context.Context, connectionID, useID string) (bool, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.approvalUsesTable, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: useID}}})
	if err != nil {
		return false, NewError("connection_stack_store_unavailable")
	}
	return len(out.Item) > 0, nil
}

func approvalUsePut(table string, r DeploymentReservation, useID string) *dynamodbtypes.Put {
	return &dynamodbtypes.Put{TableName: &table, ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(use_id)"), Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: useID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}}}
}
func deploymentItem(r DeploymentReservation) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: r.DeploymentID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: r.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, "expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}, "approval_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ApprovalID}, "challenge_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ChallengeID}, "signer_key_id": &dynamodbtypes.AttributeValueMemberS{Value: r.SignerKeyID}, "quote_id": &dynamodbtypes.AttributeValueMemberS{Value: r.QuoteID}, "client_token": &dynamodbtypes.AttributeValueMemberS{Value: r.ClientToken}, "bootstrap_session_id": &dynamodbtypes.AttributeValueMemberS{Value: r.BootstrapSessionID}, "spec_json": &dynamodbtypes.AttributeValueMemberS{Value: string(r.SpecJSON)}, "state": &dynamodbtypes.AttributeValueMemberS{Value: r.State}}
}
func recordItemForStore(r Record) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: r.ConnectionID}, "command_id": &dynamodbtypes.AttributeValueMemberS{Value: r.CommandID}, "request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: r.RequestSHA256}, "expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.ExpectedGeneration, 10)}, "node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(r.NodeCounter, 10)}, "action": &dynamodbtypes.AttributeValueMemberS{Value: r.Action}, "result_json": &dynamodbtypes.AttributeValueMemberS{Value: string(r.ResultJSON)}}
}

func deploymentFromItem(item map[string]dynamodbtypes.AttributeValue) (DeploymentReservation, error) {
	var r DeploymentReservation
	var err error
	if r.ConnectionID, err = stringAttribute(item, "connection_id"); err != nil {
		return r, err
	}
	if r.DeploymentID, err = stringAttribute(item, "deployment_id"); err != nil {
		return r, err
	}
	if r.CommandID, err = stringAttribute(item, "command_id"); err != nil {
		return r, err
	}
	if r.RequestSHA256, err = stringAttribute(item, "request_sha256"); err != nil {
		return r, err
	}
	if r.ExpectedGeneration, err = numberAttribute(item, "expected_generation", false); err != nil {
		return r, err
	}
	if r.NodeCounter, err = numberAttribute(item, "node_counter", true); err != nil {
		return r, err
	}
	if r.ApprovalID, err = stringAttribute(item, "approval_id"); err != nil {
		return r, err
	}
	if r.ChallengeID, err = stringAttribute(item, "challenge_id"); err != nil {
		return r, err
	}
	if r.SignerKeyID, err = stringAttribute(item, "signer_key_id"); err != nil {
		return r, err
	}
	if r.QuoteID, err = stringAttribute(item, "quote_id"); err != nil {
		return r, err
	}
	if r.ClientToken, err = stringAttribute(item, "client_token"); err != nil {
		return r, err
	}
	if r.BootstrapSessionID, err = stringAttribute(item, "bootstrap_session_id"); err != nil {
		return r, err
	}
	spec, err := stringAttribute(item, "spec_json")
	if err != nil {
		return r, err
	}
	r.SpecJSON = []byte(spec)
	if r.State, err = stringAttribute(item, "state"); err != nil {
		return r, err
	}
	if raw, ok := item["result_json"].(*dynamodbtypes.AttributeValueMemberS); ok {
		r.ResultJSON = []byte(raw.Value)
	}
	if validateDeploymentReservationIdentity(r) != nil {
		return r, NewError("deployment_store_invalid")
	}
	return r, nil
}
func validateDeploymentReservation(r DeploymentReservation) error {
	if validateDeploymentReservationIdentity(r) != nil || !validWorkerSession(r.WorkerSession) || r.WorkerSession.BootstrapSessionID != r.BootstrapSessionID || r.WorkerSession.ConnectionID != r.ConnectionID || r.WorkerSession.DeploymentID != r.DeploymentID || r.WorkerSession.RequestSHA256 != r.RequestSHA256 {
		return NewError("deployment_store_invalid")
	}
	return nil
}

func validateDeploymentReservationIdentity(r DeploymentReservation) error {
	if !contract.ValidConnectionID(r.ConnectionID) || !contract.ValidID(r.DeploymentID) || !contract.ValidID(r.CommandID) || !validSHA256(r.RequestSHA256) || r.ExpectedGeneration < 1 || r.NodeCounter < 0 || r.ApprovalID == "" || r.ChallengeID == "" || r.SignerKeyID == "" || !contract.ValidID(r.QuoteID) || !clientTokenPattern.MatchString(r.ClientToken) || !contract.ValidID(r.BootstrapSessionID) || len(r.SpecJSON) == 0 || len(r.SpecJSON) > contract.MaxCommandBytes || (r.State != "reserved" && r.State != "finalized") {
		return NewError("deployment_store_invalid")
	}
	return nil
}
func cloneDeployment(r DeploymentReservation) DeploymentReservation {
	r.SpecJSON = append([]byte(nil), r.SpecJSON...)
	r.ResultJSON = append([]byte(nil), r.ResultJSON...)
	return r
}

func deploymentInstanceID(raw []byte) (string, error) {
	receipt, err := contract.StoredDeploymentReceipt(raw)
	if err != nil {
		return "", err
	}
	return receipt.InstanceID, nil
}
