package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDynamoRepositoryCommitsReceiptCounterAndQuoteAtomically(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	record, quote := validQuoteRecord()

	stored, created, err := repository.Commit(t.Context(), record, &quote)
	if err != nil || !created || !stored.SameIdentity(record) {
		t.Fatalf("Commit() = (%#v, %t, %v)", stored, created, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 3 {
		t.Fatalf("transaction = %#v", client.transactInput)
	}
	counter := client.transactInput.TransactItems[0].Update
	receipt := client.transactInput.TransactItems[1].Put
	issuedQuote := client.transactInput.TransactItems[2].Put
	if counter == nil || counter.TableName == nil || *counter.TableName != "counters" || counter.ConditionExpression == nil || *counter.ConditionExpression != "attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter" {
		t.Fatalf("counter update = %#v", counter)
	}
	if receipt == nil || receipt.TableName == nil || *receipt.TableName != "receipts" || receipt.ConditionExpression == nil || !strings.Contains(*receipt.ConditionExpression, "attribute_not_exists") {
		t.Fatalf("receipt put = %#v", receipt)
	}
	if issuedQuote == nil || issuedQuote.TableName == nil || *issuedQuote.TableName != "quotes" || issuedQuote.ConditionExpression == nil || !strings.Contains(*issuedQuote.ConditionExpression, "attribute_not_exists") {
		t.Fatalf("issued quote put = %#v", issuedQuote)
	}
}

func TestDynamoRepositoryReconcilesIndeterminateCommit(t *testing.T) {
	record, quote := validQuoteRecord()
	client := &fakeDynamo{transactErr: &dynamodbtypes.TransactionCanceledException{}}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if input.TableName != nil && *input.TableName == "receipts" {
			return &dynamodb.GetItemOutput{Item: recordItem(record)}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)

	stored, created, err := repository.Commit(t.Context(), record, &quote)
	if err != nil || created || !stored.SameIdentity(record) {
		t.Fatalf("Commit() reconciliation = (%#v, %t, %v)", stored, created, err)
	}

	conflict := record
	conflict.NodeCounter++
	if _, _, err := repository.Commit(t.Context(), conflict, &quote); Code(err) != "command_id_conflict" {
		t.Fatalf("conflicting replay code = %q, want command_id_conflict", Code(err))
	}
}

func TestDynamoRepositoryClassifiesStaleCounterWithoutLeakingAWSFailure(t *testing.T) {
	record, quote := validQuoteRecord()
	client := &fakeDynamo{transactErr: &dynamodbtypes.TransactionCanceledException{}}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if input.TableName != nil && *input.TableName == "counters" {
			return &dynamodb.GetItemOutput{Item: map[string]dynamodbtypes.AttributeValue{
				"connection_id":     &dynamodbtypes.AttributeValueMemberS{Value: record.ConnectionID},
				"last_node_counter": &dynamodbtypes.AttributeValueMemberN{Value: "7"},
			}}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)

	if _, _, err := repository.Commit(t.Context(), record, &quote); Code(err) != "stale_node_counter" {
		t.Fatalf("stale counter code = %q, want stale_node_counter", Code(err))
	}

	client.transactErr = errors.New("PRIVATE_AWS_FAILURE_TEXT")
	if _, _, err := repository.Commit(t.Context(), record, &quote); err == nil || strings.Contains(err.Error(), "PRIVATE_AWS_FAILURE_TEXT") || Code(err) != "connection_stack_store_unavailable" {
		t.Fatalf("unsafe store error = %v", err)
	}
}

func TestDynamoRepositoryReservesCounterDeploymentApprovalAndChallengeAtomically(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	reservation := validDeploymentReservation()
	stored, created, err := repository.ReserveDeployment(t.Context(), reservation)
	if err != nil || !created || !stored.SameIdentity(reservation) {
		t.Fatalf("ReserveDeployment()=(%#v,%t,%v)", stored, created, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 4 {
		t.Fatalf("reserve transaction=%#v", client.transactInput)
	}
	if client.transactInput.TransactItems[0].Update == nil || client.transactInput.TransactItems[1].Put == nil || client.transactInput.TransactItems[2].Put == nil || client.transactInput.TransactItems[3].Put == nil {
		t.Fatalf("reserve transaction shape=%#v", client.transactInput.TransactItems)
	}
}

func TestDynamoRepositoryRejectsConsumedApprovalOnReservationRace(t *testing.T) {
	reservation := validDeploymentReservation()
	client := &fakeDynamo{transactErr: errors.New("PRIVATE_INDETERMINATE")}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if input.TableName != nil && *input.TableName == "approval-uses" {
			if use, ok := input.Key["use_id"].(*dynamodbtypes.AttributeValueMemberS); ok && strings.HasPrefix(use.Value, "approval#") {
				return &dynamodb.GetItemOutput{Item: map[string]dynamodbtypes.AttributeValue{"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: reservation.ConnectionID}, "use_id": &dynamodbtypes.AttributeValueMemberS{Value: use.Value}}}, nil
			}
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)
	if _, _, err := repository.ReserveDeployment(t.Context(), reservation); Code(err) != "approval_already_consumed" || strings.Contains(err.Error(), "PRIVATE_INDETERMINATE") {
		t.Fatalf("ReserveDeployment() error=%v code=%s", err, Code(err))
	}
}

func TestDynamoRepositoryRecoversReservationAfterIndeterminateCommit(t *testing.T) {
	reservation := validDeploymentReservation()
	client := &fakeDynamo{transactErr: errors.New("PRIVATE_RESPONSE_LOST")}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if input.TableName != nil && *input.TableName == "deployments" {
			return &dynamodb.GetItemOutput{Item: deploymentItem(reservation)}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)
	stored, created, err := repository.ReserveDeployment(t.Context(), reservation)
	if err != nil || created || !stored.SameIdentity(reservation) || strings.Contains(fmt.Sprint(err), "PRIVATE_RESPONSE_LOST") {
		t.Fatalf("ReserveDeployment()=(%#v,%t,%v)", stored, created, err)
	}
}

func TestDynamoRepositoryFinalizesReservationAndReceiptAtomically(t *testing.T) {
	client := &fakeDynamo{}
	repository := mustDynamoRepository(t, client)
	reservation := validDeploymentReservation()
	receipt := Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: "deployment.create", ResultJSON: []byte(`{"status":"deployment_created"}`)}
	stored, created, err := repository.FinalizeDeployment(t.Context(), reservation, receipt)
	if err != nil || !created || !stored.SameIdentity(receipt) {
		t.Fatalf("FinalizeDeployment()=(%#v,%t,%v)", stored, created, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 2 || client.transactInput.TransactItems[0].Update == nil || client.transactInput.TransactItems[1].Put == nil {
		t.Fatalf("finalize transaction=%#v", client.transactInput)
	}
}

func TestDynamoRepositoryRecoversFinalReceiptAfterIndeterminateFinalize(t *testing.T) {
	reservation := validDeploymentReservation()
	receipt := Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: "deployment.create", ResultJSON: []byte(`{"status":"deployment_created"}`)}
	client := &fakeDynamo{transactErr: errors.New("PRIVATE_RESPONSE_LOST")}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if input.TableName != nil && *input.TableName == "receipts" {
			item := recordItem(receipt)
			item["expected_generation"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(receipt.ExpectedGeneration, 10)}
			item["node_counter"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(receipt.NodeCounter, 10)}
			return &dynamodb.GetItemOutput{Item: item}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)
	stored, created, err := repository.FinalizeDeployment(t.Context(), reservation, receipt)
	if err != nil || created || !stored.SameIdentity(receipt) || strings.Contains(fmt.Sprint(err), "PRIVATE_RESPONSE_LOST") {
		t.Fatalf("FinalizeDeployment()=(%#v,%t,%v)", stored, created, err)
	}
}

type fakeDynamo struct {
	getItem       func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	transactErr   error
	transactInput *dynamodb.TransactWriteItemsInput
}

func (f *fakeDynamo) GetItem(_ context.Context, input *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.getItem != nil {
		return f.getItem(input)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func (f *fakeDynamo) TransactWriteItems(_ context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.transactInput = input
	if f.transactErr != nil {
		return nil, f.transactErr
	}
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func mustDynamoRepository(t *testing.T, client DynamoAPI) *DynamoRepository {
	t.Helper()
	repository, err := NewDynamoRepository(DynamoConfig{Client: client, ReceiptsTable: "receipts", CountersTable: "counters", IssuedQuotesTable: "quotes", DeploymentReservationsTable: "deployments", ApprovalUsesTable: "approval-uses"})
	if err != nil {
		t.Fatalf("NewDynamoRepository(): %v", err)
	}
	return repository
}

func validQuoteRecord() (Record, IssuedQuote) {
	requestSHA := strings.Repeat("a", 64)
	record := Record{
		ConnectionID: "connection-0001", CommandID: "command-0001", RequestSHA256: requestSHA,
		ExpectedGeneration: 1, NodeCounter: 7, Action: "quote.request", ResultJSON: []byte(`{"status":"quote_issued"}`),
	}
	quote := IssuedQuote{
		ConnectionID: record.ConnectionID, QuoteID: "quote-00000000000000000000000000000000",
		PlanDigest: "sha256:" + strings.Repeat("b", 64), CommandID: record.CommandID, RequestSHA256: requestSHA,
		ValidUntil: "2026-07-15T01:17:04.000Z", QuoteJSON: []byte(`{"schema":"dirextalk.aws.quote/v1"}`),
	}
	return record, quote
}

func recordItem(record Record) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"connection_id":       &dynamodbtypes.AttributeValueMemberS{Value: record.ConnectionID},
		"command_id":          &dynamodbtypes.AttributeValueMemberS{Value: record.CommandID},
		"request_sha256":      &dynamodbtypes.AttributeValueMemberS{Value: record.RequestSHA256},
		"expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
		"node_counter":        &dynamodbtypes.AttributeValueMemberN{Value: "7"},
		"action":              &dynamodbtypes.AttributeValueMemberS{Value: record.Action},
		"result_json":         &dynamodbtypes.AttributeValueMemberS{Value: string(record.ResultJSON)},
	}
}

func validDeploymentReservation() DeploymentReservation {
	return DeploymentReservation{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", CommandID: "command-deployment-0001", RequestSHA256: strings.Repeat("d", 64), ExpectedGeneration: 1, NodeCounter: 8, ApprovalID: "approval-0001", ChallengeID: "challenge-0001", SignerKeyID: "device-key-1", QuoteID: "quote-00000000000000000000000000000000", ClientToken: "dtx-" + strings.Repeat("e", 60), SpecJSON: []byte(`{"deployment_id":"deployment-0001"}`), State: "reserved"}
}
