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
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 5 {
		t.Fatalf("reserve transaction=%#v", client.transactInput)
	}
	if client.transactInput.TransactItems[0].Update == nil || client.transactInput.TransactItems[1].Put == nil || client.transactInput.TransactItems[2].Put == nil || client.transactInput.TransactItems[3].Put == nil || client.transactInput.TransactItems[4].Put == nil {
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
		if input.TableName != nil && *input.TableName == "worker-sessions" {
			return &dynamodb.GetItemOutput{Item: workerSessionItem(reservation.WorkerSession)}, nil
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
	receipt := validDeploymentRecord(reservation)
	stored, created, err := repository.FinalizeDeployment(t.Context(), reservation, receipt)
	if err != nil || !created || !stored.SameIdentity(receipt) {
		t.Fatalf("FinalizeDeployment()=(%#v,%t,%v)", stored, created, err)
	}
	if client.transactInput == nil || len(client.transactInput.TransactItems) != 3 || client.transactInput.TransactItems[0].Update == nil || client.transactInput.TransactItems[1].Update == nil || client.transactInput.TransactItems[2].Put == nil {
		t.Fatalf("finalize transaction=%#v", client.transactInput)
	}
}

func TestDynamoRepositoryRecoversFinalReceiptAfterIndeterminateFinalize(t *testing.T) {
	reservation := validDeploymentReservation()
	receipt := validDeploymentRecord(reservation)
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

func TestDynamoRepositoryActivatesWorkerLeaseWithEpochFenceAndTokenDigestOnly(t *testing.T) {
	session := validDeploymentReservation().WorkerSession
	session.State = "bound"
	session.ExpectedInstanceID = "i-0123456789abcdef0"
	activated := session
	activated.State = "active"
	activated.LeaseEpoch = 1
	activated.LeaseExpiresAt = "2026-07-14T12:05:00.000Z"
	activated.TokenSHA256 = strings.Repeat("c", 64)
	client := &fakeDynamo{}
	client.getItem = func(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if client.transactInput != nil && input.TableName != nil && *input.TableName == "worker-sessions" {
			return &dynamodb.GetItemOutput{Item: workerSessionItemForTest(activated)}, nil
		}
		return &dynamodb.GetItemOutput{}, nil
	}
	repository := mustDynamoRepository(t, client)
	stored, err := repository.ActivateWorkerSession(t.Context(), WorkerSessionClaim{
		Session: session, TokenSHA256: activated.TokenSHA256, Now: "2026-07-14T12:00:00.000Z", LeaseExpiresAt: activated.LeaseExpiresAt,
	})
	if err != nil || stored.LeaseEpoch != 1 || stored.TokenSHA256 != activated.TokenSHA256 {
		t.Fatalf("ActivateWorkerSession()=(%#v,%v)", stored, err)
	}
	update := client.transactInput.TransactItems[0].Update
	if update == nil || update.ConditionExpression == nil || !strings.Contains(*update.ConditionExpression, "lease_epoch = :lease_epoch") || update.UpdateExpression == nil || strings.Contains(*update.UpdateExpression, "access_token") {
		t.Fatalf("unsafe worker activation update=%#v", update)
	}
	for name, value := range update.ExpressionAttributeValues {
		if strings.Contains(fmt.Sprint(value), "access_token") || (name != ":token_sha256" && strings.Contains(fmt.Sprint(value), activated.TokenSHA256)) {
			t.Fatalf("token escaped digest slot name=%s value=%v", name, value)
		}
	}
}

func TestWorkerSessionParserRejectsExpandedSecretField(t *testing.T) {
	item := workerSessionItem(validDeploymentReservation().WorkerSession)
	item["access_token"] = &dynamodbtypes.AttributeValueMemberS{Value: "must-not-leak"}
	if _, err := workerSessionFromItem(item); Code(err) != "worker_session_store_invalid" {
		t.Fatalf("expanded worker item error=%v code=%s", err, Code(err))
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
	repository, err := NewDynamoRepository(DynamoConfig{Client: client, ReceiptsTable: "receipts", CountersTable: "counters", IssuedQuotesTable: "quotes", DeploymentReservationsTable: "deployments", DeploymentDestroyTable: "deployment-destroys", ServiceBackupsTable: "service-backups", ServiceRestoresTable: "service-restores", ApprovalUsesTable: "approval-uses", WorkerSessionsTable: "worker-sessions"})
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
	session := WorkerSession{BootstrapSessionID: "bootstrap-" + strings.Repeat("f", 32), ConnectionID: "connection-0001", DeploymentID: "deployment-0001", RequestSHA256: strings.Repeat("d", 64), WorkerImageDigest: "sha256:" + strings.Repeat("a", 64), ArtifactManifestDigest: "sha256:" + strings.Repeat("b", 64), BootstrapEndpoint: "https://abcdefghij.execute-api.us-east-1.amazonaws.com/prod/v2/worker-sessions", ExpectedAMIID: "ami-0123456789abcdef0", ExpectedInstanceType: "m7i.xlarge", ExpectedArchitecture: "x86_64", ExpectedVPCID: "vpc-0123456789abcdef0", ExpectedSubnetID: "subnet-0123456789abcdef0", ExpectedAvailabilityZone: "us-east-1a", ExpectedSecurityGroupID: "sg-0123456789abcdef0", State: "issued", ExpiresAt: "2026-07-14T12:10:00.123Z"}
	return DeploymentReservation{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", CommandID: "command-deployment-0001", RequestSHA256: strings.Repeat("d", 64), ExpectedGeneration: 1, NodeCounter: 8, ApprovalID: "approval-0001", ChallengeID: "challenge-0001", SignerKeyID: "device-key-1", QuoteID: "quote-00000000000000000000000000000000", ClientToken: "dtx-" + strings.Repeat("e", 60), BootstrapSessionID: session.BootstrapSessionID, WorkerSession: session, SpecJSON: []byte(`{"deployment_id":"deployment-0001"}`), State: "reserved"}
}

func validDeploymentRecord(reservation DeploymentReservation) Record {
	result := fmt.Sprintf(`{"status":"deployment_created","receipt":{"schema":"dirextalk.aws.command-receipt/v2","disposition":"committed","connection_id":%q,"expected_generation":%d,"node_counter":%d,"command_id":%q,"request_sha256":%q,"action":"deployment.create"},"deployment":{"schema":"dirextalk.aws.deployment-receipt/v1","connection_id":%q,"deployment_id":%q,"request_sha256":%q,"resource_status":"provisioning","instance_id":"i-0123456789abcdef0","volume_ids":["vol-0123456789abcdef0"],"network_interface_ids":["eni-0123456789abcdef0"]}}`, reservation.ConnectionID, reservation.ExpectedGeneration, reservation.NodeCounter, reservation.CommandID, reservation.RequestSHA256, reservation.ConnectionID, reservation.DeploymentID, reservation.RequestSHA256)
	return Record{ConnectionID: reservation.ConnectionID, CommandID: reservation.CommandID, RequestSHA256: reservation.RequestSHA256, ExpectedGeneration: reservation.ExpectedGeneration, NodeCounter: reservation.NodeCounter, Action: "deployment.create", ResultJSON: []byte(result)}
}

func workerSessionItemForTest(session WorkerSession) map[string]dynamodbtypes.AttributeValue {
	item := workerSessionItem(session)
	if session.ExpectedInstanceID != "" {
		item["expected_instance_id"] = &dynamodbtypes.AttributeValueMemberS{Value: session.ExpectedInstanceID}
	}
	if session.LeaseExpiresAt != "" {
		item["lease_expires_at"] = &dynamodbtypes.AttributeValueMemberS{Value: session.LeaseExpiresAt}
	}
	if session.TokenSHA256 != "" {
		item["token_sha256"] = &dynamodbtypes.AttributeValueMemberS{Value: session.TokenSHA256}
	}
	item["lease_epoch"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(session.LeaseEpoch, 10)}
	item["last_sequence"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(session.LastSequence, 10)}
	return item
}
