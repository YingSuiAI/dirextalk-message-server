package store

import (
	"context"
	"encoding/hex"
	"errors"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

const maxSafeInteger = int64(9007199254740991)

type DynamoAPI interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type DynamoConfig struct {
	Client                      DynamoAPI
	ReceiptsTable               string
	CountersTable               string
	IssuedQuotesTable           string
	DeploymentReservationsTable string
	DeploymentDestroyTable      string
	ServiceBackupsTable         string
	ServiceRestoresTable        string
	ApprovalUsesTable           string
	WorkerSessionsTable         string
}

type DynamoRepository struct {
	client                      DynamoAPI
	receiptsTable               string
	countersTable               string
	issuedQuotesTable           string
	deploymentReservationsTable string
	deploymentDestroyTable      string
	serviceBackupsTable         string
	serviceRestoresTable        string
	approvalUsesTable           string
	workerSessionsTable         string
}

func NewDynamoRepository(config DynamoConfig) (*DynamoRepository, error) {
	if config.Client == nil || !validTableName(config.ReceiptsTable) || !validTableName(config.CountersTable) || !validTableName(config.IssuedQuotesTable) || !validTableName(config.DeploymentReservationsTable) || !validTableName(config.DeploymentDestroyTable) || !validTableName(config.ServiceBackupsTable) || !validTableName(config.ServiceRestoresTable) || !validTableName(config.ApprovalUsesTable) || !validTableName(config.WorkerSessionsTable) || !uniqueStrings(config.ReceiptsTable, config.CountersTable, config.IssuedQuotesTable, config.DeploymentReservationsTable, config.DeploymentDestroyTable, config.ServiceBackupsTable, config.ServiceRestoresTable, config.ApprovalUsesTable, config.WorkerSessionsTable) {
		return nil, errors.New("invalid DynamoDB receipt store configuration")
	}
	return &DynamoRepository{client: config.Client, receiptsTable: config.ReceiptsTable, countersTable: config.CountersTable, issuedQuotesTable: config.IssuedQuotesTable, deploymentReservationsTable: config.DeploymentReservationsTable, deploymentDestroyTable: config.DeploymentDestroyTable, serviceBackupsTable: config.ServiceBackupsTable, serviceRestoresTable: config.ServiceRestoresTable, approvalUsesTable: config.ApprovalUsesTable, workerSessionsTable: config.WorkerSessionsTable}, nil
}

func uniqueStrings(values ...string) bool {
	seen := map[string]struct{}{}
	for _, value := range values {
		if _, ok := seen[value]; ok {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func (s *DynamoRepository) Lookup(ctx context.Context, connectionID, commandID string) (Record, bool, error) {
	if s == nil || s.client == nil || !contract.ValidConnectionID(connectionID) || !contract.ValidID(commandID) {
		return Record{}, false, NewError("connection_stack_store_unavailable")
	}
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.receiptsTable,
		ConsistentRead: boolPtr(true),
		Key: map[string]dynamodbtypes.AttributeValue{
			"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID},
			"command_id":    &dynamodbtypes.AttributeValueMemberS{Value: commandID},
		},
	})
	if err != nil {
		return Record{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return Record{}, false, nil
	}
	record, err := recordFromItem(output.Item)
	if err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

func (s *DynamoRepository) Commit(ctx context.Context, record Record, quote *IssuedQuote) (Record, bool, error) {
	if s == nil || s.client == nil || validateRecord(record) != nil || (quote != nil && validateIssuedQuote(record, *quote) != nil) {
		return Record{}, false, NewError("receipt_store_invalid")
	}
	items := []dynamodbtypes.TransactWriteItem{
		{Update: &dynamodbtypes.Update{
			TableName: &s.countersTable,
			Key: map[string]dynamodbtypes.AttributeValue{
				"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: record.ConnectionID},
			},
			UpdateExpression:    stringPtr("SET last_node_counter = :node_counter"),
			ConditionExpression: stringPtr("attribute_not_exists(last_node_counter) OR last_node_counter < :node_counter"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":node_counter": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(record.NodeCounter, 10)},
			},
		}},
		{Put: &dynamodbtypes.Put{
			TableName:           &s.receiptsTable,
			ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(command_id)"),
			Item: map[string]dynamodbtypes.AttributeValue{
				"connection_id":       &dynamodbtypes.AttributeValueMemberS{Value: record.ConnectionID},
				"command_id":          &dynamodbtypes.AttributeValueMemberS{Value: record.CommandID},
				"request_sha256":      &dynamodbtypes.AttributeValueMemberS{Value: record.RequestSHA256},
				"expected_generation": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(record.ExpectedGeneration, 10)},
				"node_counter":        &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(record.NodeCounter, 10)},
				"action":              &dynamodbtypes.AttributeValueMemberS{Value: record.Action},
				"result_json":         &dynamodbtypes.AttributeValueMemberS{Value: string(record.ResultJSON)},
			},
		}},
	}
	if quote != nil {
		items = append(items, dynamodbtypes.TransactWriteItem{Put: &dynamodbtypes.Put{
			TableName:           &s.issuedQuotesTable,
			ConditionExpression: stringPtr("attribute_not_exists(connection_id) AND attribute_not_exists(quote_id)"),
			Item: map[string]dynamodbtypes.AttributeValue{
				"connection_id":  &dynamodbtypes.AttributeValueMemberS{Value: quote.ConnectionID},
				"quote_id":       &dynamodbtypes.AttributeValueMemberS{Value: quote.QuoteID},
				"plan_digest":    &dynamodbtypes.AttributeValueMemberS{Value: quote.PlanDigest},
				"command_id":     &dynamodbtypes.AttributeValueMemberS{Value: quote.CommandID},
				"request_sha256": &dynamodbtypes.AttributeValueMemberS{Value: quote.RequestSHA256},
				"valid_until":    &dynamodbtypes.AttributeValueMemberS{Value: quote.ValidUntil},
				"quote_json":     &dynamodbtypes.AttributeValueMemberS{Value: string(quote.QuoteJSON)},
			},
		}})
	}
	_, err := s.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	if err == nil {
		return cloneRecord(record), true, nil
	}
	var canceled *dynamodbtypes.TransactionCanceledException
	if !errors.As(err, &canceled) {
		return Record{}, false, NewError("connection_stack_store_unavailable")
	}
	existing, found, lookupErr := s.Lookup(ctx, record.ConnectionID, record.CommandID)
	if lookupErr != nil {
		return Record{}, false, lookupErr
	}
	if found {
		if !existing.SameIdentity(record) {
			return Record{}, false, NewError("command_id_conflict")
		}
		return existing, false, nil
	}
	lastCounter, found, counterErr := s.lookupCounter(ctx, record.ConnectionID)
	if counterErr != nil {
		return Record{}, false, counterErr
	}
	if found && record.NodeCounter <= lastCounter {
		return Record{}, false, NewError("stale_node_counter")
	}
	return Record{}, false, NewError("receipt_race")
}

func (s *DynamoRepository) lookupCounter(ctx context.Context, connectionID string) (int64, bool, error) {
	output, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      &s.countersTable,
		ConsistentRead: boolPtr(true),
		Key: map[string]dynamodbtypes.AttributeValue{
			"connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID},
		},
	})
	if err != nil {
		return 0, false, NewError("connection_stack_store_unavailable")
	}
	if len(output.Item) == 0 {
		return 0, false, nil
	}
	value, err := numberAttribute(output.Item, "last_node_counter", true)
	if err != nil {
		return 0, false, err
	}
	return value, true, nil
}

func recordFromItem(item map[string]dynamodbtypes.AttributeValue) (Record, error) {
	connectionID, err := stringAttribute(item, "connection_id")
	if err != nil || !contract.ValidConnectionID(connectionID) {
		return Record{}, NewError("receipt_store_invalid")
	}
	commandID, err := stringAttribute(item, "command_id")
	if err != nil || !contract.ValidID(commandID) {
		return Record{}, NewError("receipt_store_invalid")
	}
	requestSHA, err := stringAttribute(item, "request_sha256")
	if err != nil || !validSHA256(requestSHA) {
		return Record{}, NewError("receipt_store_invalid")
	}
	generation, err := numberAttribute(item, "expected_generation", false)
	if err != nil || generation < 1 {
		return Record{}, NewError("receipt_store_invalid")
	}
	counter, err := numberAttribute(item, "node_counter", true)
	if err != nil {
		return Record{}, NewError("receipt_store_invalid")
	}
	action, err := stringAttribute(item, "action")
	if err != nil || !validReceiptAction(action) {
		return Record{}, NewError("receipt_store_invalid")
	}
	resultJSON, err := stringAttribute(item, "result_json")
	if err != nil || len(resultJSON) == 0 || len(resultJSON) > contract.MaxCommandBytes {
		return Record{}, NewError("receipt_store_invalid")
	}
	return Record{ConnectionID: connectionID, CommandID: commandID, RequestSHA256: requestSHA, ExpectedGeneration: generation, NodeCounter: counter, Action: action, ResultJSON: []byte(resultJSON)}, nil
}

func validateRecord(record Record) error {
	if !contract.ValidConnectionID(record.ConnectionID) || !contract.ValidID(record.CommandID) || !validSHA256(record.RequestSHA256) || record.ExpectedGeneration < 1 || record.ExpectedGeneration > maxSafeInteger || record.NodeCounter < 0 || record.NodeCounter > maxSafeInteger || !validReceiptAction(record.Action) || len(record.ResultJSON) == 0 || len(record.ResultJSON) > contract.MaxCommandBytes {
		return NewError("receipt_store_invalid")
	}
	return nil
}

func validReceiptAction(action string) bool {
	switch action {
	case contract.ActionRegistrationVerify, contract.ActionQuoteRequest, contract.ActionDeploymentCreate,
		contract.ActionDeploymentObserve, contract.ActionWorkerTaskIssue, contract.ActionWorkerTaskObserve,
		contract.ActionWorkerRecipeTaskIssue, contract.ActionWorkerRecipeTaskObserve, contract.ActionDeploymentDestroy, contract.ActionServiceBackup, contract.ActionServiceRestore:
		return true
	default:
		return false
	}
}

func validateIssuedQuote(record Record, quote IssuedQuote) error {
	if record.Action != contract.ActionQuoteRequest || quote.ConnectionID != record.ConnectionID || quote.CommandID != record.CommandID || quote.RequestSHA256 != record.RequestSHA256 || !contract.ValidID(quote.QuoteID) || len(quote.PlanDigest) != 71 || quote.PlanDigest[:7] != "sha256:" || !validSHA256(quote.PlanDigest[7:]) || len(quote.ValidUntil) == 0 || len(quote.QuoteJSON) == 0 || len(quote.QuoteJSON) > contract.MaxCommandBytes {
		return NewError("receipt_store_invalid")
	}
	return nil
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}

func stringAttribute(item map[string]dynamodbtypes.AttributeValue, name string) (string, error) {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberS)
	if !ok || value.Value == "" {
		return "", NewError("receipt_store_invalid")
	}
	return value.Value, nil
}

func numberAttribute(item map[string]dynamodbtypes.AttributeValue, name string, allowZero bool) (int64, error) {
	value, ok := item[name].(*dynamodbtypes.AttributeValueMemberN)
	if !ok {
		return 0, NewError("receipt_store_invalid")
	}
	parsed, err := strconv.ParseInt(value.Value, 10, 64)
	if err != nil || parsed > maxSafeInteger || parsed < 0 || (!allowZero && parsed == 0) || strconv.FormatInt(parsed, 10) != value.Value {
		return 0, NewError("receipt_store_invalid")
	}
	return parsed, nil
}

func cloneRecord(record Record) Record {
	record.ResultJSON = append([]byte(nil), record.ResultJSON...)
	return record
}

func validTableName(value string) bool {
	return value != "" && len(value) <= 255 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\t")
}

func boolPtr(value bool) *bool       { return &value }
func stringPtr(value string) *string { return &value }
