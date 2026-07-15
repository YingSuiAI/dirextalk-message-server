package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

var serviceSecretTokenHashPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var serviceSecretLookupTextPattern = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,256}$`)

const serviceSecretMaterializationIndex = "materialization-key-index"
const serviceSecretDeploymentIndex = "deployment-key-index"

type ServiceSecretDynamoAPI interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}
type DynamoServiceSecretStore struct {
	client ServiceSecretDynamoAPI
	table  string
}

func NewDynamoServiceSecretStore(client ServiceSecretDynamoAPI, table string) (*DynamoServiceSecretStore, error) {
	if client == nil || !validTableName(table) {
		return nil, NewError("service_secret_store_invalid")
	}
	return &DynamoServiceSecretStore{client: client, table: table}, nil
}

func (s *DynamoServiceSecretStore) LookupServiceSecret(ctx context.Context, sessionID string) (ServiceSecretSession, bool, error) {
	if s == nil || s.client == nil || !contract.ValidID(sessionID) {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{TableName: &s.table, ConsistentRead: boolPtr(true), Key: map[string]dynamodbtypes.AttributeValue{"session_id": &dynamodbtypes.AttributeValueMemberS{Value: sessionID}}})
	if err != nil {
		return ServiceSecretSession{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(out.Item) == 0 {
		return ServiceSecretSession{}, false, nil
	}
	value, err := serviceSecretFromItem(out.Item)
	if err != nil || value.SessionID != sessionID || !validServiceSecretSession(value) {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	return value, true, nil
}

func (s *DynamoServiceSecretStore) LookupCompletedServiceSecret(ctx context.Context, connectionID, deploymentID, recipeDigest, artifactDigest, slotID, secretRef string) (ServiceSecretSession, bool, error) {
	if !contract.ValidConnectionID(connectionID) || !contract.ValidID(deploymentID) || !approvedDigestPattern.MatchString(recipeDigest) || !approvedDigestPattern.MatchString(artifactDigest) || !serviceSecretLookupTextPattern.MatchString(slotID) || !approvedSecretRefPattern.MatchString(secretRef) {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	key := serviceSecretMaterializationKey(connectionID, deploymentID, recipeDigest, artifactDigest, slotID, secretRef)
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{TableName: &s.table, IndexName: stringPtr(serviceSecretMaterializationIndex), KeyConditionExpression: stringPtr("materialization_key=:key"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":key": &dynamodbtypes.AttributeValueMemberS{Value: key}}, Limit: int32Ptr(2)})
	if err != nil {
		return ServiceSecretSession{}, false, NewError("connection_stack_store_unavailable")
	}
	if len(out.Items) == 0 {
		return ServiceSecretSession{}, false, nil
	}
	if len(out.Items) != 1 {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	sessionID, err := stringAttribute(out.Items[0], "session_id")
	if err != nil {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	value, found, err := s.LookupServiceSecret(ctx, sessionID)
	if err != nil || !found || value.State != ServiceSecretCompleted || value.ConnectionID != connectionID || value.DeploymentID != deploymentID || value.RecipeDigest != recipeDigest || value.ArtifactDigest != artifactDigest || value.SlotID != slotID || value.SecretRef != secretRef {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	return value, true, nil
}

func (s *DynamoServiceSecretStore) DeleteCompletedServiceSecretBindings(ctx context.Context, connectionID, deploymentID string, secretRefs []string) error {
	if !contract.ValidConnectionID(connectionID) || !contract.ValidID(deploymentID) || len(secretRefs) == 0 || len(secretRefs) > 32 {
		return NewError("service_secret_store_invalid")
	}
	wanted := make(map[string]struct{}, len(secretRefs))
	for _, ref := range secretRefs {
		if !approvedSecretRefPattern.MatchString(ref) {
			return NewError("service_secret_store_invalid")
		}
		if _, exists := wanted[ref]; exists {
			return NewError("service_secret_store_invalid")
		}
		wanted[ref] = struct{}{}
	}
	key := serviceSecretDeploymentKey(connectionID, deploymentID)
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{TableName: &s.table, IndexName: stringPtr(serviceSecretDeploymentIndex), KeyConditionExpression: stringPtr("deployment_key=:key"), ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":key": &dynamodbtypes.AttributeValueMemberS{Value: key}}, Limit: int32Ptr(33)})
	if err != nil {
		return NewError("connection_stack_store_unavailable")
	}
	if len(out.LastEvaluatedKey) != 0 || len(out.Items) > 32 {
		return NewError("service_secret_store_invalid")
	}
	for _, item := range out.Items {
		sessionID, itemErr := stringAttribute(item, "session_id")
		if itemErr != nil {
			return NewError("service_secret_store_invalid")
		}
		session, found, lookupErr := s.LookupServiceSecret(ctx, sessionID)
		if lookupErr != nil {
			return lookupErr
		}
		if !found || session.State != ServiceSecretCompleted || session.ConnectionID != connectionID || session.DeploymentID != deploymentID {
			return NewError("service_secret_store_invalid")
		}
		if _, selected := wanted[session.SecretRef]; !selected {
			continue
		}
		_, deleteErr := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: &s.table, Key: serviceSecretKey(session.SessionID), ConditionExpression: stringPtr("#state=:completed AND connection_id=:connection_id AND deployment_id=:deployment_id AND secret_ref=:secret_ref"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":completed": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretCompleted}, ":connection_id": &dynamodbtypes.AttributeValueMemberS{Value: connectionID}, ":deployment_id": &dynamodbtypes.AttributeValueMemberS{Value: deploymentID}, ":secret_ref": &dynamodbtypes.AttributeValueMemberS{Value: session.SecretRef}}})
		if deleteErr != nil {
			return NewError("connection_stack_store_unavailable")
		}
	}
	return nil
}

func (s *DynamoServiceSecretStore) CreateServiceSecret(ctx context.Context, value ServiceSecretSession) (ServiceSecretSession, bool, error) {
	if !validServiceSecretSession(value) || value.State != ServiceSecretPending || value.EnvelopeDigest != "" || value.ProviderVersion != "" {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{TableName: &s.table, Item: serviceSecretItem(value), ConditionExpression: stringPtr("attribute_not_exists(session_id)")})
	if err == nil {
		return value, true, nil
	}
	existing, found, lookupErr := s.LookupServiceSecret(ctx, value.SessionID)
	if lookupErr != nil {
		return ServiceSecretSession{}, false, lookupErr
	}
	if found {
		if !existing.SameBinding(value) {
			return ServiceSecretSession{}, false, NewError("service_secret_session_conflict")
		}
		return existing, false, nil
	}
	return ServiceSecretSession{}, false, NewError("connection_stack_store_unavailable")
}

func (s *DynamoServiceSecretStore) ClaimServiceSecretEnvelope(ctx context.Context, id, digest string) (ServiceSecretSession, bool, error) {
	if !contract.ValidID(id) || !approvedDigestPattern.MatchString(digest) {
		return ServiceSecretSession{}, false, NewError("service_secret_store_invalid")
	}
	if current, found, err := s.LookupServiceSecret(ctx, id); err != nil {
		return ServiceSecretSession{}, false, err
	} else if !found {
		return ServiceSecretSession{}, false, NewError("service_secret_session_not_found")
	} else if current.EnvelopeDigest != "" {
		if current.EnvelopeDigest != digest {
			return ServiceSecretSession{}, false, NewError("service_secret_envelope_conflict")
		}
		return current, false, nil
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: &s.table, Key: serviceSecretKey(id), UpdateExpression: stringPtr("SET #state=:processing,envelope_digest=:digest"), ConditionExpression: stringPtr("#state=:pending AND attribute_not_exists(envelope_digest)"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":pending": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretPending}, ":processing": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretProcessing}, ":digest": &dynamodbtypes.AttributeValueMemberS{Value: digest}}})
	if err == nil {
		value, _, lookupErr := s.LookupServiceSecret(ctx, id)
		return value, true, lookupErr
	}
	current, found, lookupErr := s.LookupServiceSecret(ctx, id)
	if lookupErr != nil {
		return ServiceSecretSession{}, false, lookupErr
	}
	if found && current.EnvelopeDigest == digest {
		return current, false, nil
	}
	if found && current.EnvelopeDigest != "" {
		return ServiceSecretSession{}, false, NewError("service_secret_envelope_conflict")
	}
	return ServiceSecretSession{}, false, NewError("connection_stack_store_unavailable")
}

func (s *DynamoServiceSecretStore) FinalizeServiceSecretUpload(ctx context.Context, id, digest, version string) (ServiceSecretSession, error) {
	if !contract.ValidID(id) || !approvedDigestPattern.MatchString(digest) || version == "" || len(version) > 256 {
		return ServiceSecretSession{}, NewError("service_secret_store_invalid")
	}
	current, found, err := s.LookupServiceSecret(ctx, id)
	if err != nil {
		return ServiceSecretSession{}, err
	}
	if !found {
		return ServiceSecretSession{}, NewError("service_secret_session_not_found")
	}
	if current.EnvelopeDigest != digest {
		return ServiceSecretSession{}, NewError("service_secret_envelope_conflict")
	}
	if current.State == ServiceSecretUploaded || current.State == ServiceSecretCompleted {
		if current.ProviderVersion != version {
			return ServiceSecretSession{}, NewError("service_secret_provider_version_conflict")
		}
		return current, nil
	}
	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: &s.table, Key: serviceSecretKey(id), UpdateExpression: stringPtr("SET #state=:uploaded,provider_version=:version"), ConditionExpression: stringPtr("#state=:processing AND envelope_digest=:digest"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":processing": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretProcessing}, ":uploaded": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretUploaded}, ":digest": &dynamodbtypes.AttributeValueMemberS{Value: digest}, ":version": &dynamodbtypes.AttributeValueMemberS{Value: version}}})
	if err != nil {
		return ServiceSecretSession{}, NewError("connection_stack_store_unavailable")
	}
	value, _, err := s.LookupServiceSecret(ctx, id)
	return value, err
}

func (s *DynamoServiceSecretStore) CompleteServiceSecret(ctx context.Context, id, digest string) (ServiceSecretSession, error) {
	if !contract.ValidID(id) || !approvedDigestPattern.MatchString(digest) {
		return ServiceSecretSession{}, NewError("service_secret_store_invalid")
	}
	current, found, err := s.LookupServiceSecret(ctx, id)
	if err != nil {
		return ServiceSecretSession{}, err
	}
	if !found {
		return ServiceSecretSession{}, NewError("service_secret_session_not_found")
	}
	if current.EnvelopeDigest != digest {
		return ServiceSecretSession{}, NewError("service_secret_envelope_conflict")
	}
	if current.State == ServiceSecretCompleted {
		return current, nil
	}
	if current.State != ServiceSecretUploaded {
		return ServiceSecretSession{}, NewError("service_secret_not_uploaded")
	}
	materializationKey := serviceSecretMaterializationKey(current.ConnectionID, current.DeploymentID, current.RecipeDigest, current.ArtifactDigest, current.SlotID, current.SecretRef)
	deploymentKey := serviceSecretDeploymentKey(current.ConnectionID, current.DeploymentID)
	_, err = s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: &s.table, Key: serviceSecretKey(id), UpdateExpression: stringPtr("SET #state=:completed,materialization_key=:materialization_key,deployment_key=:deployment_key REMOVE ttl_epoch_seconds,sealed_private_key,sealed_upload_token"), ConditionExpression: stringPtr("#state=:uploaded AND envelope_digest=:digest"), ExpressionAttributeNames: map[string]string{"#state": "state"}, ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{":uploaded": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretUploaded}, ":completed": &dynamodbtypes.AttributeValueMemberS{Value: ServiceSecretCompleted}, ":digest": &dynamodbtypes.AttributeValueMemberS{Value: digest}, ":materialization_key": &dynamodbtypes.AttributeValueMemberS{Value: materializationKey}, ":deployment_key": &dynamodbtypes.AttributeValueMemberS{Value: deploymentKey}}})
	if err != nil {
		return ServiceSecretSession{}, NewError("connection_stack_store_unavailable")
	}
	value, _, err := s.LookupServiceSecret(ctx, id)
	return value, err
}

func serviceSecretKey(id string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"session_id": &dynamodbtypes.AttributeValueMemberS{Value: id}}
}
func serviceSecretMaterializationKey(values ...string) string {
	hash := sha256.New()
	for _, value := range values {
		hash.Write([]byte(value))
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func serviceSecretDeploymentKey(connectionID, deploymentID string) string {
	return serviceSecretMaterializationKey(connectionID, deploymentID)
}
func int32Ptr(value int32) *int32 { return &value }
func serviceSecretItem(s ServiceSecretSession) map[string]dynamodbtypes.AttributeValue {
	values := map[string]string{"session_id": s.SessionID, "connection_id": s.ConnectionID, "deployment_id": s.DeploymentID, "task_id": s.TaskID, "execution_id": s.ExecutionID, "manifest_digest": s.ManifestDigest, "recipe_digest": s.RecipeDigest, "artifact_digest": s.ArtifactDigest, "slot_id": s.SlotID, "secret_ref": s.SecretRef, "purpose": s.Purpose, "delivery": s.Delivery, "context_digest": s.ContextDigest, "expires_at": s.ExpiresAt, "token_sha256": s.TokenSHA256, "sealed_private_key": s.SealedPrivateKey, "sealed_upload_token": s.SealedUploadToken, "state": s.State}
	if s.EnvelopeDigest != "" {
		values["envelope_digest"] = s.EnvelopeDigest
	}
	if s.ProviderVersion != "" {
		values["provider_version"] = s.ProviderVersion
	}
	item := map[string]dynamodbtypes.AttributeValue{}
	for k, v := range values {
		item[k] = &dynamodbtypes.AttributeValueMemberS{Value: v}
	}
	if expires, err := time.Parse("2006-01-02T15:04:05.000Z", s.ExpiresAt); err == nil {
		item["ttl_epoch_seconds"] = &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(expires.Unix(), 10)}
	}
	return item
}
func serviceSecretFromItem(item map[string]dynamodbtypes.AttributeValue) (ServiceSecretSession, error) {
	var s ServiceSecretSession
	fields := map[string]*string{"session_id": &s.SessionID, "connection_id": &s.ConnectionID, "deployment_id": &s.DeploymentID, "task_id": &s.TaskID, "execution_id": &s.ExecutionID, "manifest_digest": &s.ManifestDigest, "recipe_digest": &s.RecipeDigest, "artifact_digest": &s.ArtifactDigest, "slot_id": &s.SlotID, "secret_ref": &s.SecretRef, "purpose": &s.Purpose, "delivery": &s.Delivery, "context_digest": &s.ContextDigest, "expires_at": &s.ExpiresAt, "token_sha256": &s.TokenSHA256, "state": &s.State}
	for k, target := range fields {
		value, err := stringAttribute(item, k)
		if err != nil {
			return s, err
		}
		*target = value
	}
	if v, ok := item["envelope_digest"].(*dynamodbtypes.AttributeValueMemberS); ok {
		s.EnvelopeDigest = v.Value
	}
	if v, ok := item["provider_version"].(*dynamodbtypes.AttributeValueMemberS); ok {
		s.ProviderVersion = v.Value
	}
	if v, ok := item["sealed_private_key"].(*dynamodbtypes.AttributeValueMemberS); ok {
		s.SealedPrivateKey = v.Value
	}
	if v, ok := item["sealed_upload_token"].(*dynamodbtypes.AttributeValueMemberS); ok {
		s.SealedUploadToken = v.Value
	}
	return s, nil
}
func validServiceSecretSession(s ServiceSecretSession) bool {
	contextValue := contract.ServiceSecretContextV1{SchemaVersion: contract.ServiceSecretContextSchema, SessionID: s.SessionID, ConnectionID: s.ConnectionID, DeploymentID: s.DeploymentID, TaskID: s.TaskID, ExecutionID: s.ExecutionID, ManifestDigest: s.ManifestDigest, RecipeDigest: s.RecipeDigest, ArtifactDigest: s.ArtifactDigest, SlotID: s.SlotID, SecretRef: s.SecretRef, Purpose: s.Purpose, Delivery: s.Delivery, ExpiresAt: s.ExpiresAt}
	digest, err := contextValue.Digest()
	if err != nil || digest != s.ContextDigest || !serviceSecretTokenHashPattern.MatchString(s.TokenSHA256) {
		return false
	}
	transientValid := s.SealedPrivateKey != "" && len(s.SealedPrivateKey) <= 16384 && s.SealedUploadToken != "" && len(s.SealedUploadToken) <= 16384
	switch s.State {
	case ServiceSecretPending:
		return transientValid && s.EnvelopeDigest == "" && s.ProviderVersion == ""
	case ServiceSecretProcessing:
		return transientValid && approvedDigestPattern.MatchString(s.EnvelopeDigest) && s.ProviderVersion == ""
	case ServiceSecretUploaded:
		return transientValid && approvedDigestPattern.MatchString(s.EnvelopeDigest) && s.ProviderVersion != "" && len(s.ProviderVersion) <= 256
	case ServiceSecretCompleted:
		return s.SealedPrivateKey == "" && s.SealedUploadToken == "" && approvedDigestPattern.MatchString(s.EnvelopeDigest) && s.ProviderVersion != "" && len(s.ProviderVersion) <= 256
	}
	return false
}
