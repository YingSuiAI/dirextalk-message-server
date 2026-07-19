package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestDynamoServiceSecretStoreCreateReplayAndCAS(t *testing.T) {
	client := &fakeServiceSecretDynamo{}
	store, _ := NewDynamoServiceSecretStore(client, "service-secret-sessions")
	session := validServiceSecretSessionFixture()
	stored, created, err := store.CreateServiceSecret(t.Context(), session)
	if err != nil || !created || !stored.SameBinding(session) {
		t.Fatal(created, err)
	}
	if _, ok := client.item["ttl_epoch_seconds"]; !ok {
		t.Fatal("unfinished bootstrap session has no short TTL")
	}
	if replay, created, err := store.CreateServiceSecret(t.Context(), session); err != nil || created || !replay.SameBinding(session) {
		t.Fatal(created, err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	claimed, advanced, err := store.ClaimServiceSecretEnvelope(t.Context(), session.SessionID, digest)
	if err != nil || !advanced || claimed.State != ServiceSecretProcessing {
		t.Fatal(advanced, err)
	}
	if _, advanced, err = store.ClaimServiceSecretEnvelope(t.Context(), session.SessionID, digest); err != nil || advanced {
		t.Fatal(advanced, err)
	}
	if _, _, err = store.ClaimServiceSecretEnvelope(t.Context(), session.SessionID, "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"); err == nil || Code(err) != "service_secret_envelope_conflict" {
		t.Fatalf("different digest=%v", err)
	}
	uploaded, err := store.FinalizeServiceSecretUpload(t.Context(), session.SessionID, digest, "opaque-version")
	if err != nil || uploaded.State != ServiceSecretUploaded {
		t.Fatal(err)
	}
	completed, err := store.CompleteServiceSecret(t.Context(), session.SessionID, digest)
	if err != nil || completed.State != ServiceSecretCompleted {
		t.Fatal(err)
	}
	if _, ok := client.item["ttl_epoch_seconds"]; ok {
		t.Fatal("completed service secret binding retained bootstrap TTL")
	}
	if _, ok := client.item["sealed_private_key"]; ok {
		t.Fatal("completed service secret retained sealed private key")
	}
	if _, ok := client.item["sealed_upload_token"]; ok {
		t.Fatal("completed service secret retained sealed upload token")
	}
	if _, ok := client.item["token_sha256"]; !ok {
		t.Fatal("completed service secret lost the one-way token hash required for exact upload receipt replay")
	}
	materialized, found, err := store.LookupCompletedServiceSecret(t.Context(), session.ConnectionID, session.DeploymentID, session.RecipeDigest, session.ArtifactDigest, session.SlotID, session.SecretRef)
	if err != nil || !found || materialized.SessionID != session.SessionID {
		t.Fatalf("lookup completed found=%v err=%v", found, err)
	}
	if err = store.DeleteCompletedServiceSecretBindings(t.Context(), session.ConnectionID, session.DeploymentID, []string{session.SecretRef}); err != nil || len(client.item) != 0 {
		t.Fatalf("delete completed binding item=%v err=%v", client.item, err)
	}
	if client.deleteInput == nil || client.deleteInput.ConditionExpression == nil || !strings.Contains(*client.deleteInput.ConditionExpression, "connection_id=:connection_id") || !strings.Contains(*client.deleteInput.ConditionExpression, "deployment_id=:deployment_id") || !strings.Contains(*client.deleteInput.ConditionExpression, "secret_ref=:secret_ref") {
		t.Fatalf("service-secret binding delete is not exact: %#v", client.deleteInput)
	}
	if err = store.DeleteCompletedServiceSecretBindings(t.Context(), session.ConnectionID, session.DeploymentID, []string{session.SecretRef}); err != nil {
		t.Fatalf("lost-response replay delete=%v", err)
	}
	client.denied = true
	if _, _, err = store.LookupServiceSecret(t.Context(), session.SessionID); err == nil || Code(err) != "connection_stack_store_unavailable" {
		t.Fatalf("AccessDenied=%v", err)
	}
}

func validServiceSecretSessionFixture() ServiceSecretSession {
	contextValue := contract.ServiceSecretContextV1{SchemaVersion: contract.ServiceSecretContextSchema, SessionID: "secret-session-0001", ConnectionID: "connection-0001", DeploymentID: "deployment-0001", TaskID: "task-secret-0001", ExecutionID: "execution-0001", ManifestDigest: "sha256:3333333333333333333333333333333333333333333333333333333333333333", RecipeDigest: "sha256:1111111111111111111111111111111111111111111111111111111111111111", ArtifactDigest: "sha256:2222222222222222222222222222222222222222222222222222222222222222", SlotID: "model_token", SecretRef: "secret_ref:model-token-001", Purpose: "model inference", Delivery: "environment", ExpiresAt: "2026-07-15T12:10:00.000Z"}
	digest, _ := contextValue.Digest()
	return ServiceSecretSession{SessionID: contextValue.SessionID, ConnectionID: contextValue.ConnectionID, DeploymentID: contextValue.DeploymentID, TaskID: contextValue.TaskID, ExecutionID: contextValue.ExecutionID, ManifestDigest: contextValue.ManifestDigest, RecipeDigest: contextValue.RecipeDigest, ArtifactDigest: contextValue.ArtifactDigest, SlotID: contextValue.SlotID, SecretRef: contextValue.SecretRef, Purpose: contextValue.Purpose, Delivery: contextValue.Delivery, ContextDigest: digest, ExpiresAt: contextValue.ExpiresAt, TokenSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", SealedPrivateKey: "kms-private", SealedUploadToken: "kms-token", State: ServiceSecretPending}
}

type fakeServiceSecretDynamo struct {
	item        map[string]dynamodbtypes.AttributeValue
	deleteInput *dynamodb.DeleteItemInput
	denied      bool
}

func (f *fakeServiceSecretDynamo) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if f.denied {
		return nil, errors.New("AccessDenied")
	}
	return &dynamodb.GetItemOutput{Item: cloneSecretItem(f.item)}, nil
}
func (f *fakeServiceSecretDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if f.denied {
		return nil, errors.New("AccessDenied")
	}
	key := in.ExpressionAttributeValues[":key"].(*dynamodbtypes.AttributeValueMemberS).Value
	attribute := "materialization_key"
	if in.IndexName != nil && *in.IndexName == serviceSecretDeploymentIndex {
		attribute = "deployment_key"
	}
	if value, ok := f.item[attribute].(*dynamodbtypes.AttributeValueMemberS); ok && value.Value == key {
		return &dynamodb.QueryOutput{Items: []map[string]dynamodbtypes.AttributeValue{cloneSecretItem(f.item)}}, nil
	}
	return &dynamodb.QueryOutput{}, nil
}
func (f *fakeServiceSecretDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.denied {
		return nil, errors.New("AccessDenied")
	}
	if len(f.item) != 0 {
		return nil, errors.New("ConditionalCheckFailed")
	}
	f.item = cloneSecretItem(in.Item)
	return &dynamodb.PutItemOutput{}, nil
}
func (f *fakeServiceSecretDynamo) UpdateItem(_ context.Context, u *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if f.denied {
		return nil, errors.New("AccessDenied")
	}
	values := u.ExpressionAttributeValues
	if strings.Contains(*u.UpdateExpression, "envelope_digest") {
		value := values[":processing"].(*dynamodbtypes.AttributeValueMemberS)
		f.item["state"] = value
		f.item["envelope_digest"] = values[":digest"]
		return &dynamodb.UpdateItemOutput{}, nil
	}
	if strings.Contains(*u.UpdateExpression, "provider_version") {
		value := values[":uploaded"].(*dynamodbtypes.AttributeValueMemberS)
		f.item["state"] = value
		f.item["provider_version"] = values[":version"]
		return &dynamodb.UpdateItemOutput{}, nil
	}
	if value, ok := values[":completed"].(*dynamodbtypes.AttributeValueMemberS); ok {
		f.item["state"] = value
		f.item["materialization_key"] = values[":materialization_key"]
		f.item["deployment_key"] = values[":deployment_key"]
		delete(f.item, "ttl_epoch_seconds")
		delete(f.item, "sealed_private_key")
		delete(f.item, "sealed_upload_token")
		return &dynamodb.UpdateItemOutput{}, nil
	}
	return nil, errors.New("unsupported")
}
func (f *fakeServiceSecretDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if f.denied {
		return nil, errors.New("AccessDenied")
	}
	f.deleteInput = in
	delete(f.item, "session_id")
	f.item = nil
	return &dynamodb.DeleteItemOutput{}, nil
}
func cloneSecretItem(item map[string]dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
	if item == nil {
		return nil
	}
	out := make(map[string]dynamodbtypes.AttributeValue, len(item))
	for k, v := range item {
		out[k] = v
	}
	return out
}
