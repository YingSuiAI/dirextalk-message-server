package provider

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	secretstypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
)

func TestAWSServiceSecretKeySealerBindsPurposeAndContext(t *testing.T) {
	client := &fakeSecretKMS{}
	sealer, _ := NewAWSServiceSecretKeySealer(client, "alias/dirextalk-service-secrets")
	aad := []byte("canonical-context")
	value := bytes.Repeat([]byte{7}, 32)
	sealed, err := sealer.SealServiceSecretToken(t.Context(), value, aad)
	if err != nil || sealed == "" {
		t.Fatal(err)
	}
	opened, err := sealer.UnsealServiceSecretToken(t.Context(), sealed, aad)
	if err != nil || !bytes.Equal(opened, value) {
		t.Fatalf("open=%x err=%v", opened, err)
	}
	if client.encryptContext["dirextalk:purpose"] != "upload-token" || client.encryptContext["dirextalk:context_sha256"] == "" || client.decryptContext["dirextalk:context_sha256"] != client.encryptContext["dirextalk:context_sha256"] {
		t.Fatalf("contexts=%v %v", client.encryptContext, client.decryptContext)
	}
	client.err = errors.New("AccessDenied")
	if _, err = sealer.SealServiceSecretKey(t.Context(), value, aad); err == nil {
		t.Fatal("AccessDenied accepted")
	}
}

func TestAWSServiceSecretProviderUsesDeterministicNameAndEnvelopeVersion(t *testing.T) {
	client := &fakeSecretsManager{}
	provider, _ := NewAWSServiceSecretProvider(client, "connection-0001", "alias/dirextalk-service-secrets")
	binding := api.ServiceSecretProviderBinding{ConnectionID: "connection-0001", DeploymentID: "deployment-0001", SecretRef: "secret_ref:model-token-001", EnvelopeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	value := []byte("test-secret-value")
	version, err := provider.PutServiceSecret(t.Context(), binding, value)
	if err != nil || version != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("version=%s err=%v", version, err)
	}
	expected := "dirextalk/connection-0001/deployment-0001/" + "f0a39a97ff601127bcfcfaf6b3e43d5c579da673"
	if aws.ToString(client.create.Name) != expected || aws.ToString(client.put.SecretId) != expected || aws.ToString(client.put.ClientRequestToken) != version || !bytes.Equal(client.put.SecretBinary, value) || aws.ToString(client.create.KmsKeyId) != "alias/dirextalk-service-secrets" {
		t.Fatalf("create=%#v put=%#v", client.create, client.put)
	}
	tags := map[string]string{}
	for _, tag := range client.create.Tags {
		tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	if tags["dirextalk:managed-by"] != "dirextalk-cloud-orchestrator" || tags["dirextalk:connection-id"] != binding.ConnectionID || tags["dirextalk:deployment-id"] != binding.DeploymentID || len(tags) != 3 {
		t.Fatalf("unsafe service-secret tags=%v", tags)
	}
	client.exists = true
	if replay, err := provider.PutServiceSecret(t.Context(), binding, value); err != nil || replay != version {
		t.Fatalf("replay=%s err=%v", replay, err)
	}
	client.binary = append([]byte(nil), value...)
	read, err := provider.GetServiceSecret(t.Context(), api.ServiceSecretReadBinding{ConnectionID: binding.ConnectionID, DeploymentID: binding.DeploymentID, SecretRef: binding.SecretRef, ProviderVersion: version})
	if err != nil || !bytes.Equal(read, value) || aws.ToString(client.get.VersionId) != version {
		t.Fatalf("read=%q err=%v", read, err)
	}
	client.err = errors.New("AccessDenied")
	if _, err = provider.PutServiceSecret(t.Context(), binding, value); err == nil {
		t.Fatal("AccessDenied accepted")
	}
	if _, err = provider.GetServiceSecret(t.Context(), api.ServiceSecretReadBinding{ConnectionID: binding.ConnectionID, DeploymentID: binding.DeploymentID, SecretRef: binding.SecretRef, ProviderVersion: version}); err == nil {
		t.Fatal("GetSecretValue AccessDenied accepted")
	}
}

type fakeSecretKMS struct {
	encryptContext, decryptContext map[string]string
	blob, plaintext                []byte
	err                            error
}

func (f *fakeSecretKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.encryptContext = in.EncryptionContext
	f.plaintext = append([]byte(nil), in.Plaintext...)
	f.blob = append([]byte("sealed:"), in.Plaintext...)
	return &kms.EncryptOutput{CiphertextBlob: f.blob}, nil
}
func (f *fakeSecretKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.decryptContext = in.EncryptionContext
	if !bytes.Equal(in.CiphertextBlob, f.blob) {
		return nil, errors.New("ciphertext mismatch")
	}
	return &kms.DecryptOutput{Plaintext: append([]byte(nil), f.plaintext...)}, nil
}

type fakeSecretsManager struct {
	create *secretsmanager.CreateSecretInput
	put    *secretsmanager.PutSecretValueInput
	exists bool
	err    error
	get    *secretsmanager.GetSecretValueInput
	binary []byte
}

func (f *fakeSecretsManager) GetSecretValue(_ context.Context, in *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	f.get = in
	if f.err != nil {
		return nil, f.err
	}
	return &secretsmanager.GetSecretValueOutput{SecretBinary: append([]byte(nil), f.binary...), VersionId: in.VersionId}, nil
}

func (f *fakeSecretsManager) CreateSecret(_ context.Context, in *secretsmanager.CreateSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	f.create = in
	if f.err != nil {
		return nil, f.err
	}
	if f.exists {
		return nil, &secretstypes.ResourceExistsException{Message: aws.String("exists")}
	}
	f.exists = true
	return &secretsmanager.CreateSecretOutput{}, nil
}
func (f *fakeSecretsManager) PutSecretValue(_ context.Context, in *secretsmanager.PutSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error) {
	f.put = in
	if f.err != nil {
		return nil, f.err
	}
	return &secretsmanager.PutSecretValueOutput{VersionId: in.ClientRequestToken}, nil
}
