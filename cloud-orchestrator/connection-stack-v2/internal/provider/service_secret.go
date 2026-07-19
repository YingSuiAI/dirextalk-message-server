package provider

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	secretstypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

type KMSServiceSecretAPI interface {
	Encrypt(context.Context, *kms.EncryptInput, ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
}
type SecretsManagerServiceSecretAPI interface {
	CreateSecret(context.Context, *secretsmanager.CreateSecretInput, ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	PutSecretValue(context.Context, *secretsmanager.PutSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error)
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}

type AWSServiceSecretKeySealer struct {
	client KMSServiceSecretAPI
	keyID  string
}

func NewAWSServiceSecretKeySealer(client KMSServiceSecretAPI, keyID string) (*AWSServiceSecretKeySealer, error) {
	if client == nil || strings.TrimSpace(keyID) == "" {
		return nil, api.NewError("service_secret_key_unavailable", 503)
	}
	return &AWSServiceSecretKeySealer{client: client, keyID: keyID}, nil
}
func (s *AWSServiceSecretKeySealer) SealServiceSecretKey(ctx context.Context, value, aad []byte) (string, error) {
	return s.seal(ctx, "x25519-private", value, aad)
}
func (s *AWSServiceSecretKeySealer) UnsealServiceSecretKey(ctx context.Context, value string, aad []byte) ([]byte, error) {
	return s.unseal(ctx, "x25519-private", value, aad)
}
func (s *AWSServiceSecretKeySealer) SealServiceSecretToken(ctx context.Context, value, aad []byte) (string, error) {
	return s.seal(ctx, "upload-token", value, aad)
}
func (s *AWSServiceSecretKeySealer) UnsealServiceSecretToken(ctx context.Context, value string, aad []byte) ([]byte, error) {
	return s.unseal(ctx, "upload-token", value, aad)
}
func (s *AWSServiceSecretKeySealer) seal(ctx context.Context, purpose string, value, aad []byte) (string, error) {
	if len(value) != 32 || len(aad) == 0 {
		return "", api.NewError("service_secret_key_unavailable", 503)
	}
	contextValues := serviceSecretEncryptionContext(purpose, aad)
	out, err := s.client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(s.keyID), Plaintext: value, EncryptionContext: contextValues})
	if err != nil || len(out.CiphertextBlob) == 0 {
		return "", api.NewError("service_secret_key_unavailable", 503)
	}
	return base64.RawURLEncoding.EncodeToString(out.CiphertextBlob), nil
}
func (s *AWSServiceSecretKeySealer) unseal(ctx context.Context, purpose, value string, aad []byte) ([]byte, error) {
	blob, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || base64.RawURLEncoding.EncodeToString(blob) != value || len(blob) == 0 || len(aad) == 0 {
		return nil, api.NewError("service_secret_key_unavailable", 503)
	}
	out, err := s.client.Decrypt(ctx, &kms.DecryptInput{KeyId: aws.String(s.keyID), CiphertextBlob: blob, EncryptionContext: serviceSecretEncryptionContext(purpose, aad)})
	if err != nil || len(out.Plaintext) != 32 {
		return nil, api.NewError("service_secret_key_unavailable", 503)
	}
	return append([]byte(nil), out.Plaintext...), nil
}
func serviceSecretEncryptionContext(purpose string, aad []byte) map[string]string {
	sum := sha256.Sum256(aad)
	return map[string]string{"dirextalk:purpose": purpose, "dirextalk:context_sha256": hex.EncodeToString(sum[:])}
}

type AWSServiceSecretProvider struct {
	client              SecretsManagerServiceSecretAPI
	connectionID, keyID string
}

func NewAWSServiceSecretProvider(client SecretsManagerServiceSecretAPI, connectionID, keyID string) (*AWSServiceSecretProvider, error) {
	if client == nil || !contract.ValidConnectionID(connectionID) || strings.TrimSpace(keyID) == "" {
		return nil, api.NewError("service_secret_provider_unavailable", 503)
	}
	return &AWSServiceSecretProvider{client: client, connectionID: connectionID, keyID: keyID}, nil
}
func (p *AWSServiceSecretProvider) PutServiceSecret(ctx context.Context, b api.ServiceSecretProviderBinding, value []byte) (string, error) {
	if b.ConnectionID != p.connectionID || !contract.ValidID(b.DeploymentID) || !strings.HasPrefix(b.SecretRef, "secret_ref:") || len(value) == 0 || len(value) > contract.MaxServiceSecretPlaintext || !strings.HasPrefix(b.EnvelopeDigest, "sha256:") {
		return "", api.NewError("service_secret_provider_unavailable", 503)
	}
	name := serviceSecretName(b.ConnectionID, b.DeploymentID, b.SecretRef)
	_, err := p.client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String(name), KmsKeyId: aws.String(p.keyID), Description: aws.String("Dirextalk deployment-scoped service secret"), Tags: []secretstypes.Tag{{Key: aws.String("dirextalk:managed-by"), Value: aws.String("dirextalk-cloud-orchestrator")}, {Key: aws.String("dirextalk:connection-id"), Value: aws.String(b.ConnectionID)}, {Key: aws.String("dirextalk:deployment-id"), Value: aws.String(b.DeploymentID)}}})
	var exists *secretstypes.ResourceExistsException
	if err != nil && !errors.As(err, &exists) {
		return "", api.NewError("service_secret_provider_unavailable", 503)
	}
	token := strings.TrimPrefix(b.EnvelopeDigest, "sha256:")
	if len(token) != 64 {
		return "", api.NewError("service_secret_provider_unavailable", 503)
	}
	out, err := p.client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: aws.String(name), ClientRequestToken: aws.String(token), SecretBinary: append([]byte(nil), value...)})
	if err != nil || aws.ToString(out.VersionId) == "" {
		return "", api.NewError("service_secret_provider_unavailable", 503)
	}
	return aws.ToString(out.VersionId), nil
}
func (p *AWSServiceSecretProvider) GetServiceSecret(ctx context.Context, b api.ServiceSecretReadBinding) ([]byte, error) {
	if b.ConnectionID != p.connectionID || !contract.ValidID(b.DeploymentID) || !strings.HasPrefix(b.SecretRef, "secret_ref:") || b.ProviderVersion == "" || len(b.ProviderVersion) > 256 {
		return nil, api.NewError("service_secret_provider_unavailable", 503)
	}
	name := serviceSecretName(b.ConnectionID, b.DeploymentID, b.SecretRef)
	out, err := p.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(name), VersionId: aws.String(b.ProviderVersion)})
	if err != nil || len(out.SecretBinary) == 0 || len(out.SecretBinary) > contract.MaxServiceSecretPlaintext || out.SecretString != nil {
		return nil, api.NewError("service_secret_provider_unavailable", 503)
	}
	return append([]byte(nil), out.SecretBinary...), nil
}
func serviceSecretName(connectionID, deploymentID, secretRef string) string {
	sum := sha256.Sum256([]byte(secretRef))
	return "dirextalk/" + connectionID + "/" + deploymentID + "/" + hex.EncodeToString(sum[:20])
}
