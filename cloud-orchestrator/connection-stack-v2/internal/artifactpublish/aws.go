package artifactpublish

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// AWSStore is an adapter, not a general-purpose S3 client.  It owns the
// fixed bucket and KMS policy, then derives every AWS request from an
// immutable descriptor or a version-pinned binding.
type AWSStore struct {
	client s3API
	policy Policy
}

func NewAWSStore(config aws.Config, policy Policy) (*AWSStore, error) {
	return newAWSStore(s3.NewFromConfig(config), policy)
}

func newAWSStore(client s3API, policy Policy) (*AWSStore, error) {
	if client == nil || policy.Validate() != nil {
		return nil, ErrInvalidPolicy
	}
	return &AWSStore{client: client, policy: policy}, nil
}

func (store *AWSStore) PutImmutable(ctx context.Context, descriptor ArtifactDescriptor, body io.Reader) (string, error) {
	if store == nil || store.client == nil || store.policy.Validate() != nil || descriptor.Validate() != nil || body == nil {
		return "", ErrInvalidDescriptor
	}
	output, err := store.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(store.policy.Bucket),
		Key:                  aws.String(objectKey(descriptor)),
		Body:                 body,
		ContentLength:        aws.Int64(descriptor.SizeBytes),
		ChecksumSHA256:       aws.String(checksumBase64(descriptor.SHA256)),
		ContentType:          aws.String(contentType(descriptor.Kind)),
		ServerSideEncryption: s3types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(store.policy.KMSKeyID),
	})
	if err != nil || output == nil || !validVersionID(aws.ToString(output.VersionId)) {
		return "", ErrProvider
	}
	return aws.ToString(output.VersionId), nil
}

func (store *AWSStore) HeadImmutable(ctx context.Context, binding Binding) (ObjectMetadata, error) {
	if store == nil || store.client == nil || store.policy.Validate() != nil || binding.ValidateFor(store.policy) != nil {
		return ObjectMetadata{}, ErrInvalidBinding
	}
	output, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(binding.Bucket),
		Key:          aws.String(binding.Key),
		VersionId:    aws.String(binding.VersionID),
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil || output == nil || output.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms {
		return ObjectMetadata{}, ErrProvider
	}
	return ObjectMetadata{
		VersionID:   aws.ToString(output.VersionId),
		SHA256:      aws.ToString(output.ChecksumSHA256),
		SizeBytes:   aws.ToInt64(output.ContentLength),
		ContentType: aws.ToString(output.ContentType),
		KMSKeyID:    aws.ToString(output.SSEKMSKeyId),
	}, nil
}
