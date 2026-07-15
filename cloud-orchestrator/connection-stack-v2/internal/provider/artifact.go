package provider

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/api"
	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsv4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type artifactS3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}
type artifactPresigner interface {
	PresignPutObject(context.Context, *s3.PutObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error)
	PresignGetObject(context.Context, *s3.GetObjectInput, ...func(*s3.PresignOptions)) (*awsv4.PresignedHTTPRequest, error)
}
type AWSArtifactProvider struct {
	client           artifactS3API
	presign          artifactPresigner
	bucket, kmsKeyID string
	now              func() time.Time
}

func NewAWSArtifactProvider(config aws.Config, bucket, kmsKeyID string) (*AWSArtifactProvider, error) {
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(kmsKeyID) == "" {
		return nil, errors.New("invalid artifact provider")
	}
	client := s3.NewFromConfig(config)
	return &AWSArtifactProvider{client: client, presign: s3.NewPresignClient(client), bucket: bucket, kmsKeyID: kmsKeyID, now: time.Now}, nil
}
func (p *AWSArtifactProvider) PresignPut(ctx context.Context, key string, b contract.ArtifactBinding, ttl time.Duration) (string, time.Time, error) {
	out, err := p.presign.PresignPutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(p.bucket), Key: aws.String(key), ContentLength: aws.Int64(b.SizeBytes), ContentType: aws.String(b.MediaType), ChecksumSHA256: aws.String(b.ChecksumBase64()), ServerSideEncryption: s3types.ServerSideEncryptionAwsKms}, func(o *s3.PresignOptions) { o.Expires = ttl })
	if err != nil {
		return "", time.Time{}, errors.New("artifact provider unavailable")
	}
	return out.URL, p.now().UTC().Add(ttl), nil
}
func (p *AWSArtifactProvider) Head(ctx context.Context, key, version string) (api.ArtifactObjectObservation, error) {
	out, err := p.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(p.bucket), Key: aws.String(key), VersionId: aws.String(version), ChecksumMode: s3types.ChecksumModeEnabled})
	if err != nil {
		return api.ArtifactObjectObservation{}, errors.New("artifact provider unavailable")
	}
	if out.ServerSideEncryption != s3types.ServerSideEncryptionAwsKms || aws.ToString(out.SSEKMSKeyId) != p.kmsKeyID {
		return api.ArtifactObjectObservation{}, errors.New("artifact provider unavailable")
	}
	return api.ArtifactObjectObservation{VersionID: aws.ToString(out.VersionId), ContentType: aws.ToString(out.ContentType), ChecksumSHA256: aws.ToString(out.ChecksumSHA256), ServerSideEncryption: string(out.ServerSideEncryption), KMSKeyID: aws.ToString(out.SSEKMSKeyId), SizeBytes: aws.ToInt64(out.ContentLength)}, nil
}
func (p *AWSArtifactProvider) PresignGet(ctx context.Context, key, version string, ttl time.Duration) (string, time.Time, error) {
	out, err := p.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(p.bucket), Key: aws.String(key), VersionId: aws.String(version)}, func(o *s3.PresignOptions) { o.Expires = ttl })
	if err != nil {
		return "", time.Time{}, errors.New("artifact provider unavailable")
	}
	return out.URL, p.now().UTC().Add(ttl), nil
}
