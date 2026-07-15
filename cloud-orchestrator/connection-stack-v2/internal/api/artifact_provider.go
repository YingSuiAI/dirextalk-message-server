package api

import (
	"context"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

type ArtifactObjectObservation struct {
	VersionID, ContentType, ChecksumSHA256, ServerSideEncryption, KMSKeyID string
	SizeBytes                                                              int64
}
type ArtifactProvider interface {
	PresignPut(ctx context.Context, key string, binding contract.ArtifactBinding, ttl time.Duration) (url string, expiresAt time.Time, err error)
	Head(ctx context.Context, key, versionID string) (ArtifactObjectObservation, error)
	PresignGet(ctx context.Context, key, versionID string, ttl time.Duration) (url string, expiresAt time.Time, err error)
}
