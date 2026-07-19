package workerimage

import (
	"context"
	"io"
	"os"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
)

// ReleaseStore is the narrow, durable boundary used by the explicit Worker
// release-publish mode. Lookup is mandatory: when a provider commits an
// object but loses its response, a retry recovers the same version-pinned
// binding instead of uploading another mutable candidate.
//
// Implementations must key Lookup and Publish by the complete immutable
// descriptor. They must never resolve a binding through an unversioned S3
// object, tag, prefix, URL, or arbitrary bucket/key pair.
type ReleaseStore interface {
	LookupWorkerArchive(context.Context, artifactpublish.ArtifactDescriptor) (artifactpublish.Binding, bool, error)
	PublishWorkerArchive(context.Context, artifactpublish.ArtifactDescriptor, io.Reader) (artifactpublish.Binding, error)
	VerifyWorkerArchive(context.Context, artifactpublish.Binding) (artifactpublish.Binding, error)
}

// ReleasePublisher publishes the archive once before an AMI build. Unlike
// Builder's default temporary upload path, the returned binding is retained
// and can be embedded in the resulting ImageManifest.
type ReleasePublisher struct{ store ReleaseStore }

func NewReleasePublisher(store ReleaseStore) (*ReleasePublisher, error) {
	if store == nil {
		return nil, ErrInvalidConfig
	}
	return &ReleasePublisher{store: store}, nil
}

func (publisher *ReleasePublisher) Publish(ctx context.Context, artifact ValidatedArtifact) (artifactpublish.Binding, error) {
	if publisher == nil || publisher.store == nil || ctx == nil {
		return artifactpublish.Binding{}, ErrInvalidConfig
	}
	descriptor, err := releaseDescriptor(artifact)
	if err != nil {
		return artifactpublish.Binding{}, err
	}
	// ValidatedArtifact is an exported value, so re-validate the on-disk
	// archive at the release boundary before accepting its claimed digest.
	observed, err := ValidateArchive(artifact.Path)
	if err != nil || !sameReleaseArtifact(observed, artifact) {
		return artifactpublish.Binding{}, ErrInvalidArtifact
	}
	artifact = observed
	if binding, found, lookupErr := publisher.store.LookupWorkerArchive(ctx, descriptor); lookupErr != nil {
		return artifactpublish.Binding{}, lookupErr
	} else if found {
		return publisher.verify(ctx, descriptor, binding)
	}

	file, err := os.Open(artifact.Path)
	if err != nil {
		return artifactpublish.Binding{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != artifact.ArchiveSize {
		return artifactpublish.Binding{}, ErrInvalidArtifact
	}
	binding, publishErr := publisher.store.PublishWorkerArchive(ctx, descriptor, file)
	if publishErr == nil {
		return publisher.verify(ctx, descriptor, binding)
	}

	// The provider may have committed the version but lost its response. A
	// descriptor lookup is the only recovery route; do not issue a second put.
	recovered, found, lookupErr := publisher.store.LookupWorkerArchive(ctx, descriptor)
	if lookupErr == nil && found {
		return publisher.verify(ctx, descriptor, recovered)
	}
	return artifactpublish.Binding{}, publishErr
}

func releaseDescriptor(artifact ValidatedArtifact) (artifactpublish.ArtifactDescriptor, error) {
	descriptor := artifactpublish.ArtifactDescriptor{
		Kind:      artifactpublish.KindWorkerArchive,
		Version:   artifact.Catalog.ArtifactVersion,
		SHA256:    artifact.ArchiveSHA256,
		SizeBytes: artifact.ArchiveSize,
	}
	if descriptor.Validate() != nil {
		return artifactpublish.ArtifactDescriptor{}, ErrInvalidConfig
	}
	return descriptor, nil
}

func sameReleaseArtifact(left, right ValidatedArtifact) bool {
	return left.Catalog.ArtifactVersion == right.Catalog.ArtifactVersion &&
		left.ArchiveSHA256 == right.ArchiveSHA256 && left.ArchiveSize == right.ArchiveSize &&
		left.CatalogDigest == right.CatalogDigest && left.Catalog.WorkerResourceManifestDigest == right.Catalog.WorkerResourceManifestDigest
}

func (publisher *ReleasePublisher) verify(ctx context.Context, descriptor artifactpublish.ArtifactDescriptor, binding artifactpublish.Binding) (artifactpublish.Binding, error) {
	if validateReleaseBinding(binding, descriptor.Version, descriptor.SHA256) != nil || binding.SizeBytes != descriptor.SizeBytes {
		return artifactpublish.Binding{}, ErrBuildFailed
	}
	verified, err := publisher.store.VerifyWorkerArchive(ctx, binding)
	if err != nil || verified != binding || validateReleaseBinding(verified, descriptor.Version, descriptor.SHA256) != nil || verified.SizeBytes != descriptor.SizeBytes {
		return artifactpublish.Binding{}, ErrBuildFailed
	}
	return verified, nil
}
