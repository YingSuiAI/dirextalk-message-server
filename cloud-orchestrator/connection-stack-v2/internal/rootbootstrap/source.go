package rootbootstrap

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
)

var (
	ErrInvalidArtifactSource   = errors.New("root bootstrap artifact source is invalid")
	ErrArtifactContentMismatch = errors.New("root bootstrap artifact content does not match its release descriptor")
)

// ReadSeekCloser is the minimal release-file capability required to hash a
// local artifact and rewind that same opened file for immutable publication.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}

// ArtifactSource opens a server-owned release artifact. The resolver verifies
// the descriptor itself, so a source implementation cannot assert a digest on
// the resolver's behalf.
type ArtifactSource interface {
	Open(context.Context, string, artifactpublish.ArtifactDescriptor) (ReadSeekCloser, error)
}

// FileSource accepts only the exact local regular file supplied by the fixed
// release configuration. It rejects symlinks and rechecks the opened handle
// to reduce path-swap races before Resolver hashes the bytes.
type FileSource struct{}

func (FileSource) Open(ctx context.Context, path string, descriptor artifactpublish.ArtifactDescriptor) (ReadSeekCloser, error) {
	if ctx == nil || ctx.Err() != nil || !validLocalPath(path) || descriptor.Validate() != nil {
		return nil, ErrInvalidArtifactSource
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 || before.Size() != descriptor.SizeBytes {
		return nil, ErrInvalidArtifactSource
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, ErrInvalidArtifactSource
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || after.Size() != descriptor.SizeBytes || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, ErrInvalidArtifactSource
	}
	return file, nil
}
