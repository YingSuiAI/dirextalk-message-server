package workerimage

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
)

func TestReleasePublisherPinsWorkerArchiveAndRecoversLostPublishResponse(t *testing.T) {
	artifact := artifactFixture(t)
	for _, responseLoss := range []bool{false, true} {
		t.Run(map[bool]string{false: "publish", true: "response_loss"}[responseLoss], func(t *testing.T) {
			store := &fakeReleaseStore{policy: releasePolicy(), responseLoss: responseLoss}
			publisher, err := NewReleasePublisher(store)
			if err != nil {
				t.Fatal(err)
			}
			binding, err := publisher.Publish(context.Background(), artifact)
			if err != nil {
				t.Fatalf("Publish(): %v", err)
			}
			if binding.Kind != artifactpublish.KindWorkerArchive || binding.Version != artifact.Catalog.ArtifactVersion || binding.VersionID == "" || binding.SHA256 != artifact.ArchiveSHA256 || binding.SizeBytes != artifact.ArchiveSize || store.publishCalls != 1 || store.verifyCalls == 0 {
				t.Fatalf("unexpected immutable binding=%#v store=%#v", binding, store)
			}
			again, err := publisher.Publish(context.Background(), artifact)
			if err != nil || again != binding || store.publishCalls != 1 {
				t.Fatalf("idempotent Publish() binding=%#v err=%v puts=%d", again, err, store.publishCalls)
			}
		})
	}
}

func TestReleasePublisherRejectsUnapprovedVersionBeforeStoreMutation(t *testing.T) {
	for _, version := range []string{"latest", "v1.0.3", "1.0.3", "v1.2.3"} {
		t.Run(version, func(t *testing.T) {
			artifact := artifactFixture(t)
			artifact.Catalog.ArtifactVersion = version
			store := &fakeReleaseStore{policy: releasePolicy()}
			publisher, _ := NewReleasePublisher(store)
			if _, err := publisher.Publish(context.Background(), artifact); !errors.Is(err, ErrInvalidConfig) || store.publishCalls != 0 || store.lookupCalls != 0 {
				t.Fatalf("version %q error=%v publish=%d lookup=%d", version, err, store.publishCalls, store.lookupCalls)
			}
		})
	}
}

func TestReleasePublisherRejectsArtifactFactMismatchBeforeStoreMutation(t *testing.T) {
	artifact := artifactFixture(t)
	artifact.ArchiveSHA256 = "sha256:" + strings.Repeat("0", 64)
	store := &fakeReleaseStore{policy: releasePolicy()}
	publisher, _ := NewReleasePublisher(store)
	if _, err := publisher.Publish(context.Background(), artifact); !errors.Is(err, ErrInvalidArtifact) || store.publishCalls != 0 || store.lookupCalls != 0 {
		t.Fatalf("tampered artifact error=%v publish=%d lookup=%d", err, store.publishCalls, store.lookupCalls)
	}
}

type fakeReleaseStore struct {
	policy       artifactpublish.Policy
	binding      artifactpublish.Binding
	descriptor   artifactpublish.ArtifactDescriptor
	published    bool
	responseLoss bool
	lookupCalls  int
	publishCalls int
	verifyCalls  int
}

func (store *fakeReleaseStore) LookupWorkerArchive(_ context.Context, descriptor artifactpublish.ArtifactDescriptor) (artifactpublish.Binding, bool, error) {
	store.lookupCalls++
	if !store.published || descriptor != store.descriptor {
		return artifactpublish.Binding{}, false, nil
	}
	return store.binding, true, nil
}

func (store *fakeReleaseStore) PublishWorkerArchive(_ context.Context, descriptor artifactpublish.ArtifactDescriptor, body io.Reader) (artifactpublish.Binding, error) {
	store.publishCalls++
	raw, err := io.ReadAll(body)
	if err != nil || int64(len(raw)) != descriptor.SizeBytes || namedBytes(raw) != descriptor.SHA256 {
		return artifactpublish.Binding{}, ErrBuildFailed
	}
	binding, err := artifactpublish.NewBinding(store.policy, descriptor, "3Lg5kqtJlcpXroDTDmJ+.yKk6aYxEtR2")
	if err != nil {
		return artifactpublish.Binding{}, err
	}
	store.binding, store.descriptor, store.published = binding, descriptor, true
	if store.responseLoss {
		return artifactpublish.Binding{}, errors.New("publish response lost")
	}
	return binding, nil
}

func (store *fakeReleaseStore) VerifyWorkerArchive(_ context.Context, binding artifactpublish.Binding) (artifactpublish.Binding, error) {
	store.verifyCalls++
	if !store.published || binding != store.binding {
		return artifactpublish.Binding{}, ErrBuildFailed
	}
	return binding, nil
}

func releasePolicy() artifactpublish.Policy {
	return artifactpublish.Policy{Bucket: "dirextalk-release-artifacts", KMSKeyID: "alias/dirextalk-worker-release"}
}
