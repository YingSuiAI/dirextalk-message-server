package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

type trustedArtifactTestStore struct {
	requests []cloudmodule.RegisterTrustedRecipeArtifactRequest
	err      error
}

func TestTrustedArtifactExecutionBindingIncludesPinnedImageSource(t *testing.T) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	source := cloudcontracts.OCIImageSourceReferenceV1("ghcr.io/dirextalk/service@" + digest)
	artifact := cloudcontracts.CompiledRecipeArtifactV1{ArtifactDigest: digest, ImageSource: source, SizeBytes: 1, Architecture: cloudcontracts.ArchitectureAMD64,
		VolumeSlots: []cloudcontracts.RecipeVolumeSlotRequirementV1{{SlotID: "state", ReadOnly: false}}, DataSlots: []cloudcontracts.RecipeDataSlotRequirementV1{{SlotID: "knowledge", ReadOnly: true}}}
	descriptor := cloudcontracts.OCIServiceBundleV1{ArtifactDigest: digest, ImageDigest: digest, ImageSource: source, ImageSizeBytes: 1, Architecture: cloudcontracts.ArchitectureAMD64,
		VolumeTargets: []cloudcontracts.OCIServiceStorageTargetV1{{SlotID: "state", ContainerTarget: "/var/lib/service", ReadOnly: false}}, DataTargets: []cloudcontracts.OCIServiceStorageTargetV1{{SlotID: "knowledge", ContainerTarget: "/opt/knowledge", ReadOnly: true}}}
	bundle := recipeexec.Bundle{ArtifactDigest: digest, ActionIDs: []string{}}
	if !trustedArtifactExecutionBindingExact(artifact, bundle, descriptor) {
		t.Fatal("matching pinned source was not bound")
	}
	descriptor.ImageSource = cloudcontracts.OCIImageSourceReferenceV1("quay.io/dirextalk/service@" + digest)
	if trustedArtifactExecutionBindingExact(artifact, bundle, descriptor) {
		t.Fatal("catalog source drift was accepted")
	}
	descriptor.ImageSource = source
	descriptor.DataTargets[0].ReadOnly = false
	if trustedArtifactExecutionBindingExact(artifact, bundle, descriptor) {
		t.Fatal("catalog data target read-only drift was accepted")
	}
}

func (store *trustedArtifactTestStore) RegisterTrustedCloudRecipeArtifact(_ context.Context, request cloudmodule.RegisterTrustedRecipeArtifactRequest) (cloudmodule.RegisterTrustedRecipeArtifactResult, error) {
	store.requests = append(store.requests, request)
	return cloudmodule.RegisterTrustedRecipeArtifactResult{}, store.err
}

func TestTrustedArtifactRegistrarGatesStartupOnLoadAndRegistration(t *testing.T) {
	now := time.Date(2026, time.July, 16, 2, 0, 0, 0, time.UTC)
	store := &trustedArtifactTestStore{}
	registrar := NewTrustedArtifactRegistrar(store, "/controller/controller-trusted-artifact-catalog.json", func() time.Time { return now })
	artifact := cloudcontracts.CompiledRecipeArtifactV1{RecipeID: "recipe-trusted-0001"}
	registrar.load = func(string) (trustedArtifactRegistration, error) {
		return trustedArtifactRegistration{artifact: artifact}, nil
	}
	if err := registrar.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != 1 || store.requests[0].Artifact.RecipeID != artifact.RecipeID || store.requests[0].RegisteredAt != now.UnixMilli() {
		t.Fatalf("registration requests=%#v", store.requests)
	}

	loadFailure := NewTrustedArtifactRegistrar(store, registrar.catalogFile, func() time.Time { return now })
	loadFailure.load = func(string) (trustedArtifactRegistration, error) {
		return trustedArtifactRegistration{}, errors.New("invalid bundle")
	}
	if err := loadFailure.Register(context.Background()); !errors.Is(err, ErrTrustedArtifactCatalogInvalid) || len(store.requests) != 1 {
		t.Fatalf("load failure err=%v requests=%d", err, len(store.requests))
	}
	store.err = cloudmodule.ErrRecipeArtifactConflict
	if err := registrar.Register(context.Background()); !errors.Is(err, cloudmodule.ErrRecipeArtifactConflict) {
		t.Fatalf("registration failure err=%v", err)
	}
}
