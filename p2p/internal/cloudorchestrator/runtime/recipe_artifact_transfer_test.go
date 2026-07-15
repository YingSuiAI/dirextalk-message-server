package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRecipeArtifactTransferPersistsVersionThenReplaysExactCompleteCommand(t *testing.T) {
	now := time.Date(2026, time.July, 16, 7, 0, 0, 0, time.UTC)
	claim := recipeInstallTestClaim(t)
	archive := TrustedRecipeArtifactArchive{
		Path: "/controller/stage-s.tar", ArchiveSHA256: strings.Repeat("9", 64), SizeBytes: 4096,
		ControllerCatalogDigest: "sha256:" + strings.Repeat("8", 64), RecipeDigest: claim.Manifest.RecipeDigest,
		ArtifactDigest: claim.Manifest.ArtifactDigest, WorkerResourceManifestDigest: claim.Manifest.WorkerResourceManifestDigest,
	}
	store := &recipeArtifactTransferMemoryStore{claim: claim}
	transport := &recipeArtifactTransferMemoryTransport{now: now, failComplete: true}
	uploader := &recipeArtifactUploaderMemory{versionID: "stage-s-version-0001"}
	manager, err := NewRecipeArtifactTransferManager(store, transport, uploader, archive, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Ensure(t.Context(), claim); err == nil || store.versionID != uploader.versionID || store.verified {
		t.Fatalf("first ensure err=%v store=%#v", err, store)
	}
	if !store.versionRecordedBeforeComplete || transport.prepareBuilds != 1 || transport.completeBuilds != 1 || uploader.calls != 1 {
		t.Fatalf("incorrect transfer order store=%#v transport=%#v uploader=%#v", store, transport, uploader)
	}
	transport.failComplete = false
	if err := manager.Ensure(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	if !store.verified || transport.prepareBuilds != 1 || transport.completeBuilds != 1 || len(transport.completeEnvelopes) != 2 || transport.completeEnvelopes[0] != transport.completeEnvelopes[1] {
		t.Fatalf("complete replay was not exact: store=%#v transport=%#v", store, transport)
	}
	if store.prepare.NodeCounter == store.complete.NodeCounter || store.prepare.Action != RecipeArtifactPutAction || store.complete.Action != RecipeArtifactPutAction || store.prepare.RequestDigest == store.complete.RequestDigest {
		t.Fatalf("prepare/complete command binding invalid: prepare=%#v complete=%#v", store.prepare, store.complete)
	}
}

type recipeArtifactTransferMemoryStore struct {
	claim                         RecipeInstallClaim
	binding                       RecipeArtifactTransferBinding
	prepare, complete             RecipeArtifactTransferCommand
	versionID                     string
	verified                      bool
	versionRecordedBeforeComplete bool
}

func (store *recipeArtifactTransferMemoryStore) LoadOrCreateRecipeArtifactTransfer(_ context.Context, claim RecipeInstallClaim, binding RecipeArtifactTransferBinding) (RecipeArtifactTransfer, error) {
	if claim.ExecutionID != store.claim.ExecutionID {
		return RecipeArtifactTransfer{}, errors.New("wrong claim")
	}
	if store.binding == (RecipeArtifactTransferBinding{}) {
		store.binding = binding
	}
	if store.binding != binding {
		return RecipeArtifactTransfer{}, errors.New("binding conflict")
	}
	if store.verified {
		return RecipeArtifactTransfer{Phase: RecipeArtifactTransferPhaseVerified, ConnectionID: claim.ConnectionID, Binding: binding, VersionID: store.versionID}, nil
	}
	if store.versionID == "" {
		if store.prepare.CommandID == "" {
			request := RecipeArtifactPutPrepareRequest{Schema: RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}
			digest, _ := request.Digest()
			store.prepare = RecipeArtifactTransferCommand{CommandID: "artifact-prepare-command", ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: 20, Phase: RecipeArtifactTransferPhasePrepare, Action: RecipeArtifactPutAction, RequestDigest: digest, State: "allocated"}
		}
		return RecipeArtifactTransfer{Phase: RecipeArtifactTransferPhasePrepare, ConnectionID: claim.ConnectionID, Binding: binding, Command: store.prepare}, nil
	}
	if store.complete.CommandID == "" {
		request := RecipeArtifactPutCompleteRequest{Schema: RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: store.versionID}
		digest, _ := request.Digest()
		store.complete = RecipeArtifactTransferCommand{CommandID: "artifact-complete-command", ExecutionID: claim.ExecutionID, DeploymentID: claim.DeploymentID, TaskID: claim.TaskID, ConnectionID: claim.ConnectionID, NodeKeyID: claim.NodeKeyID, ExpectedGeneration: claim.ExpectedGeneration, NodeCounter: 21, Phase: RecipeArtifactTransferPhaseComplete, Action: RecipeArtifactPutAction, RequestDigest: digest, State: "allocated"}
	}
	return RecipeArtifactTransfer{Phase: RecipeArtifactTransferPhaseComplete, ConnectionID: claim.ConnectionID, Binding: binding, VersionID: store.versionID, Command: store.complete}, nil
}

func (store *recipeArtifactTransferMemoryStore) PersistRecipeArtifactTransferCommand(_ context.Context, _ RecipeInstallClaim, transfer RecipeArtifactTransfer, signed SignedRecipeArtifactTransferCommand) error {
	command := &store.prepare
	if transfer.Phase == RecipeArtifactTransferPhaseComplete {
		command = &store.complete
	}
	command.SignedEnvelope, command.PayloadJSON, command.PayloadSHA256, command.RequestSHA256 = signed.EnvelopeJSON, signed.PayloadJSON, signed.PayloadSHA256, signed.RequestSHA256
	command.IssuedAt, command.ExpiresAt, command.State = signed.IssuedAt, signed.ExpiresAt, "signed"
	return nil
}

func (store *recipeArtifactTransferMemoryStore) RecordRecipeArtifactVersion(_ context.Context, _ RecipeInstallClaim, _ RecipeArtifactTransfer, versionID string) error {
	store.versionID = versionID
	store.versionRecordedBeforeComplete = versionID != ""
	return nil
}

func (store *recipeArtifactTransferMemoryStore) CommitRecipeArtifactTransfer(_ context.Context, _ RecipeInstallClaim, transfer RecipeArtifactTransfer) error {
	store.versionRecordedBeforeComplete = store.versionID != "" && transfer.VersionID == store.versionID
	store.verified = store.versionRecordedBeforeComplete
	return nil
}

type recipeArtifactTransferMemoryTransport struct {
	now               time.Time
	prepareBuilds     int
	completeBuilds    int
	failComplete      bool
	completeEnvelopes []string
}

func (transport *recipeArtifactTransferMemoryTransport) BuildRecipeArtifactPrepareCommand(_ RecipeArtifactTransferCommand, _ RecipeArtifactPutPrepareRequest, _ time.Time) (SignedRecipeArtifactTransferCommand, error) {
	transport.prepareBuilds++
	return transferSignedFixture("prepare", transport.now), nil
}
func (transport *recipeArtifactTransferMemoryTransport) RequestRecipeArtifactPrepare(context.Context, string, RecipeArtifactTransferCommand, SignedRecipeArtifactTransferCommand, RecipeArtifactPutPrepareRequest) (RecipeArtifactUploadGrant, error) {
	return RecipeArtifactUploadGrant{Method: "PUT", URL: "https://upload.invalid/object?X-Amz-Signature=fixed", ExpiresAt: transport.now.Add(time.Minute), Headers: map[string]string{}}, nil
}
func (transport *recipeArtifactTransferMemoryTransport) BuildRecipeArtifactCompleteCommand(_ RecipeArtifactTransferCommand, _ RecipeArtifactPutCompleteRequest, _ time.Time) (SignedRecipeArtifactTransferCommand, error) {
	transport.completeBuilds++
	return transferSignedFixture("complete", transport.now), nil
}
func (transport *recipeArtifactTransferMemoryTransport) RequestRecipeArtifactComplete(_ context.Context, _ string, _ RecipeArtifactTransferCommand, signed SignedRecipeArtifactTransferCommand, _ RecipeArtifactPutCompleteRequest) error {
	transport.completeEnvelopes = append(transport.completeEnvelopes, signed.EnvelopeJSON)
	if transport.failComplete {
		return errors.New("complete response lost")
	}
	return nil
}

func transferSignedFixture(phase string, now time.Time) SignedRecipeArtifactTransferCommand {
	return SignedRecipeArtifactTransferCommand{EnvelopeJSON: `{"phase":"` + phase + `"}`, PayloadJSON: `{"sealed":true}`, PayloadSHA256: strings.Repeat("a", 64), RequestSHA256: strings.Repeat("b", 64), IssuedAt: now, ExpiresAt: now.Add(4 * time.Minute)}
}

type recipeArtifactUploaderMemory struct {
	versionID string
	calls     int
}

func (uploader *recipeArtifactUploaderMemory) Upload(context.Context, TrustedRecipeArtifactArchive, RecipeArtifactUploadGrant) (string, error) {
	uploader.calls++
	return uploader.versionID, nil
}
