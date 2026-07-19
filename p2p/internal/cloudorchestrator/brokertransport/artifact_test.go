package brokertransport

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

func TestArtifactPrepareAndCompleteUseDistinctPersistableCommands(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := New(privateKey, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	binding := runtime.RecipeArtifactTransferBinding{
		DeploymentID: "deployment-artifact-1", TaskID: "task-artifact-1", ExecutionID: "execution-artifact-1",
		RecipeDigest: namedTestDigest("a"), ArtifactDigest: namedTestDigest("b"), ManifestDigest: namedTestDigest("c"),
		ArchiveSHA256: strings.Repeat("d", 64), SizeBytes: 4096, MediaType: runtime.RecipeArtifactTarMediaType,
	}
	prepareRequest := runtime.RecipeArtifactPutPrepareRequest{Schema: runtime.RecipeArtifactPutPrepareSchema, RecipeArtifactTransferBinding: binding}
	prepareDigest, err := prepareRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	base := runtime.RecipeArtifactTransferCommand{
		CommandID: "artifact-command-prepare", ExecutionID: binding.ExecutionID, DeploymentID: binding.DeploymentID, TaskID: binding.TaskID,
		ConnectionID: "connection-artifact-1", NodeKeyID: "node-key-artifact-1", ExpectedGeneration: 2, NodeCounter: 41,
		Phase: runtime.RecipeArtifactTransferPhasePrepare, Action: runtime.RecipeArtifactPutAction, RequestDigest: prepareDigest,
	}
	now := time.Date(2026, 7, 16, 4, 0, 0, 0, time.UTC)
	prepareSigned, err := transport.BuildRecipeArtifactPrepareCommand(base, prepareRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	prepareEnvelope, err := persistedRecipeArtifactCommand(base, prepareSigned)
	if err != nil {
		t.Fatal(err)
	}
	if request, err := prepareEnvelope.PrepareRequest(); err != nil || request.Schema != runtime.RecipeArtifactPutPrepareSchema {
		t.Fatalf("prepare request=%#v err=%v", request, err)
	}

	completeRequest := runtime.RecipeArtifactPutCompleteRequest{
		Schema: runtime.RecipeArtifactPutCompleteSchema, RecipeArtifactTransferBinding: binding, VersionID: "3Lg_artifact-version-1",
	}
	completeDigest, err := completeRequest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	complete := base
	complete.CommandID = "artifact-command-complete"
	complete.NodeCounter++
	complete.Phase = runtime.RecipeArtifactTransferPhaseComplete
	complete.RequestDigest = completeDigest
	completeSigned, err := transport.BuildRecipeArtifactCompleteCommand(complete, completeRequest, now)
	if err != nil {
		t.Fatal(err)
	}
	completeEnvelope, err := persistedRecipeArtifactCommand(complete, completeSigned)
	if err != nil {
		t.Fatal(err)
	}
	if request, err := completeEnvelope.CompleteRequest(); err != nil || request.Schema != runtime.RecipeArtifactPutCompleteSchema || request.VersionID != completeRequest.VersionID {
		t.Fatalf("complete request=%#v err=%v", request, err)
	}
	if prepareEnvelope.Action != completeEnvelope.Action || prepareEnvelope.NodeCounter == completeEnvelope.NodeCounter ||
		prepareSigned.RequestSHA256 == completeSigned.RequestSHA256 || prepareDigest == completeDigest {
		t.Fatalf("prepare=%#v complete=%#v", prepareEnvelope, completeEnvelope)
	}
}

func namedTestDigest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
