package store

import (
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
)

func TestDynamoArtifactStoreAtomicallyFencesPrepareAndComplete(t *testing.T) {
	fake := &fakeDynamo{}
	store, err := NewDynamoArtifactStore(fake, "receipts", "counters", "artifacts")
	if err != nil {
		t.Fatal(err)
	}
	binding := contract.ArtifactBinding{DeploymentID: "deployment-0001", TaskID: "task-artifact-0001", ExecutionID: "execution-0001", RecipeDigest: "sha256:" + strings.Repeat("1", 64), ArtifactDigest: "sha256:" + strings.Repeat("2", 64), ManifestDigest: "sha256:" + strings.Repeat("3", 64), ArchiveSHA256: strings.Repeat("4", 64), SizeBytes: 1024, MediaType: contract.ArtifactMediaType}
	receipt := Record{ConnectionID: "connection-0001", CommandID: "command-artifact-prepare-0001", RequestSHA256: strings.Repeat("5", 64), ExpectedGeneration: 1, NodeCounter: 2, Action: contract.ActionArtifactPut, ResultJSON: []byte(`{"status":"uploading"}`)}
	artifact := ArtifactRecord{ConnectionID: receipt.ConnectionID, Binding: binding, ObjectKey: binding.ObjectKey(), State: "uploading", ExpiresAt: "2026-07-15T01:10:00.000Z"}
	if _, _, _, err = store.PrepareArtifact(t.Context(), receipt, artifact); err != nil || len(fake.transactInput.TransactItems) != 3 || fake.transactInput.TransactItems[2].Put == nil {
		t.Fatalf("prepare transaction=%#v err=%v", fake.transactInput, err)
	}
	receipt.CommandID = "command-artifact-complete-0001"
	receipt.NodeCounter = 3
	receipt.RequestSHA256 = strings.Repeat("6", 64)
	receipt.ResultJSON = []byte(`{"status":"verified"}`)
	artifact.State = "verified"
	artifact.VersionID = "version-0001"
	artifact.ExpiresAt = ""
	artifact.VerifiedAt = "2026-07-15T01:05:00.000Z"
	if _, _, _, err = store.CompleteArtifact(t.Context(), receipt, artifact); err != nil || len(fake.transactInput.TransactItems) != 3 || fake.transactInput.TransactItems[2].Update == nil {
		t.Fatalf("complete transaction=%#v err=%v", fake.transactInput, err)
	}
}
