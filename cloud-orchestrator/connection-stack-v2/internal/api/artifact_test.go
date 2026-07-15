package api

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

func TestArtifactPrepareCompleteBindsFinalizedRecipeAndVerifiesObject(t *testing.T) {
	broker, deployments, recipes, key, manifest := scopedRecipeTaskBroker(t)
	broker.ArtifactEnabled = true
	reservation := deployments.deployments["connection-0001\x00deployment-0001"]
	reservation.WorkerSession.WorkerImageDigest = "sha256:" + strings.Repeat("8", 64)
	deployments.deployments["connection-0001\x00deployment-0001"] = reservation
	if response := issueRecipeManifest(t, broker, key, manifest); response.Code != 200 {
		t.Fatalf("issue=%d %s", response.Code, response.Body.String())
	}
	digest, _ := manifest.Digest()
	binding := contract.ArtifactBinding{DeploymentID: manifest.DeploymentID, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, RecipeDigest: manifest.RecipeDigest, ArtifactDigest: manifest.ArtifactDigest, ManifestDigest: digest, ArchiveSHA256: strings.Repeat("a", 64), SizeBytes: 1024, MediaType: contract.ArtifactMediaType}
	artifacts := &memoryArtifactStore{records: map[string]commandstore.ArtifactRecord{}}
	provider := &fakeArtifactProvider{}
	broker.ArtifactStore = artifacts
	broker.ArtifactProvider = provider
	broker.RecipeTasks = recipes
	prepare, _ := json.Marshal(contract.ArtifactPutPrepareRequest{Schema: contract.ArtifactPutPrepareSchema, ArtifactBinding: binding})
	raw := signedReadOnlyCommand(t, key, "command-artifact-prepare-0001", 2, contract.ActionArtifactPut, prepare)
	response := serve(t, broker, "POST", commandPath, raw)
	if response.Code != 200 || provider.puts != 1 || !strings.Contains(response.Body.String(), `"status":"uploading"`) {
		t.Fatalf("prepare=%d %s puts=%d", response.Code, response.Body.String(), provider.puts)
	}
	response = serve(t, broker, "POST", commandPath, raw)
	if response.Code != 200 || provider.puts != 2 {
		t.Fatalf("prepare replay=%d puts=%d", response.Code, provider.puts)
	}
	complete, _ := json.Marshal(contract.ArtifactPutCompleteRequest{Schema: contract.ArtifactPutCompleteSchema, ArtifactBinding: binding, VersionID: "version-0001"})
	raw = signedReadOnlyCommand(t, key, "command-artifact-complete-0001", 3, contract.ActionArtifactPut, complete)
	response = serve(t, broker, "POST", commandPath, raw)
	if response.Code != 200 || provider.heads != 1 || artifacts.records[artifactTestKey(binding)].State != "verified" {
		t.Fatalf("complete=%d %s heads=%d", response.Code, response.Body.String(), provider.heads)
	}
	response = serve(t, broker, "POST", commandPath, raw)
	if response.Code != 200 || provider.heads != 1 {
		t.Fatalf("complete replay=%d heads=%d", response.Code, provider.heads)
	}
	bad := binding
	bad.ArtifactDigest = "sha256:" + strings.Repeat("9", 64)
	prepare, _ = json.Marshal(contract.ArtifactPutPrepareRequest{Schema: contract.ArtifactPutPrepareSchema, ArtifactBinding: bad})
	raw = signedReadOnlyCommand(t, key, "command-artifact-bad-0001", 4, contract.ActionArtifactPut, prepare)
	response = serve(t, broker, "POST", commandPath, raw)
	if response.Code != 403 || provider.puts != 2 {
		t.Fatalf("scope mismatch=%d puts=%d", response.Code, provider.puts)
	}
}

type memoryArtifactStore struct {
	mu      sync.Mutex
	records map[string]commandstore.ArtifactRecord
}

func artifactTestKey(b contract.ArtifactBinding) string { return b.DeploymentID + "\x00" + b.TaskID }
func (s *memoryArtifactStore) LookupArtifact(_ context.Context, connectionID, deploymentID, taskID string) (commandstore.ArtifactRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[deploymentID+"\x00"+taskID]
	return r, ok, nil
}
func (s *memoryArtifactStore) PrepareArtifact(_ context.Context, receipt commandstore.Record, a commandstore.ArtifactRecord) (commandstore.Record, commandstore.ArtifactRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := artifactTestKey(a.Binding)
	if v, ok := s.records[k]; ok {
		return receipt, v, false, nil
	}
	s.records[k] = a
	return receipt, a, true, nil
}
func (s *memoryArtifactStore) CompleteArtifact(_ context.Context, receipt commandstore.Record, a commandstore.ArtifactRecord) (commandstore.Record, commandstore.ArtifactRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[artifactTestKey(a.Binding)] = a
	return receipt, a, true, nil
}

type fakeArtifactProvider struct{ puts, heads int }

func (p *fakeArtifactProvider) PresignPut(_ context.Context, _ string, _ contract.ArtifactBinding, _ time.Duration) (string, time.Time, error) {
	p.puts++
	return "https://s3.example.invalid/put?sig=redacted", time.Date(2026, 7, 15, 1, 13, 0, 0, time.UTC), nil
}
func (p *fakeArtifactProvider) Head(_ context.Context, _, version string) (ArtifactObjectObservation, error) {
	p.heads++
	return ArtifactObjectObservation{VersionID: version, ContentType: contract.ArtifactMediaType, ChecksumSHA256: contract.ArtifactBinding{ArchiveSHA256: strings.Repeat("a", 64)}.ChecksumBase64(), ServerSideEncryption: "aws:kms", SizeBytes: 1024}, nil
}
func (p *fakeArtifactProvider) PresignGet(_ context.Context, _, version string, _ time.Duration) (string, time.Time, error) {
	return "https://s3.example.invalid/get?versionId=" + url.QueryEscape(version) + "&sig=redacted", time.Date(2026, 7, 15, 1, 13, 0, 0, time.UTC), nil
}
