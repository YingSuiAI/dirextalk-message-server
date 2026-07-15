package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestOCIConfigFilesAndExecutableDigestFailClosed(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "catalog.json")
	content := []byte(`{"schema":"test"}`)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readStrictRegularFile(path, 1024)
	if err != nil || string(got) != string(content) {
		t.Fatalf("strict read=%q err=%v", got, err)
	}
	digest, err := streamRegularFileSHA256(path)
	sum := sha256.Sum256(content)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if err != nil || digest != want {
		t.Fatalf("stream digest=%q err=%v want=%q", digest, err, want)
	}
	link := filepath.Join(directory, "catalog-link.json")
	if err := os.Symlink(path, link); err == nil {
		if _, err := readStrictRegularFile(link, 1024); !errors.Is(err, errConfigInvalid) {
			t.Fatalf("symlink read error=%v", err)
		}
	}
	if _, err := readStrictRegularFile(path, int64(len(content)-1)); !errors.Is(err, errConfigInvalid) {
		t.Fatalf("oversize read error=%v", err)
	}
}

func TestOCIRequiresOneExactApprovedWorkerResourceManifestDigest(t *testing.T) {
	manifest := cloudworker.BootstrapManifest{WorkerImageDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ArtifactManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if got, err := approvedWorkerResourceManifestDigest(manifest); err != nil || got != manifest.WorkerImageDigest {
		t.Fatalf("approved manifest digest=%q err=%v", got, err)
	}
	manifest.ArtifactManifestDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := approvedWorkerResourceManifestDigest(manifest); !errors.Is(err, recipeexec.ErrExecutorConfiguration) {
		t.Fatalf("mismatched bootstrap digest error=%v", err)
	}
}

func TestOCIReadinessFirstValidationGateRequiresExactFixedProbe(t *testing.T) {
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: cloudworker.FixedReadinessEvidenceDigest()}
	catalog := recipeexec.WorkerOCICatalogV1{Entries: []recipeexec.WorkerOCICatalogEntryV1{{Descriptor: cloudorchestrator.OCIServiceBundleV1{Health: cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: probe}}}}}
	if err := validateInitialOCIReadiness(catalog); err != nil {
		t.Fatal(err)
	}
	catalog.Entries[0].Descriptor.Health.Semantic.Path = "/semantic"
	if err := validateInitialOCIReadiness(catalog); !errors.Is(err, recipeexec.ErrExecutorConfiguration) {
		t.Fatalf("readiness drift error=%v", err)
	}
}

func TestOCIRecipeFailsBeforeAnyTaskClaimWhenDependenciesAreUnavailable(t *testing.T) {
	now := time.Date(2026, time.July, 15, 2, 0, 0, 0, time.UTC)
	config := writeWorkerBootstrapConfig(t, now, true, time.Second)
	config.ociRecipe = true
	config.recipeCheckpointDir = filepath.Join(t.TempDir(), "checkpoints")
	config.ociCatalogFile = filepath.Join(t.TempDir(), "missing-catalog.json")
	config.workerResourceFile = filepath.Join(t.TempDir(), "missing-resource.json")
	provider := &recordingIdentityProofProvider{proof: validWorkerIdentityProof()}
	client := &recordingWorkerSessionClient{}
	err := runWithDependencies(context.Background(), config, provider,
		func(_ cloudworker.BootstrapManifest, _ cloudworker.SessionClientConfig) (workerSessionClient, error) {
			return client, nil
		},
		func() time.Time { return now },
	)
	if !errors.Is(err, errRunFailed) || client.heartbeatCalls != 0 || client.claimTaskCalls != 0 || client.retryTaskCalls != 0 {
		t.Fatalf("OCI fail-closed result err=%v client=%#v", err, client)
	}
}
