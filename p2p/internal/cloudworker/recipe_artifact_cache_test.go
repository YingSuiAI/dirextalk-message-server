package cloudworker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/artifactbuilder"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestRecipeArtifactCacheDownloadsValidatesRegistersAndReusesByArtifactDigest(t *testing.T) {
	fixture := buildRecipeArtifactFixture(t)
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.Method != http.MethodGet || request.URL.Query().Get("temporary") != "secret" || request.Header.Get("Accept-Encoding") != "identity" {
			http.Error(writer, "invalid", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", recipeexec.RecipeArtifactMediaTypeV1)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(fixture.archive)
	}))
	defer server.Close()

	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	resolver, err := recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, fixture.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	var proxyCalls atomic.Int32
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.Proxy = func(*http.Request) (*url.URL, error) {
		proxyCalls.Add(1)
		return url.Parse("http://proxy.example.invalid")
	}
	directClient := *server.Client()
	directClient.Transport = transport
	session := &SessionClient{client: &directClient, now: func() time.Time { return now }}
	cacheRoot := filepath.Join(t.TempDir(), "deployment-private", "cache")
	cache, err := NewRecipeArtifactCache(session, cacheRoot, fixture.manifest.DeploymentID, resolver, fixture.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	claim := recipeArtifactClaim(t, fixture, recipeArtifactAccess(server.URL+"/artifact?temporary=secret", fixture, now.Add(10*time.Minute)))
	if err := cache.Prepare(context.Background(), claim); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), fixture.manifest.ArtifactDigest); err != nil {
		t.Fatalf("downloaded artifact was not registered: %v", err)
	}
	cachedClaim := claim
	cachedClaim.ArtifactAccess = nil
	if err := cache.Prepare(context.Background(), cachedClaim); err != nil {
		t.Fatalf("cached Prepare() error = %v", err)
	}
	if requests.Load() != 1 || proxyCalls.Load() != 0 {
		t.Fatalf("artifact requests=%d proxy calls=%d", requests.Load(), proxyCalls.Load())
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(cacheRoot)
		if err != nil {
			t.Fatalf("stat cache root: %v", err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("cache root mode = %v", info.Mode().Perm())
		}
	}
	if err := filepath.Walk(cacheRoot, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() {
			return walkErr
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte("temporary=secret")) || bytes.Contains(raw, []byte(server.URL)) {
			t.Fatalf("temporary artifact URL was persisted in %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRecipeArtifactCacheRejectsTarLinks(t *testing.T) {
	fixture := buildRecipeArtifactFixture(t)
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	for name, kind := range map[string]byte{"symlink": tar.TypeSymlink, "hardlink": tar.TypeLink} {
		t.Run(name, func(t *testing.T) {
			var archive bytes.Buffer
			writer := tar.NewWriter(&archive)
			_ = writer.WriteHeader(&tar.Header{Name: artifactbuilder.ControllerTrustedArtifactCatalog, Linkname: "target", Mode: 0o644, Typeflag: kind})
			_ = writer.Close()
			sum := sha256.Sum256(archive.Bytes())
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				response.Header().Set("Content-Type", recipeexec.RecipeArtifactMediaTypeV1)
				_, _ = response.Write(archive.Bytes())
			}))
			defer server.Close()
			resolver, _ := recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, fixture.runningWorkerDigest)
			cache, err := NewRecipeArtifactCache(&SessionClient{client: server.Client(), now: func() time.Time { return now }}, filepath.Join(t.TempDir(), "cache"), fixture.manifest.DeploymentID, resolver, fixture.runningWorkerDigest)
			if err != nil {
				t.Fatal(err)
			}
			access := recipeArtifactAccess(server.URL+"/artifact?temporary=secret", fixture, now.Add(time.Minute))
			access.SizeBytes, access.ArchiveSHA256 = int64(archive.Len()), hex.EncodeToString(sum[:])
			if err := cache.Prepare(context.Background(), recipeArtifactClaim(t, fixture, access)); err == nil {
				t.Fatalf("%s tar entry was accepted", name)
			}
		})
	}
}

func TestRecipeArtifactCacheRejectsRedirectsAndUnsafeTarWithoutLeakingGrant(t *testing.T) {
	fixture := buildRecipeArtifactFixture(t)
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	payload := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", recipeexec.RecipeArtifactMediaTypeV1)
		_, _ = writer.Write(fixture.archive)
	}))
	defer payload.Close()
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, payload.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	resolver, _ := recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, fixture.runningWorkerDigest)
	cache, err := NewRecipeArtifactCache(&SessionClient{client: redirect.Client(), now: func() time.Time { return now }}, filepath.Join(t.TempDir(), "cache"), fixture.manifest.DeploymentID, resolver, fixture.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	secretURL := redirect.URL + "/artifact?temporary=must-not-leak"
	claim := recipeArtifactClaim(t, fixture, recipeArtifactAccess(secretURL, fixture, now.Add(time.Minute)))
	if err := cache.Prepare(context.Background(), claim); err == nil || strings.Contains(err.Error(), "must-not-leak") || strings.Contains(err.Error(), redirect.URL) {
		t.Fatalf("redirect error leaked or accepted grant: %v", err)
	}
	resolver, _ = recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, fixture.runningWorkerDigest)
	cache, err = NewRecipeArtifactCache(&SessionClient{client: payload.Client(), now: func() time.Time { return now }}, filepath.Join(t.TempDir(), "cache"), fixture.manifest.DeploymentID, resolver, fixture.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	wrongChecksum := recipeArtifactAccess(payload.URL+"/artifact?temporary=must-not-leak", fixture, now.Add(time.Minute))
	wrongChecksum.ArchiveSHA256 = strings.Repeat("0", 64)
	if err := cache.Prepare(context.Background(), recipeArtifactClaim(t, fixture, wrongChecksum)); err == nil || strings.Contains(err.Error(), "must-not-leak") {
		t.Fatalf("checksum error leaked or accepted grant: %v", err)
	}
	drifted := fixture
	drifted.manifest.SemanticReadiness.Path = "/different"
	resolver, _ = recipeexec.NewDynamicOCICatalogResolver(drifted.manifest.WorkerResourceManifestDigest, drifted.runningWorkerDigest)
	cache, err = NewRecipeArtifactCache(&SessionClient{client: payload.Client(), now: func() time.Time { return now }}, filepath.Join(t.TempDir(), "cache"), drifted.manifest.DeploymentID, resolver, drifted.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Prepare(context.Background(), recipeArtifactClaim(t, drifted, recipeArtifactAccess(payload.URL+"/artifact?temporary=secret", drifted, now.Add(time.Minute)))); err == nil {
		t.Fatal("archive compiled contract drift was accepted")
	}
	wrongRunningDigest := "sha256:" + strings.Repeat("f", 64)
	resolver, _ = recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, wrongRunningDigest)
	cache, err = NewRecipeArtifactCache(&SessionClient{client: payload.Client(), now: func() time.Time { return now }}, filepath.Join(t.TempDir(), "cache"), fixture.manifest.DeploymentID, resolver, wrongRunningDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Prepare(context.Background(), recipeArtifactClaim(t, fixture, recipeArtifactAccess(payload.URL+"/artifact?temporary=secret", fixture, now.Add(time.Minute)))); err == nil {
		t.Fatal("archive built for a different Worker binary was accepted")
	}

	var unsafe bytes.Buffer
	writer := tar.NewWriter(&unsafe)
	content := []byte("escape")
	_ = writer.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg})
	_, _ = writer.Write(content)
	_ = writer.Close()
	unsafeSum := sha256.Sum256(unsafe.Bytes())
	unsafeServer := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", recipeexec.RecipeArtifactMediaTypeV1)
		_, _ = response.Write(unsafe.Bytes())
	}))
	defer unsafeServer.Close()
	resolver, _ = recipeexec.NewDynamicOCICatalogResolver(fixture.manifest.WorkerResourceManifestDigest, fixture.runningWorkerDigest)
	root := t.TempDir()
	cache, err = NewRecipeArtifactCache(&SessionClient{client: unsafeServer.Client(), now: func() time.Time { return now }}, filepath.Join(root, "cache"), fixture.manifest.DeploymentID, resolver, fixture.runningWorkerDigest)
	if err != nil {
		t.Fatal(err)
	}
	access := recipeArtifactAccess(unsafeServer.URL+"/artifact?temporary=secret", fixture, now.Add(time.Minute))
	access.SizeBytes, access.ArchiveSHA256 = int64(unsafe.Len()), hex.EncodeToString(unsafeSum[:])
	if err := cache.Prepare(context.Background(), recipeArtifactClaim(t, fixture, access)); err == nil {
		t.Fatal("unsafe tar path was accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "escape")); !os.IsNotExist(err) {
		t.Fatalf("unsafe tar escaped cache: %v", err)
	}
}

type recipeArtifactFixture struct {
	archive             []byte
	archiveSHA256       string
	manifest            cloudorchestrator.RecipeExecutionManifestV1
	runningWorkerDigest string
}

func buildRecipeArtifactFixture(t *testing.T) recipeArtifactFixture {
	t.Helper()
	digest := func(character string) string { return "sha256:" + strings.Repeat(character, 64) }
	workerBytes := []byte("dirextalk-cloud-worker-test-binary")
	workerSum := sha256.Sum256(workerBytes)
	workerDigest := "sha256:" + hex.EncodeToString(workerSum[:])
	workerPath := filepath.Join(t.TempDir(), "cloud-worker")
	if err := os.WriteFile(workerPath, workerBytes, 0o755); err != nil {
		t.Fatal(err)
	}
	healthContract := cloudorchestrator.HealthContractV1{Liveness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/health"}, Readiness: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/ready"}, Semantic: cloudorchestrator.ProbeV1{Kind: cloudorchestrator.ProbeHTTP, Target: "/semantic"}}
	lifecycle := cloudorchestrator.LifecycleContractV1{Start: "start_v1", Stop: "stop_v1", Restart: "restart_v1", Upgrade: "upgrade_v1", Rollback: "rollback_v1", Backup: "backup_v1", Restore: "restore_v1", Destroy: "destroy_v1"}
	recipe := cloudorchestrator.RecipeV1{SchemaVersion: cloudorchestrator.SchemaVersionV1, RecipeID: "recipe-dynamic-0001", Name: "Dynamic artifact fixture", Maturity: cloudorchestrator.RecipeExperimental,
		Sources:      []cloudorchestrator.RecipeSourceV1{{URL: "https://github.com/example/service", Version: "v1.2.3", Commit: "0123456789abcdef0123456789abcdef01234567", ArtifactDigest: digest("1"), License: "Apache-2.0", RetrievedAt: time.Date(2026, 7, 16, 0, 0, 0, 0, time.UTC), Official: true}},
		Requirements: cloudorchestrator.ResourceRequirementsV1{MinVCPU: 2, MinMemoryMiB: 4096, MinDiskGiB: 40, Architecture: cloudorchestrator.ArchitectureAMD64},
		Install:      cloudorchestrator.InstallContractV1{RootRequired: true, TimeoutSeconds: 1800, CheckpointNames: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1(), Steps: []cloudorchestrator.InstallStepV1{{ID: "install", Summary: "Install service", TimeoutSeconds: 1800}}}, Health: healthContract, Lifecycle: lifecycle}
	probe := cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: FixedReadinessEvidenceDigest()}
	spec := artifactbuilder.BuildSpecV1{SchemaVersion: artifactbuilder.BuildSpecV1Schema, ArtifactVersion: "v1.1.0-dynamic.1", RecipeRevision: 1, ImageSource: cloudorchestrator.OCIImageSourceReferenceV1("ghcr.io/dirextalk/dynamic-service@" + digest("2")), ImageDigest: digest("2"), ImageSizeBytes: 1 << 20,
		Actions: []cloudorchestrator.CompiledRecipeActionV1{{Kind: cloudorchestrator.CompiledRecipeActionInstall, ActionID: "install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: cloudorchestrator.OCIServiceInstallCheckpointSequenceV1()}},
		Health:  cloudorchestrator.OCIServiceHealthV1{Liveness: probe, Readiness: probe, Semantic: probe}}
	recipeRaw, _ := json.Marshal(recipe)
	specRaw, _ := json.Marshal(spec)
	var archive bytes.Buffer
	result, err := artifactbuilder.BuildArchive(recipeRaw, specRaw, workerPath, &archive)
	if err != nil {
		t.Fatal(err)
	}
	recipeDigest, _ := recipe.Digest()
	manifest := testRecipeExecutionManifest()
	manifest.RecipeDigest = recipeDigest
	manifest.WorkerResourceManifestDigest = result.Catalog.WorkerResourceManifestDigest
	manifest.ArtifactDigest = spec.ImageDigest
	manifest.ActionID = spec.Actions[0].ActionID
	manifest.RootRequired = spec.Actions[0].RootRequired
	manifest.TimeoutSeconds = spec.Actions[0].TimeoutSeconds
	manifest.CheckpointSequence = append([]string(nil), spec.Actions[0].CheckpointSequence...)
	manifest.SemanticReadiness = spec.Health.Semantic
	return recipeArtifactFixture{archive: archive.Bytes(), archiveSHA256: strings.TrimPrefix(result.ArchiveSHA256, "sha256:"), manifest: manifest, runningWorkerDigest: workerDigest}
}

func recipeArtifactAccess(rawURL string, fixture recipeArtifactFixture, expiresAt time.Time) *recipeexec.ArtifactAccessV1 {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	query := parsed.Query()
	query.Set("versionId", "version-0001")
	parsed.RawQuery = query.Encode()
	return &recipeexec.ArtifactAccessV1{Method: http.MethodGet, URL: parsed.String(), ExpiresAt: expiresAt.UTC().Format("2006-01-02T15:04:05.000Z"), VersionID: "version-0001", MediaType: recipeexec.RecipeArtifactMediaTypeV1, SizeBytes: int64(len(fixture.archive)), ArchiveSHA256: fixture.archiveSHA256}
}

func recipeArtifactClaim(t *testing.T, fixture recipeArtifactFixture, access *recipeexec.ArtifactAccessV1) ClaimedRecipeTask {
	t.Helper()
	manifestDigest, err := fixture.manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-artifact-0001", ExecutionID: fixture.manifest.ExecutionID, DeploymentID: fixture.manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: manifestDigest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), fixture.manifest.CheckpointSequence...), Attempt: 1}
	return ClaimedRecipeTask{Task: task, Manifest: fixture.manifest, ArtifactAccess: access, Epoch: 1}
}
