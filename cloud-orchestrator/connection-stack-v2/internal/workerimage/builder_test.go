package workerimage

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestBuildVerifyDestroyHappyAndResponseLossRecovery(t *testing.T) {
	artifact := artifactFixture(t)
	for _, responseLoss := range []bool{false, true} {
		t.Run(map[bool]string{false: "happy", true: "response_loss"}[responseLoss], func(t *testing.T) {
			clock := &fakeClock{now: time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC)}
			provider := &fakeProvider{artifact: artifact, responseLoss: responseLoss, images: map[string]ImageObservation{}}
			builder, _ := NewBuilder(provider, clock)
			builder.poll = time.Second
			config := buildConfigFixture(artifact)
			provider.marker = successMarker(config.ArtifactVersion, artifact, config.DynamicRecipeArtifacts)
			manifest, err := builder.Build(context.Background(), config, artifact)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			if provider.putCalls != 1 || provider.deleteObjectCalls != 1 || provider.terminateCalls != 1 || manifest.TrustedCatalogDigest != artifact.CatalogDigest || manifest.RecipeArtifactMode != RecipeArtifactStatic {
				t.Fatalf("cleanup/binding mismatch: %#v", provider)
			}
			if provider.launchSpec.Tags["dirextalk:recipe-artifact-mode"] != RecipeArtifactStatic ||
				!strings.Contains(provider.launchSpec.UserData, "podman pull") ||
				!strings.Contains(provider.launchSpec.UserData, "CLOUD_WORKER_OCI_CATALOG_FILE=/opt/dirextalk-worker/worker-oci-catalog.json") ||
				!strings.Contains(provider.launchSpec.UserData, "CLOUD_WORKER_RESOURCE_MANIFEST_FILE=/opt/dirextalk-worker/worker-resource-manifest.json") ||
				strings.Contains(provider.launchSpec.UserData, "CLOUD_WORKER_DYNAMIC_RECIPE_ARTIFACTS_ENABLED=true") {
				t.Fatal("default static image behavior changed")
			}
			if err := builder.Verify(context.Background(), manifest); err != nil {
				t.Fatalf("Verify: %v", err)
			}
			second, err := builder.Build(context.Background(), config, artifact)
			if err != nil || second.ImageID != manifest.ImageID || provider.putCalls != 1 {
				t.Fatalf("idempotent Build=%#v err=%v put_calls=%d", second, err, provider.putCalls)
			}
			if err := builder.Destroy(context.Background(), manifest); err != nil {
				t.Fatalf("Destroy: %v", err)
			}
			if err := builder.Destroy(context.Background(), manifest); err != nil {
				t.Fatalf("Destroy idempotent: %v", err)
			}
		})
	}
}

func TestBuildDynamicRecipeArtifactImageDoesNotPrepullOrBindStaticCatalog(t *testing.T) {
	artifact := artifactFixture(t)
	clock := &fakeClock{now: time.Date(2026, time.July, 16, 0, 0, 0, 0, time.UTC)}
	provider := &fakeProvider{artifact: artifact, images: map[string]ImageObservation{}}
	builder, _ := NewBuilder(provider, clock)
	builder.poll = time.Second
	config := buildConfigFixture(artifact)
	config.DynamicRecipeArtifacts = true
	provider.marker = successMarker(config.ArtifactVersion, artifact, true)

	manifest, err := builder.Build(context.Background(), config, artifact)
	if err != nil {
		t.Fatalf("Build dynamic image: %v", err)
	}
	if manifest.RecipeArtifactMode != RecipeArtifactDynamic || provider.launchSpec.Tags["dirextalk:recipe-artifact-mode"] != RecipeArtifactDynamic {
		t.Fatalf("dynamic mode not bound to manifest/tags: manifest=%#v tags=%#v", manifest, provider.launchSpec.Tags)
	}
	if !strings.HasSuffix(manifest.ImageName, "-dynamic") {
		t.Fatalf("dynamic image identity can collide with a static image: %q", manifest.ImageName)
	}
	manifestDocument := map[string]any{}
	rawManifest, _ := json.Marshal(manifest)
	if err := json.Unmarshal(rawManifest, &manifestDocument); err != nil {
		t.Fatal(err)
	}
	delete(manifestDocument, "recipe_artifact_mode")
	rawManifest, _ = json.Marshal(manifestDocument)
	if _, err := ParseImageManifest(rawManifest); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("manifest without explicit Recipe artifact mode was accepted: %v", err)
	}
	userData := provider.launchSpec.UserData
	for _, forbidden := range []string{"podman pull", "CLOUD_WORKER_OCI_CATALOG_FILE=", "CLOUD_WORKER_RESOURCE_MANIFEST_FILE="} {
		if strings.Contains(userData, forbidden) {
			t.Fatalf("dynamic image contains static binding %q", forbidden)
		}
	}
	for _, required := range []string{
		"CLOUD_WORKER_OCI_RECIPE_ENABLED=true",
		"CLOUD_WORKER_DYNAMIC_RECIPE_ARTIFACTS_ENABLED=true",
		"CLOUD_WORKER_RECIPE_CHECKPOINT_DIR=/var/lib/dirextalk-cloud-worker/checkpoints",
		"chown root:root /etc/dirextalk-cloud-worker/worker.env",
		"chmod 0600 /etc/dirextalk-cloud-worker/worker.env",
		"'" + strings.TrimPrefix(artifact.Catalog.WorkerBinaryDigest, "sha256:") + "' /opt/dirextalk-worker/cloud-worker | sha256sum -c -",
	} {
		if !strings.Contains(userData, required) {
			t.Fatalf("dynamic image is missing %q", required)
		}
	}
	image := provider.images[manifest.ImageName]
	image.Tags["dirextalk:recipe-artifact-mode"] = RecipeArtifactStatic
	provider.images[manifest.ImageName] = image
	if err := builder.Verify(context.Background(), manifest); !errors.Is(err, ErrBuildFailed) {
		t.Fatalf("Verify accepted static tag for dynamic image: %v", err)
	}
}

func TestValidateArchiveRejectsTamperAndSymlink(t *testing.T) {
	artifact := artifactFixture(t)
	if artifact.Catalog.ArtifactVersion != "v1.2.0-stage-t.1" || artifact.ImageDigest != digest("2") || artifact.Catalog.ImageSource != "ghcr.io/dirextalk/worker-fixture@"+artifact.ImageDigest {
		t.Fatal("valid archive binding missing")
	}
	raw, err := os.ReadFile(artifact.Path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 1
	tampered := filepath.Join(t.TempDir(), "tampered.tar")
	if err := os.WriteFile(tampered, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ValidateArchive(tampered); err == nil {
		t.Fatal("tampered archive accepted")
	}
	link := filepath.Join(filepath.Dir(tampered), "link.tar")
	if err := os.Symlink(artifact.Path, link); err == nil {
		if _, err := ValidateArchive(link); err == nil {
			t.Fatal("symlink archive accepted")
		}
	}
}

func TestBuildConfigRequiresUniquePrereleaseVersion(t *testing.T) {
	artifact := artifactFixture(t)
	for _, forbidden := range []string{"latest", "v1.0.3", "1.0.3"} {
		config := buildConfigFixture(artifact)
		config.ArtifactVersion = forbidden
		if err := config.Validate(artifact); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("version %q accepted: %v", forbidden, err)
		}
	}
}

type fakeClock struct{ now time.Time }

func (clock *fakeClock) Now() time.Time { return clock.now }
func (clock *fakeClock) After(duration time.Duration) <-chan time.Time {
	clock.now = clock.now.Add(duration)
	channel := make(chan time.Time, 1)
	channel <- clock.now
	return channel
}

type fakeProvider struct {
	artifact                                    ValidatedArtifact
	responseLoss                                bool
	marker                                      string
	builder                                     BuilderObservation
	images                                      map[string]ImageObservation
	launchSpec                                  LaunchSpec
	putCalls, deleteObjectCalls, terminateCalls int
}

func (provider *fakeProvider) PutArtifact(_ context.Context, _, _, _ string, file *os.File, size int64, digest string) (ArtifactUpload, error) {
	provider.putCalls++
	info, _ := file.Stat()
	if info.Size() != size || digest != provider.artifact.ArchiveSHA256 {
		return ArtifactUpload{}, ErrBuildFailed
	}
	return ArtifactUpload{VersionID: "version-0001"}, nil
}
func (provider *fakeProvider) PresignArtifactGET(context.Context, string, string, string, time.Duration) (string, error) {
	return "https://artifacts.example.invalid/worker.tar?X-Amz-Signature=redacted", nil
}
func (provider *fakeProvider) DeleteArtifact(context.Context, string, string, string) error {
	provider.deleteObjectCalls++
	return nil
}
func (provider *fakeProvider) FindBuilder(_ context.Context, name string) (BuilderObservation, bool, error) {
	if provider.builder.InstanceID == "" {
		return BuilderObservation{}, false, nil
	}
	return provider.builder, true, nil
}
func (provider *fakeProvider) LaunchBuilder(_ context.Context, spec LaunchSpec) (BuilderObservation, error) {
	provider.launchSpec = spec
	if strings.Contains(spec.UserData, "X-Amz-Signature=redacted") || !strings.Contains(spec.UserData, "EnvironmentFile=-/etc/dirextalk-cloud-worker/bootstrap.env") {
		return BuilderObservation{}, ErrBuildFailed
	}
	provider.builder = BuilderObservation{InstanceID: "i-0123456789abcdef0", Name: spec.Name, State: BuilderRunning}
	if provider.responseLoss {
		return BuilderObservation{}, errors.New("response lost")
	}
	return provider.builder, nil
}
func (provider *fakeProvider) ObserveBuilder(context.Context, string) (BuilderObservation, error) {
	provider.builder.State = BuilderStopped
	return provider.builder, nil
}
func (provider *fakeProvider) ConsoleOutput(context.Context, string) (string, error) {
	return "cloud-init\n" + provider.marker + "\n", nil
}
func (provider *fakeProvider) TerminateBuilder(context.Context, string) error {
	provider.terminateCalls++
	provider.builder.State = BuilderTerminated
	return nil
}
func (provider *fakeProvider) FindImageByName(_ context.Context, name string) (ImageObservation, bool, error) {
	image, ok := provider.images[name]
	return image, ok, nil
}
func (provider *fakeProvider) FindImageByID(_ context.Context, id string) (ImageObservation, bool, error) {
	for _, image := range provider.images {
		if image.ImageID == id {
			return image, true, nil
		}
	}
	return ImageObservation{}, false, nil
}
func (provider *fakeProvider) CreateImage(_ context.Context, _ string, name string, tags map[string]string) (string, error) {
	provider.images[name] = ImageObservation{ImageID: "ami-0123456789abcdef0", Name: name, State: "available", SnapshotIDs: []string{"snap-0123456789abcdef0"}, SnapshotsEncrypted: true, Tags: tags}
	if provider.responseLoss {
		return "", errors.New("response lost")
	}
	return "ami-0123456789abcdef0", nil
}
func (provider *fakeProvider) DeregisterImage(_ context.Context, id string) error {
	for name, image := range provider.images {
		if image.ImageID == id {
			delete(provider.images, name)
		}
	}
	return nil
}
func (provider *fakeProvider) DeleteSnapshot(_ context.Context, _ string, tags map[string]string) error {
	if tags["dirextalk:trusted-catalog-digest"] == "" {
		return ErrBuildFailed
	}
	return nil
}

func buildConfigFixture(artifact ValidatedArtifact) BuildConfig {
	return BuildConfig{Region: "us-east-1", BaseAMIID: "ami-0abcdef0123456789", SubnetID: "subnet-0abcdef0123456789", SecurityGroupID: "sg-0abcdef0123456789", Bucket: "dirextalk-worker-artifacts", ObjectKey: "worker/v1.2.0-stage-t.1/artifact.tar", ArtifactVersion: artifact.Catalog.ArtifactVersion, InstanceType: "m7i.large", OCISource: "ghcr.io/dirextalk/worker-fixture@" + artifact.ImageDigest, Timeout: 10 * time.Minute}
}

func artifactFixture(t *testing.T) ValidatedArtifact {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.tar")
	binary := []byte("worker-binary")
	action := compiledAction{Kind: "install", ActionID: "install_v1", RootRequired: true, TimeoutSeconds: 1800, CheckpointSequence: []string{"artifact_verified", "container_created", "container_started", "health_verified"}}
	imageSource := "ghcr.io/dirextalk/worker-fixture@" + digest("2")
	artifact := compiledArtifact{SchemaVersion: "dirextalk.compiled-recipe-artifact/v1", RecipeID: "recipe-0001", RecipeDigest: digest("1"), RecipeRevision: 2, ImageSource: imageSource, OfficialSourceArtifactDigests: []string{digest("3")}, Architecture: "amd64", Requirements: requirements{MinVCPU: 2, MinMemoryMiB: 4096, MinDiskGiB: 40, Architecture: "amd64"}, ArtifactDigest: digest("2"), MediaType: "application/vnd.oci.image.manifest.v1+json", SizeBytes: 1024, Actions: []compiledAction{action}, HealthContractDigest: digest("4"), LifecycleContractDigest: digest("5"), VolumeSlots: []volumeSlot{}, DataSlots: []volumeSlot{}, SecretSlots: []secretSlot{}}
	readiness := probe{Scheme: "http", Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: digest("6")}
	descriptor := bundle{SchemaVersion: "dirextalk.oci-service-bundle/v1", ImageSource: imageSource, ArtifactDigest: digest("2"), ImageDigest: digest("2"), ImageSizeBytes: 1024, Architecture: "amd64", Actions: []compiledAction{action}, Health: health{readiness, readiness, readiness}, HealthContractDigest: digest("4"), LifecycleContractDigest: digest("5")}
	bundleDigest, _ := digestCanonical(descriptor)
	oci := workerCatalog{SchemaVersion: "dirextalk.worker-oci-catalog/v1", Entries: []workerCatalogEntry{{ArtifactDigest: digest("2"), BundleDigest: bundleDigest, ActionIDs: []string{"install_v1"}, SecretTargets: []secretTarget{}, Descriptor: descriptor}}}
	digestDoc := workerCatalogDigestDocument{SchemaVersion: oci.SchemaVersion, Entries: []workerCatalogDigestEntry{{ArtifactDigest: digest("2"), BundleDigest: bundleDigest, ActionIDs: []string{"install_v1"}, SecretTargets: []secretTarget{}}}}
	ociDigest, _ := digestJSON(digestDoc)
	workerDigest := namedBytes(binary)
	manifest := workerManifest{SchemaVersion: "dirextalk.worker-resource-manifest/v1", WorkerBinaryDigest: workerDigest, CatalogDigest: ociDigest, RuntimeIdentity: RuntimeIdentity}
	manifestDigest, _ := digestJSON(manifest)
	artifact.WorkerResourceManifestDigest = manifestDigest
	normalizeArtifact(&artifact)
	artifactDigest, _ := digestCanonical(artifact)
	artifactRaw, _ := json.Marshal(artifact)
	ociRaw, _ := json.Marshal(oci)
	manifestRaw, _ := json.Marshal(manifest)
	files := []ArtifactFile{{"cloud-worker", workerDigest, uint64(len(binary)), 0o755}, {"compiled-recipe-artifact.json", namedBytes(artifactRaw), uint64(len(artifactRaw)), 0o644}, {"worker-oci-catalog.json", namedBytes(ociRaw), uint64(len(ociRaw)), 0o644}, {"worker-resource-manifest.json", namedBytes(manifestRaw), uint64(len(manifestRaw)), 0o644}}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	catalog := TrustedCatalog{SchemaVersion: TrustedCatalogSchema, ArtifactVersion: "v1.2.0-stage-t.1", RecipeID: "recipe-0001", RecipeDigest: digest("1"), RecipeRevision: 2, ImageSource: imageSource, CompiledRecipeArtifactDigest: artifactDigest, WorkerResourceManifestDigest: manifestDigest, WorkerOCICatalogDigest: ociDigest, WorkerBinaryDigest: workerDigest, RuntimeIdentity: RuntimeIdentity, Files: files}
	catalogRaw, _ := json.Marshal(catalog)
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := tar.NewWriter(file)
	payloads := []struct {
		name string
		mode int64
		raw  []byte
	}{{controllerCatalogPath, 0o644, catalogRaw}, {"compiled-recipe-artifact.json", 0o644, artifactRaw}, {"worker-oci-catalog.json", 0o644, ociRaw}, {"worker-resource-manifest.json", 0o644, manifestRaw}, {"cloud-worker", 0o755, binary}}
	for _, item := range payloads {
		if err := writer.WriteHeader(&tar.Header{Name: item.name, Mode: item.mode, Size: int64(len(item.raw)), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(item.raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	validated, err := ValidateArchive(path)
	if err != nil {
		t.Fatalf("ValidateArchive fixture: %v", err)
	}
	return validated
}
func digest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
func namedBytes(raw []byte) string   { sum := sha256.Sum256(raw); return namedSHA(sum[:]) }
