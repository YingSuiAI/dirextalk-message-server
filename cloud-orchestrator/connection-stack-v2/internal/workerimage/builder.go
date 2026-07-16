package workerimage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type ArtifactUpload struct{ VersionID string }
type BuilderState string

const (
	BuilderPending    BuilderState = "pending"
	BuilderRunning    BuilderState = "running"
	BuilderStopped    BuilderState = "stopped"
	BuilderTerminated BuilderState = "terminated"
)

type BuilderObservation struct {
	InstanceID string
	Name       string
	State      BuilderState
}
type ImageObservation struct {
	ImageID            string
	Name               string
	State              string
	SnapshotIDs        []string
	SnapshotsEncrypted bool
	Tags               map[string]string
}
type LaunchSpec struct {
	Name, ClientToken, BaseAMIID, SubnetID, SecurityGroupID, InstanceType, UserData string
	Tags                                                                            map[string]string
}

type Provider interface {
	PutArtifact(context.Context, string, string, string, *os.File, int64, string) (ArtifactUpload, error)
	PresignArtifactGET(context.Context, string, string, string, time.Duration) (string, error)
	DeleteArtifact(context.Context, string, string, string) error
	FindBuilder(context.Context, string) (BuilderObservation, bool, error)
	LaunchBuilder(context.Context, LaunchSpec) (BuilderObservation, error)
	ObserveBuilder(context.Context, string) (BuilderObservation, error)
	ConsoleOutput(context.Context, string) (string, error)
	TerminateBuilder(context.Context, string) error
	FindImageByName(context.Context, string) (ImageObservation, bool, error)
	FindImageByID(context.Context, string) (ImageObservation, bool, error)
	CreateImage(context.Context, string, string, map[string]string) (string, error)
	DeregisterImage(context.Context, string) error
	DeleteSnapshot(context.Context, string, map[string]string) error
}

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now().UTC() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Builder struct {
	provider Provider
	clock    Clock
	poll     time.Duration
}

func NewBuilder(provider Provider, clock Clock) (*Builder, error) {
	if provider == nil {
		return nil, ErrInvalidConfig
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Builder{provider: provider, clock: clock, poll: 10 * time.Second}, nil
}

func (builder *Builder) Build(ctx context.Context, config BuildConfig, artifact ValidatedArtifact) (manifest ImageManifest, err error) {
	if builder == nil || builder.provider == nil || ctx == nil || config.Validate(artifact) != nil {
		return ImageManifest{}, ErrInvalidConfig
	}
	mode := recipeArtifactMode(config.DynamicRecipeArtifacts)
	name := imageName(config.ArtifactVersion, artifact.CatalogDigest, mode)
	tags := imageTags(config.ArtifactVersion, artifact, mode)
	deadline := builder.clock.Now().Add(config.Timeout)
	if existing, found, findErr := builder.provider.FindImageByName(ctx, name); findErr != nil {
		return ImageManifest{}, findErr
	} else if found {
		manifest, verifyErr := builder.manifestFromObservation(config, artifact, existing)
		if verifyErr != nil {
			return ImageManifest{}, verifyErr
		}
		if stale, staleFound, staleErr := builder.provider.FindBuilder(ctx, name+"-builder"); staleErr != nil {
			return ImageManifest{}, staleErr
		} else if staleFound {
			if terminateErr := builder.provider.TerminateBuilder(ctx, stale.InstanceID); terminateErr != nil {
				return ImageManifest{}, terminateErr
			}
		}
		return manifest, nil
	}

	var upload ArtifactUpload
	if config.ReleaseSource != nil {
		// Explicit release mode uses a previously verified immutable archive;
		// it must never re-upload or delete that retained release object.
		upload = ArtifactUpload{VersionID: config.ReleaseSource.VersionID}
	} else {
		file, openErr := os.Open(artifact.Path)
		if openErr != nil {
			return ImageManifest{}, openErr
		}
		defer file.Close()
		var putErr error
		upload, putErr = builder.provider.PutArtifact(ctx, config.Bucket, config.ObjectKey, config.ArtifactVersion, file, artifact.ArchiveSize, artifact.ArchiveSHA256)
		if putErr != nil {
			return ImageManifest{}, putErr
		}
		defer func() {
			if cleanupErr := builder.provider.DeleteArtifact(context.WithoutCancel(ctx), config.Bucket, config.ObjectKey, upload.VersionID); err == nil && cleanupErr != nil {
				err = cleanupErr
			}
		}()
	}
	url, presignErr := builder.provider.PresignArtifactGET(ctx, config.Bucket, config.ObjectKey, upload.VersionID, 15*time.Minute)
	if presignErr != nil {
		return ImageManifest{}, presignErr
	}
	userData, userErr := fixedUserData(url, config.OCISource, config.ArtifactVersion, artifact, config.DynamicRecipeArtifacts)
	if userErr != nil {
		return ImageManifest{}, userErr
	}
	builderName := name + "-builder"
	observation, found, findErr := builder.provider.FindBuilder(ctx, builderName)
	if findErr != nil {
		return ImageManifest{}, findErr
	}
	if !found {
		observation, err = builder.provider.LaunchBuilder(ctx, LaunchSpec{Name: builderName, ClientToken: clientToken(builderName), BaseAMIID: config.BaseAMIID, SubnetID: config.SubnetID, SecurityGroupID: config.SecurityGroupID, InstanceType: config.InstanceType, UserData: userData, Tags: tags})
		if err != nil {
			recovered, recoveredFound, recoverErr := builder.waitBuilderByName(ctx, builderName, recoveryDeadline(builder.clock.Now(), deadline))
			if recoverErr != nil || !recoveredFound {
				return ImageManifest{}, err
			}
			observation = recovered
		}
	}
	if observation.InstanceID == "" {
		return ImageManifest{}, ErrBuildFailed
	}
	defer func() {
		if cleanupErr := builder.provider.TerminateBuilder(context.WithoutCancel(ctx), observation.InstanceID); err == nil && cleanupErr != nil {
			err = cleanupErr
		}
	}()
	for observation.State != BuilderStopped {
		if observation.State == BuilderTerminated || !builder.clock.Now().Before(deadline) {
			return ImageManifest{}, ErrBuildFailed
		}
		select {
		case <-ctx.Done():
			return ImageManifest{}, ctx.Err()
		case <-builder.clock.After(builder.poll):
		}
		observation, err = builder.provider.ObserveBuilder(ctx, observation.InstanceID)
		if err != nil {
			return ImageManifest{}, err
		}
	}
	console, consoleErr := builder.provider.ConsoleOutput(ctx, observation.InstanceID)
	if consoleErr != nil || !strings.Contains(console, successMarker(config.ArtifactVersion, artifact, config.DynamicRecipeArtifacts)) {
		return ImageManifest{}, ErrBuildFailed
	}
	image, found, findErr := builder.provider.FindImageByName(ctx, name)
	if findErr != nil {
		return ImageManifest{}, findErr
	}
	if !found {
		imageID, createErr := builder.provider.CreateImage(ctx, observation.InstanceID, name, tags)
		if createErr != nil {
			recovered, recoveredFound, recoverErr := builder.waitImageByName(ctx, name, recoveryDeadline(builder.clock.Now(), deadline))
			if recoverErr != nil || !recoveredFound {
				return ImageManifest{}, createErr
			}
			image = recovered
		} else {
			image = ImageObservation{ImageID: imageID, Name: name, State: "pending", Tags: tags}
		}
	}
	for image.State != "available" {
		if image.State == "failed" || !builder.clock.Now().Before(deadline) {
			return ImageManifest{}, ErrBuildFailed
		}
		select {
		case <-ctx.Done():
			return ImageManifest{}, ctx.Err()
		case <-builder.clock.After(builder.poll):
		}
		image, found, err = builder.provider.FindImageByID(ctx, image.ImageID)
		if err != nil || !found {
			if err != nil {
				return ImageManifest{}, err
			}
			return ImageManifest{}, ErrBuildFailed
		}
	}
	return builder.manifestFromObservation(config, artifact, image)
}

func (builder *Builder) Verify(ctx context.Context, manifest ImageManifest) error {
	if builder == nil || ctx == nil || manifest.Validate() != nil {
		return ErrInvalidConfig
	}
	image, found, err := builder.provider.FindImageByID(ctx, manifest.ImageID)
	if err != nil {
		return err
	}
	if !found {
		return ErrBuildFailed
	}
	return verifyImage(image, manifest.ImageName, manifest.ArtifactVersion, manifest.TrustedCatalogDigest, manifest.ArchiveSHA256, manifest.RecipeArtifactMode, manifest.SnapshotIDs)
}
func (builder *Builder) Destroy(ctx context.Context, manifest ImageManifest) error {
	if builder == nil || ctx == nil || manifest.Validate() != nil {
		return ErrInvalidConfig
	}
	image, found, err := builder.provider.FindImageByID(ctx, manifest.ImageID)
	if err != nil {
		return err
	}
	if found {
		if err := verifyImage(image, manifest.ImageName, manifest.ArtifactVersion, manifest.TrustedCatalogDigest, manifest.ArchiveSHA256, manifest.RecipeArtifactMode, manifest.SnapshotIDs); err != nil {
			return err
		}
		if err := builder.provider.DeregisterImage(ctx, manifest.ImageID); err != nil {
			return err
		}
	}
	deadline := builder.clock.Now().Add(5 * time.Minute)
	expectedTags := imageTags(manifest.ArtifactVersion, ValidatedArtifact{CatalogDigest: manifest.TrustedCatalogDigest, ArchiveSHA256: manifest.ArchiveSHA256}, manifest.RecipeArtifactMode)
	for _, snapshot := range manifest.SnapshotIDs {
		for {
			err := builder.provider.DeleteSnapshot(ctx, snapshot, expectedTags)
			if err == nil {
				break
			}
			if !builder.clock.Now().Before(deadline) {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-builder.clock.After(builder.poll):
			}
		}
	}
	_, found, err = builder.provider.FindImageByID(ctx, manifest.ImageID)
	if err != nil {
		return err
	}
	if found {
		return ErrBuildFailed
	}
	return nil
}

func (builder *Builder) waitBuilderByName(ctx context.Context, name string, deadline time.Time) (BuilderObservation, bool, error) {
	for {
		observation, found, err := builder.provider.FindBuilder(ctx, name)
		if err == nil && found {
			return observation, true, nil
		}
		if !builder.clock.Now().Before(deadline) {
			return BuilderObservation{}, false, err
		}
		select {
		case <-ctx.Done():
			return BuilderObservation{}, false, ctx.Err()
		case <-builder.clock.After(builder.poll):
		}
	}
}
func (builder *Builder) waitImageByName(ctx context.Context, name string, deadline time.Time) (ImageObservation, bool, error) {
	for {
		observation, found, err := builder.provider.FindImageByName(ctx, name)
		if err == nil && found {
			return observation, true, nil
		}
		if !builder.clock.Now().Before(deadline) {
			return ImageObservation{}, false, err
		}
		select {
		case <-ctx.Done():
			return ImageObservation{}, false, ctx.Err()
		case <-builder.clock.After(builder.poll):
		}
	}
}
func recoveryDeadline(now, deadline time.Time) time.Time {
	candidate := now.Add(time.Minute)
	if deadline.Before(candidate) {
		return deadline
	}
	return candidate
}

func (builder *Builder) manifestFromObservation(config BuildConfig, artifact ValidatedArtifact, image ImageObservation) (ImageManifest, error) {
	mode := recipeArtifactMode(config.DynamicRecipeArtifacts)
	if err := verifyImage(image, imageName(config.ArtifactVersion, artifact.CatalogDigest, mode), config.ArtifactVersion, artifact.CatalogDigest, artifact.ArchiveSHA256, mode, nil); err != nil {
		return ImageManifest{}, err
	}
	snapshots := append([]string(nil), image.SnapshotIDs...)
	sort.Strings(snapshots)
	var sourceArchive = config.ReleaseSource
	if sourceArchive != nil {
		copied := *sourceArchive
		sourceArchive = &copied
	}
	manifest := ImageManifest{SchemaVersion: ImageManifestSchema, ArtifactVersion: config.ArtifactVersion, Region: config.Region, ImageID: image.ImageID, ImageName: image.Name, BaseAMIID: config.BaseAMIID, OCISource: config.OCISource, ArchiveSHA256: artifact.ArchiveSHA256, TrustedCatalogDigest: artifact.CatalogDigest, WorkerResourceManifestDigest: artifact.Catalog.WorkerResourceManifestDigest, WorkerOCICatalogDigest: artifact.Catalog.WorkerOCICatalogDigest, WorkerBinaryDigest: artifact.Catalog.WorkerBinaryDigest, RecipeArtifactMode: mode, SourceArchive: sourceArchive, SnapshotIDs: snapshots, CreatedAt: builder.clock.Now().UTC().Format(time.RFC3339Nano)}
	return manifest, manifest.Validate()
}

func verifyImage(image ImageObservation, name, version, catalogDigest, archiveDigest, mode string, wantSnapshots []string) error {
	if image.ImageID == "" || image.Name != name || image.State != "available" || !image.SnapshotsEncrypted || len(image.SnapshotIDs) == 0 {
		return ErrBuildFailed
	}
	for key, value := range imageTags(version, ValidatedArtifact{CatalogDigest: catalogDigest, ArchiveSHA256: archiveDigest}, mode) {
		if image.Tags[key] != value {
			return ErrBuildFailed
		}
	}
	if wantSnapshots != nil {
		left := append([]string(nil), image.SnapshotIDs...)
		right := append([]string(nil), wantSnapshots...)
		sort.Strings(left)
		sort.Strings(right)
		if strings.Join(left, ",") != strings.Join(right, ",") {
			return ErrBuildFailed
		}
	}
	return nil
}
func imageName(version, digest, mode string) string {
	name := "dirextalk-worker-" + strings.TrimPrefix(version, "v") + "-" + strings.TrimPrefix(digest, "sha256:")[:16]
	if mode == RecipeArtifactDynamic {
		return name + "-dynamic"
	}
	return name
}
func imageTags(version string, artifact ValidatedArtifact, mode string) map[string]string {
	return map[string]string{"dirextalk:managed": "true", "dirextalk:worker-image-version": version, "dirextalk:trusted-catalog-digest": artifact.CatalogDigest, "dirextalk:archive-digest": artifact.ArchiveSHA256, "dirextalk:recipe-artifact-mode": mode}
}
func clientToken(name string) string {
	sum := sha256.Sum256([]byte("dirextalk-worker-image/v1\x00" + name))
	return hex.EncodeToString(sum[:])
}
func successMarker(version string, artifact ValidatedArtifact, dynamic bool) string {
	marker := "DIREXTALK_WORKER_IMAGE_READY version=" + version + " catalog=" + artifact.CatalogDigest + " archive=" + artifact.ArchiveSHA256
	if dynamic {
		return marker + " mode=dynamic"
	}
	return marker
}

func recipeArtifactMode(dynamic bool) string {
	if dynamic {
		return RecipeArtifactDynamic
	}
	return RecipeArtifactStatic
}

func fixedUserData(url, ociSource, version string, artifact ValidatedArtifact, dynamic bool) (string, error) {
	if url == "" || strings.ContainsAny(url, "\r\n") || len(ociSourcePattern.FindStringSubmatch(ociSource)) != 3 {
		return "", ErrInvalidConfig
	}
	encodedURL := base64.StdEncoding.EncodeToString([]byte(url))
	marker := successMarker(version, artifact, dynamic)
	ociSetup := ""
	workerEnvironment := `CLOUD_WORKER_OCI_RECIPE_ENABLED=true
CLOUD_WORKER_DYNAMIC_RECIPE_ARTIFACTS_ENABLED=true
CLOUD_WORKER_RECIPE_CHECKPOINT_DIR=/var/lib/dirextalk-cloud-worker/checkpoints`
	if !dynamic {
		ociSetup = fmt.Sprintf("podman pull --quiet '%s'\ntest \"$(podman image inspect --format '{{.Digest}}' '%s')\" = '%s'", ociSource, ociSource, artifact.ImageDigest)
		workerEnvironment = `CLOUD_WORKER_OCI_RECIPE_ENABLED=true
CLOUD_WORKER_OCI_CATALOG_FILE=/opt/dirextalk-worker/worker-oci-catalog.json
CLOUD_WORKER_RESOURCE_MANIFEST_FILE=/opt/dirextalk-worker/worker-resource-manifest.json
CLOUD_WORKER_RECIPE_CHECKPOINT_DIR=/var/lib/dirextalk-cloud-worker/checkpoints`
	}
	script := fmt.Sprintf(`#!/bin/bash
set -euo pipefail
umask 077
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl podman
install -d -m 0755 /opt/dirextalk-worker /etc/dirextalk-cloud-worker /var/lib/dirextalk-cloud-worker/checkpoints
printf '%%s' '%s' | base64 -d > /tmp/dirextalk-artifact-url
curl --proto '=https' --tlsv1.2 --fail --location "$(cat /tmp/dirextalk-artifact-url)" -o /tmp/dirextalk-worker.tar
rm -f /tmp/dirextalk-artifact-url
printf '%%s  %%s\n' '%s' /tmp/dirextalk-worker.tar | sha256sum -c -
tar -xf /tmp/dirextalk-worker.tar -C /opt/dirextalk-worker --no-same-owner --no-same-permissions
printf '%%s  %%s\n' '%s' /opt/dirextalk-worker/cloud-worker | sha256sum -c -
install -m 0755 /opt/dirextalk-worker/cloud-worker /usr/local/bin/cloud-worker
%s
install -o root -g root -m 0600 /dev/null /etc/dirextalk-cloud-worker/worker.env
cat > /etc/dirextalk-cloud-worker/worker.env <<'EOF'
%s
EOF
chown root:root /etc/dirextalk-cloud-worker/worker.env
chmod 0600 /etc/dirextalk-cloud-worker/worker.env
cat > /etc/systemd/system/dirextalk-cloud-worker.service <<'EOF'
[Unit]
After=network-online.target
Wants=network-online.target
[Service]
Type=simple
EnvironmentFile=/etc/dirextalk-cloud-worker/worker.env
EnvironmentFile=-/etc/dirextalk-cloud-worker/bootstrap.env
ExecStart=/usr/local/bin/cloud-worker
Restart=on-failure
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
rm -f /tmp/dirextalk-worker.tar
printf '%%s\n' '%s' > /dev/ttyS0
shutdown -h now
`, encodedURL, strings.TrimPrefix(artifact.ArchiveSHA256, "sha256:"), strings.TrimPrefix(artifact.Catalog.WorkerBinaryDigest, "sha256:"), ociSetup, workerEnvironment, marker)
	return script, nil
}
