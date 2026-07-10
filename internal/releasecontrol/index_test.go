package releasecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestValidateReleaseIndexBindsEmbeddedManifestBytesAndExactLegacyDigest(t *testing.T) {
	manifest := compactTestManifest("v1.0.0", ">=0.15.2 <1.0.0", strings.Repeat("a", 64))
	index := testReleaseIndex(manifest, manifestSHA(manifest), strings.Repeat("d", 64))
	parsed, err := ValidateReleaseIndex([]byte(index))
	if err != nil {
		t.Fatalf("ValidateReleaseIndex: %v", err)
	}
	if parsed.LatestVersion != "v1.0.0" || len(parsed.Releases) != 1 || len(parsed.UpgradeEdges) != 1 {
		t.Fatalf("unexpected parsed index: %+v", parsed)
	}
}

func TestValidateReleaseIndexRejectsTamperedManifestDigest(t *testing.T) {
	manifest := compactTestManifest("v1.0.0", ">=0.15.2 <1.0.0", strings.Repeat("a", 64))
	index := testReleaseIndex(manifest, "sha256:"+strings.Repeat("f", 64), strings.Repeat("d", 64))
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "manifest_digest") {
		t.Fatalf("error = %v, want manifest_digest rejection", err)
	}
}

func TestValidateReleaseIndexRejectsMissingExactSourceDigest(t *testing.T) {
	manifest := compactTestManifest("v1.0.0", ">=0.15.2 <1.0.0", strings.Repeat("a", 64))
	index := testReleaseIndex(manifest, manifestSHA(manifest), "")
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "from_image_digests") {
		t.Fatalf("error = %v, want exact source digest rejection", err)
	}
}

func TestValidateReleaseIndexRejectsNonCanonicalEmbeddedManifest(t *testing.T) {
	manifest := strings.Replace(compactTestManifest("v1.0.0", ">=0.15.2 <1.0.0", strings.Repeat("a", 64)), `,"version"`, `, "version"`, 1)
	index := testReleaseIndex(manifest, manifestSHA(manifest), strings.Repeat("d", 64))
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "canonical compact JSON") {
		t.Fatalf("error = %v, want canonical encoding rejection", err)
	}
}

func TestValidateReleaseIndexRejectsTrailingWhitespace(t *testing.T) {
	manifest := compactTestManifest("v1.0.0", ">=0.15.2 <1.0.0", strings.Repeat("a", 64))
	index := testReleaseIndex(manifest, manifestSHA(manifest), strings.Repeat("d", 64)) + "\n"
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "canonical compact JSON") {
		t.Fatalf("error = %v, want whole-index canonical encoding rejection", err)
	}
}

func TestValidateReleaseIndexRejectsUnindexedSourceOutsideBootstrapEdge(t *testing.T) {
	manifest := compactTestManifest("v1.0.0", ">=0.14.0 <1.0.0", strings.Repeat("a", 64))
	index := strings.Replace(
		testReleaseIndex(manifest, manifestSHA(manifest), strings.Repeat("d", 64)),
		`"from_version":"v0.15.2"`,
		`"from_version":"v0.14.0"`,
		1,
	)
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("error = %v, want bootstrap edge rejection", err)
	}
}

func TestValidateReleaseIndexRejectsAmbiguousPathToLatest(t *testing.T) {
	v100 := compactTestManifest("v1.0.0", "=0.15.2", strings.Repeat("a", 64))
	v110 := compactTestManifest("v1.1.0", "=1.0.0", strings.Repeat("b", 64))
	v120 := compactTestManifest("v1.2.0", ">=1.0.0 <1.2.0", strings.Repeat("c", 64))
	index := compactIndexForTest(
		"v1.2.0",
		[]string{v100, v110, v120},
		`[{"from_version":"v0.15.2","from_image_digests":["sha256:`+strings.Repeat("d", 64)+`"],"to_version":"v1.0.0"},{"from_version":"v1.0.0","from_image_digests":["sha256:`+strings.Repeat("a", 64)+`"],"to_version":"v1.1.0"},{"from_version":"v1.0.0","from_image_digests":["sha256:`+strings.Repeat("a", 64)+`"],"to_version":"v1.2.0"},{"from_version":"v1.1.0","from_image_digests":["sha256:`+strings.Repeat("b", 64)+`"],"to_version":"v1.2.0"}]`,
	)
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("error = %v, want ambiguous path rejection", err)
	}
}

func TestValidateReleaseIndexRequiresPreviousReleasePathToLatest(t *testing.T) {
	v100 := compactTestManifest("v1.0.0", "=0.15.2", strings.Repeat("a", 64))
	v110 := compactTestManifest("v1.1.0", "=1.0.0", strings.Repeat("b", 64))
	v120 := compactTestManifest("v1.2.0", "=1.0.0", strings.Repeat("c", 64))
	index := compactIndexForTest(
		"v1.2.0",
		[]string{v100, v110, v120},
		`[{"from_version":"v0.15.2","from_image_digests":["sha256:`+strings.Repeat("d", 64)+`"],"to_version":"v1.0.0"},{"from_version":"v1.0.0","from_image_digests":["sha256:`+strings.Repeat("a", 64)+`"],"to_version":"v1.2.0"}]`,
	)
	if _, err := ValidateReleaseIndex([]byte(index)); err == nil || !strings.Contains(err.Error(), "previous release") {
		t.Fatalf("error = %v, want previous release path rejection", err)
	}
}

func compactTestManifest(version, upgradeFrom, digest string) string {
	manifest := Manifest{
		ManifestVersion:               1,
		Version:                       version,
		Image:                         "dirextalk/message-server:" + version,
		ImageDigest:                   "sha256:" + digest,
		UpgradeFrom:                   []string{upgradeFrom},
		SchemaVersion:                 1,
		SchemaCompatVersion:           1,
		MinimumClientVersion:          "v1.0.0",
		MaximumClientVersionExclusive: "v2.0.0",
		BackupRequired:                true,
		RollbackSupported:             true,
		RollbackMode:                  "restore_backup",
		ReleaseNotesURL:               "https://github.com/YingSuiAI/dirextalk-message-server/releases/tag/" + version,
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func manifestSHA(manifest string) string {
	digest := sha256.Sum256([]byte(manifest))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func testReleaseIndex(manifest, digest, sourceDigest string) string {
	digests := `[]`
	if sourceDigest != "" {
		digests = fmt.Sprintf(`["sha256:%s"]`, sourceDigest)
	}
	return fmt.Sprintf(`{"release_index_version":1,"latest_version":"v1.0.0","releases":[{"manifest":%s,"manifest_digest":%q}],"upgrade_edges":[{"from_version":"v0.15.2","from_image_digests":%s,"to_version":"v1.0.0"}]}`, manifest, digest, digests)
}

func compactIndexForTest(latest string, manifests []string, edges string) string {
	releases := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		releases = append(releases, fmt.Sprintf(`{"manifest":%s,"manifest_digest":%q}`, manifest, manifestSHA(manifest)))
	}
	return fmt.Sprintf(`{"release_index_version":1,"latest_version":%q,"releases":[%s],"upgrade_edges":%s}`, latest, strings.Join(releases, ","), edges)
}
