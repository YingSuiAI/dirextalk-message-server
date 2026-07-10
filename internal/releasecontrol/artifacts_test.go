package releasecontrol

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

func TestValidateReleaseArtifactsRejectsManifestWhitespace(t *testing.T) {
	manifest, index := validReleaseArtifactsForTest()
	manifest = append(manifest, '\n')
	if _, err := ValidateReleaseArtifacts(
		manifest,
		checksumForTest("release-manifest.json", manifest),
		index,
		checksumForTest("release-index.json", index),
	); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("error = %v, want canonical manifest rejection", err)
	}
}

func TestValidateReleaseArtifactsRejectsChecksumMismatchAndFilename(t *testing.T) {
	manifest, index := validReleaseArtifactsForTest()
	for _, test := range []struct {
		name             string
		manifestChecksum []byte
		indexChecksum    []byte
	}{
		{"manifest digest", []byte(strings.Repeat("0", 64) + "  release-manifest.json\n"), checksumForTest("release-index.json", index)},
		{"manifest filename", checksumForTest("wrong.json", manifest), checksumForTest("release-index.json", index)},
		{"index digest", checksumForTest("release-manifest.json", manifest), []byte(strings.Repeat("0", 64) + "  release-index.json\n")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateReleaseArtifacts(manifest, test.manifestChecksum, index, test.indexChecksum); err == nil || !strings.Contains(err.Error(), "checksum") {
				t.Fatalf("error = %v, want checksum rejection", err)
			}
		})
	}
}

func TestValidateReleaseArtifactsAcceptsCanonicalBoundAssets(t *testing.T) {
	manifest, index := validReleaseArtifactsForTest()
	parsed, err := ValidateReleaseArtifacts(
		manifest,
		checksumForTest("release-manifest.json", manifest),
		index,
		checksumForTest("release-index.json", index),
	)
	if err != nil {
		t.Fatalf("ValidateReleaseArtifacts: %v", err)
	}
	if parsed.LatestVersion != "v1.0.0" {
		t.Fatalf("latest_version = %q", parsed.LatestVersion)
	}
}

func validReleaseArtifactsForTest() ([]byte, []byte) {
	manifest := []byte(compactTestManifest("v1.0.0", "=0.15.2", strings.Repeat("a", 64)))
	index := []byte(testReleaseIndex(string(manifest), manifestSHA(string(manifest)), strings.Repeat("d", 64)))
	return manifest, index
}

func checksumForTest(name string, data []byte) []byte {
	digest := sha256.Sum256(data)
	return []byte(hex.EncodeToString(digest[:]) + "  " + name + "\n")
}
