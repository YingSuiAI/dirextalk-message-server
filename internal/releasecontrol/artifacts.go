package releasecontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

var checksumLinePattern = regexp.MustCompile(`^([0-9a-f]{64})  ([a-z0-9.-]+)\n$`)

// ValidateReleaseArtifacts verifies the exact bytes published as the four
// immutable GitHub Release assets. It intentionally does not trim input.
func ValidateReleaseArtifacts(manifestData, manifestChecksum, indexData, indexChecksum []byte) (ReleaseIndex, error) {
	if err := validateChecksumAsset("release-manifest.json", manifestData, manifestChecksum); err != nil {
		return ReleaseIndex{}, err
	}
	if err := validateChecksumAsset("release-index.json", indexData, indexChecksum); err != nil {
		return ReleaseIndex{}, err
	}
	manifest, err := ValidateManifest(manifestData)
	if err != nil {
		return ReleaseIndex{}, fmt.Errorf("validate manifest: %w", err)
	}
	canonicalManifest, err := json.Marshal(manifest)
	if err != nil {
		return ReleaseIndex{}, fmt.Errorf("encode manifest: %w", err)
	}
	if !bytes.Equal(manifestData, canonicalManifest) {
		return ReleaseIndex{}, fmt.Errorf("release manifest must use canonical compact JSON without surrounding whitespace")
	}
	index, err := ValidateReleaseIndex(indexData)
	if err != nil {
		return ReleaseIndex{}, fmt.Errorf("validate index: %w", err)
	}
	latest := index.Releases[len(index.Releases)-1]
	digest := sha256.Sum256(manifestData)
	manifestDigest := "sha256:" + hex.EncodeToString(digest[:])
	if latest.Manifest.Version != manifest.Version || latest.ManifestDigest != manifestDigest {
		return ReleaseIndex{}, fmt.Errorf("manifest asset does not equal the latest indexed release")
	}
	latestBytes, err := json.Marshal(latest.Manifest)
	if err != nil || !bytes.Equal(latestBytes, manifestData) {
		return ReleaseIndex{}, fmt.Errorf("manifest asset bytes do not equal the latest indexed manifest")
	}
	return index, nil
}

func validateChecksumAsset(name string, data, checksum []byte) error {
	matches := checksumLinePattern.FindSubmatch(checksum)
	if matches == nil || string(matches[2]) != name {
		return fmt.Errorf("%s checksum file must contain one canonical line", name)
	}
	digest := sha256.Sum256(data)
	if string(matches[1]) != hex.EncodeToString(digest[:]) {
		return fmt.Errorf("%s checksum mismatch", name)
	}
	return nil
}
