package releasecontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
)

const SupportedReleaseIndexVersion = 1

const (
	legacyBootstrapVersion = "v0.15.2"
	firstFormalVersion     = "v1.0.0"
)

type IndexedRelease struct {
	Manifest       Manifest `json:"manifest"`
	ManifestDigest string   `json:"manifest_digest"`
}

type UpgradeEdge struct {
	FromVersion      string   `json:"from_version"`
	FromImageDigests []string `json:"from_image_digests"`
	ToVersion        string   `json:"to_version"`
}

type ReleaseIndex struct {
	ReleaseIndexVersion int              `json:"release_index_version"`
	LatestVersion       string           `json:"latest_version"`
	Releases            []IndexedRelease `json:"releases"`
	UpgradeEdges        []UpgradeEdge    `json:"upgrade_edges"`
}

type rawIndexedRelease struct {
	Manifest       json.RawMessage `json:"manifest"`
	ManifestDigest string          `json:"manifest_digest"`
}

type rawReleaseIndex struct {
	ReleaseIndexVersion int                 `json:"release_index_version"`
	LatestVersion       string              `json:"latest_version"`
	Releases            []rawIndexedRelease `json:"releases"`
	UpgradeEdges        []UpgradeEdge       `json:"upgrade_edges"`
}

func ValidateReleaseIndex(data []byte) (ReleaseIndex, error) {
	var raw rawReleaseIndex
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return ReleaseIndex{}, fmt.Errorf("decode release index: %w", err)
	}
	if err := ensureIndexEOF(decoder); err != nil {
		return ReleaseIndex{}, err
	}
	if raw.ReleaseIndexVersion != SupportedReleaseIndexVersion {
		return ReleaseIndex{}, fmt.Errorf("release_index_version %d is not supported", raw.ReleaseIndexVersion)
	}
	if _, err := parseCanonicalVersion("latest_version", raw.LatestVersion); err != nil {
		return ReleaseIndex{}, err
	}
	if len(raw.Releases) == 0 {
		return ReleaseIndex{}, fmt.Errorf("releases must not be empty")
	}

	index := ReleaseIndex{
		ReleaseIndexVersion: raw.ReleaseIndexVersion,
		LatestVersion:       raw.LatestVersion,
		UpgradeEdges:        raw.UpgradeEdges,
		Releases:            make([]IndexedRelease, 0, len(raw.Releases)),
	}
	manifestByVersion := make(map[string]Manifest, len(raw.Releases))
	for position, release := range raw.Releases {
		manifestBytes := bytes.TrimSpace(release.Manifest)
		manifest, err := ValidateManifest(manifestBytes)
		if err != nil {
			return ReleaseIndex{}, fmt.Errorf("releases[%d].manifest is invalid: %w", position, err)
		}
		canonicalManifest, err := json.Marshal(manifest)
		if err != nil {
			return ReleaseIndex{}, fmt.Errorf("releases[%d].manifest cannot be encoded: %w", position, err)
		}
		if !bytes.Equal(manifestBytes, canonicalManifest) {
			return ReleaseIndex{}, fmt.Errorf("releases[%d].manifest must use canonical compact JSON", position)
		}
		digest := sha256.Sum256(manifestBytes)
		expectedDigest := "sha256:" + hex.EncodeToString(digest[:])
		if release.ManifestDigest != expectedDigest {
			return ReleaseIndex{}, fmt.Errorf("releases[%d].manifest_digest does not bind the embedded manifest bytes", position)
		}
		if _, duplicate := manifestByVersion[manifest.Version]; duplicate {
			return ReleaseIndex{}, fmt.Errorf("release version %s is duplicated", manifest.Version)
		}
		if position > 0 {
			previous, _ := parseCanonicalVersion("version", index.Releases[position-1].Manifest.Version)
			current, _ := parseCanonicalVersion("version", manifest.Version)
			if !previous.LessThan(current) {
				return ReleaseIndex{}, fmt.Errorf("releases must be in strict SemVer order")
			}
		}
		manifestByVersion[manifest.Version] = manifest
		index.Releases = append(index.Releases, IndexedRelease{Manifest: manifest, ManifestDigest: release.ManifestDigest})
	}
	if index.Releases[len(index.Releases)-1].Manifest.Version != index.LatestVersion {
		return ReleaseIndex{}, fmt.Errorf("latest_version must equal the final release version")
	}
	if len(index.UpgradeEdges) == 0 {
		return ReleaseIndex{}, fmt.Errorf("upgrade_edges must not be empty")
	}

	seenEdges := make(map[string]struct{}, len(index.UpgradeEdges))
	for position, edge := range index.UpgradeEdges {
		from, err := parseCanonicalVersion("from_version", edge.FromVersion)
		if err != nil {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d]: %w", position, err)
		}
		to, err := parseCanonicalVersion("to_version", edge.ToVersion)
		if err != nil {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d]: %w", position, err)
		}
		if !from.LessThan(to) {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d] must move forward", position)
		}
		target, ok := manifestByVersion[edge.ToVersion]
		if !ok {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d] target release is missing", position)
		}
		if err := target.ValidateUpgradeFrom(edge.FromVersion); err != nil {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d] violates target manifest: %w", position, err)
		}
		if len(edge.FromImageDigests) == 0 {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d].from_image_digests must not be empty", position)
		}
		for digestPosition, digest := range edge.FromImageDigests {
			if !digestPattern.MatchString(digest) {
				return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d].from_image_digests[%d] is invalid", position, digestPosition)
			}
			if digestPosition > 0 && edge.FromImageDigests[digestPosition-1] >= digest {
				return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d].from_image_digests must be sorted and unique", position)
			}
		}
		if source, formal := manifestByVersion[edge.FromVersion]; formal {
			if len(edge.FromImageDigests) != 1 || edge.FromImageDigests[0] != source.ImageDigest {
				return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d] must bind the formal source manifest digest", position)
			}
		} else if edge.ToVersion == index.Releases[0].Manifest.Version && target.BaselineResetFromVersion == edge.FromVersion {
			// A release-history reset has one exact, newly verified source. It is
			// explicit in the first manifest, so an unindexed source cannot be
			// mistaken for a normal upgrade edge.
		} else if edge.FromVersion != legacyBootstrapVersion || edge.ToVersion != firstFormalVersion {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges[%d] is not the unique supported bootstrap or declared baseline-reset edge", position)
		}
		key := edge.FromVersion + "\x00" + edge.ToVersion
		if _, duplicate := seenEdges[key]; duplicate {
			return ReleaseIndex{}, fmt.Errorf("upgrade edge %s -> %s is duplicated", edge.FromVersion, edge.ToVersion)
		}
		seenEdges[key] = struct{}{}
		if position > 0 && !upgradeEdgeLess(index.UpgradeEdges[position-1], edge) {
			return ReleaseIndex{}, fmt.Errorf("upgrade_edges must be in strict SemVer order")
		}
	}
	if len(index.Releases) > 1 {
		previous := index.Releases[len(index.Releases)-2].Manifest.Version
		if paths := countUpgradePaths(previous, index.LatestVersion, index.UpgradeEdges, make(map[string]int)); paths != 1 {
			return ReleaseIndex{}, fmt.Errorf("previous release %s must have one unique path to %s", previous, index.LatestVersion)
		}
	}
	pathSources := make(map[string]struct{}, len(index.Releases)+1)
	if baseline := index.Releases[0].Manifest.BaselineResetFromVersion; baseline != "" {
		pathSources[baseline] = struct{}{}
	} else {
		pathSources[legacyBootstrapVersion] = struct{}{}
	}
	for _, release := range index.Releases[:len(index.Releases)-1] {
		pathSources[release.Manifest.Version] = struct{}{}
	}
	for source := range pathSources {
		paths := countUpgradePaths(source, index.LatestVersion, index.UpgradeEdges, make(map[string]int))
		if paths > 1 {
			return ReleaseIndex{}, fmt.Errorf("upgrade path from %s to %s is ambiguous", source, index.LatestVersion)
		}
		if paths == 0 {
			return ReleaseIndex{}, fmt.Errorf("upgrade path from %s to %s is unsupported", source, index.LatestVersion)
		}
	}
	canonicalIndex, err := json.Marshal(index)
	if err != nil {
		return ReleaseIndex{}, fmt.Errorf("release index cannot be encoded: %w", err)
	}
	if !bytes.Equal(data, canonicalIndex) {
		return ReleaseIndex{}, fmt.Errorf("release index must use canonical compact JSON without surrounding whitespace")
	}
	return index, nil
}

func countUpgradePaths(from, target string, edges []UpgradeEdge, memo map[string]int) int {
	if from == target {
		return 1
	}
	if count, ok := memo[from]; ok {
		return count
	}
	count := 0
	for _, edge := range edges {
		if edge.FromVersion != from {
			continue
		}
		count += countUpgradePaths(edge.ToVersion, target, edges, memo)
		if count > 1 {
			count = 2
			break
		}
	}
	memo[from] = count
	return count
}

func upgradeEdgeLess(left, right UpgradeEdge) bool {
	leftFrom, _ := parseCanonicalVersion("from_version", left.FromVersion)
	rightFrom, _ := parseCanonicalVersion("from_version", right.FromVersion)
	if leftFrom.Equal(rightFrom) {
		leftTo, _ := parseCanonicalVersion("to_version", left.ToVersion)
		rightTo, _ := parseCanonicalVersion("to_version", right.ToVersion)
		return leftTo.LessThan(rightTo)
	}
	return leftFrom.LessThan(rightFrom)
}

func ensureIndexEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode release index: multiple JSON values")
		}
		return fmt.Errorf("decode release index: %w", err)
	}
	return nil
}
