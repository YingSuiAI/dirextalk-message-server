package recipeexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	WorkerOCICatalogV1Schema         = "dirextalk.worker-oci-catalog/v1"
	WorkerResourceManifestV1Schema   = "dirextalk.worker-resource-manifest/v1"
	WorkerRuntimeIdentityPodmanV1    = "dirextalk-podman-v1"
	maxWorkerOCICatalogBytes         = 1 << 20
	maxWorkerResourceManifestBytes   = 16 << 10
	maxWorkerOCICatalogEntries       = 128
	maxWorkerOCICatalogSecretTargets = 64
)

var (
	ErrWorkerOCICatalogInvalid        = errors.New("worker OCI catalog is invalid")
	ErrWorkerResourceManifestInvalid  = errors.New("worker resource manifest is invalid")
	ErrWorkerResourceManifestMismatch = errors.New("worker resource manifest binding does not match")
	ErrWorkerOCIBundleNotFound        = errors.New("worker OCI bundle is not registered")
)

// WorkerOCICatalogEntryV1 is a closed AMI-owned capability binding. The
// descriptor is data for the typed OCI driver, not an arbitrary executable
// path, image tag, URL, or command supplied by a task.
type WorkerOCICatalogEntryV1 struct {
	ArtifactDigest string                               `json:"artifact_digest"`
	BundleDigest   string                               `json:"bundle_digest"`
	ActionIDs      []string                             `json:"action_ids"`
	SecretTargets  []SecretTarget                       `json:"-"`
	Descriptor     cloudorchestrator.OCIServiceBundleV1 `json:"descriptor"`
}

type WorkerOCICatalogV1 struct {
	SchemaVersion string                    `json:"schema_version"`
	Entries       []WorkerOCICatalogEntryV1 `json:"entries"`
}

// WorkerResourceManifestV1 binds the separately measured Worker binary to an
// immutable catalog and one fixed runtime implementation. Its digest is the
// value carried by bootstrap and approved execution manifests.
type WorkerResourceManifestV1 struct {
	SchemaVersion      string `json:"schema_version"`
	WorkerBinaryDigest string `json:"worker_binary_digest"`
	CatalogDigest      string `json:"catalog_digest"`
	RuntimeIdentity    string `json:"runtime_identity"`
}

type OCICatalogResolver struct {
	bundles                      map[string]Bundle
	descriptors                  map[string]cloudorchestrator.OCIServiceBundleV1
	catalogDigest                string
	workerResourceManifestDigest string
}

var _ BundleResolver = (*OCICatalogResolver)(nil)

func (catalog WorkerOCICatalogV1) Validate() error {
	_, err := normalizeWorkerOCICatalog(catalog)
	return err
}

func (catalog WorkerOCICatalogV1) Digest() (string, error) {
	normalized, err := normalizeWorkerOCICatalog(catalog)
	if err != nil {
		return "", err
	}
	canonical := workerOCICatalogDigestDocument{
		SchemaVersion: normalized.SchemaVersion,
		Entries:       make([]workerOCICatalogDigestEntry, len(normalized.Entries)),
	}
	for index, entry := range normalized.Entries {
		canonical.Entries[index] = workerOCICatalogDigestEntry{
			ArtifactDigest: entry.ArtifactDigest,
			BundleDigest:   entry.BundleDigest,
			ActionIDs:      append([]string(nil), entry.ActionIDs...),
			SecretTargets:  secretTargetsToJSON(entry.SecretTargets),
		}
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return "", ErrWorkerOCICatalogInvalid
	}
	return digestWorkerDocument(raw), nil
}

func (manifest WorkerResourceManifestV1) Validate() error {
	if manifest.SchemaVersion != WorkerResourceManifestV1Schema ||
		!validTaskDigest(manifest.WorkerBinaryDigest) ||
		!validTaskDigest(manifest.CatalogDigest) ||
		manifest.RuntimeIdentity != WorkerRuntimeIdentityPodmanV1 {
		return ErrWorkerResourceManifestInvalid
	}
	return nil
}

func (manifest WorkerResourceManifestV1) Digest() (string, error) {
	if err := manifest.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(workerResourceManifestDigestDocument(manifest))
	if err != nil {
		return "", ErrWorkerResourceManifestInvalid
	}
	return digestWorkerDocument(raw), nil
}

func NewOCICatalogResolver(catalog WorkerOCICatalogV1, manifest WorkerResourceManifestV1, approvedManifestDigest, runningWorkerBinaryDigest string) (*OCICatalogResolver, error) {
	normalized, err := normalizeWorkerOCICatalog(catalog)
	if err != nil {
		return nil, err
	}
	catalogDigest, err := normalized.Digest()
	if err != nil {
		return nil, err
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if manifest.CatalogDigest != catalogDigest {
		return nil, ErrWorkerResourceManifestMismatch
	}
	manifestDigest, err := manifest.Digest()
	if err != nil {
		return nil, err
	}
	if !validTaskDigest(approvedManifestDigest) || approvedManifestDigest != manifestDigest {
		return nil, ErrWorkerResourceManifestMismatch
	}
	if runningWorkerBinaryDigest != "" && (!validTaskDigest(runningWorkerBinaryDigest) || runningWorkerBinaryDigest != manifest.WorkerBinaryDigest) {
		return nil, ErrWorkerResourceManifestMismatch
	}

	resolver := &OCICatalogResolver{
		bundles:                      make(map[string]Bundle, len(normalized.Entries)),
		descriptors:                  make(map[string]cloudorchestrator.OCIServiceBundleV1, len(normalized.Entries)),
		catalogDigest:                catalogDigest,
		workerResourceManifestDigest: manifestDigest,
	}
	for _, entry := range normalized.Entries {
		resolver.bundles[entry.ArtifactDigest] = Bundle{
			ArtifactDigest: entry.ArtifactDigest,
			ActionIDs:      append([]string(nil), entry.ActionIDs...),
			SecretTargets:  append([]SecretTarget(nil), entry.SecretTargets...),
		}
		resolver.descriptors[entry.ArtifactDigest] = cloneOCIServiceDescriptor(entry.Descriptor)
	}
	return resolver, nil
}

func (resolver *OCICatalogResolver) Resolve(ctx context.Context, artifactDigest string) (Bundle, error) {
	if ctx == nil || resolver == nil {
		return Bundle{}, ErrWorkerOCICatalogInvalid
	}
	if err := ctx.Err(); err != nil {
		return Bundle{}, err
	}
	if !validTaskDigest(artifactDigest) {
		return Bundle{}, ErrWorkerOCIBundleNotFound
	}
	bundle, found := resolver.bundles[artifactDigest]
	if !found {
		return Bundle{}, ErrWorkerOCIBundleNotFound
	}
	return cloneBundle(bundle), nil
}

func (resolver *OCICatalogResolver) LookupDescriptor(ctx context.Context, artifactDigest string) (cloudorchestrator.OCIServiceBundleV1, error) {
	if ctx == nil || resolver == nil {
		return cloudorchestrator.OCIServiceBundleV1{}, ErrWorkerOCICatalogInvalid
	}
	if err := ctx.Err(); err != nil {
		return cloudorchestrator.OCIServiceBundleV1{}, err
	}
	if !validTaskDigest(artifactDigest) {
		return cloudorchestrator.OCIServiceBundleV1{}, ErrWorkerOCIBundleNotFound
	}
	descriptor, found := resolver.descriptors[artifactDigest]
	if !found {
		return cloudorchestrator.OCIServiceBundleV1{}, ErrWorkerOCIBundleNotFound
	}
	return cloneOCIServiceDescriptor(descriptor), nil
}

func (resolver *OCICatalogResolver) CatalogDigest() string {
	if resolver == nil {
		return ""
	}
	return resolver.catalogDigest
}

func (resolver *OCICatalogResolver) WorkerResourceManifestDigest() string {
	if resolver == nil {
		return ""
	}
	return resolver.workerResourceManifestDigest
}

func ParseWorkerOCICatalogV1(raw []byte) (WorkerOCICatalogV1, error) {
	if len(raw) == 0 || len(raw) > maxWorkerOCICatalogBytes {
		return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
	}
	top, err := exactWorkerJSONObject(raw, []string{"schema_version", "entries"})
	if err != nil {
		return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
	}
	var catalog WorkerOCICatalogV1
	if json.Unmarshal(top["schema_version"], &catalog.SchemaVersion) != nil {
		return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
	}
	var rawEntries []json.RawMessage
	if json.Unmarshal(top["entries"], &rawEntries) != nil {
		return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
	}
	catalog.Entries = make([]WorkerOCICatalogEntryV1, len(rawEntries))
	for index, rawEntry := range rawEntries {
		entry, parseErr := parseWorkerOCICatalogEntry(rawEntry)
		if parseErr != nil {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		catalog.Entries[index] = entry
	}
	normalized, err := normalizeWorkerOCICatalog(catalog)
	if err != nil {
		return WorkerOCICatalogV1{}, err
	}
	return normalized, nil
}

func ParseWorkerResourceManifestV1(raw []byte) (WorkerResourceManifestV1, error) {
	if len(raw) == 0 || len(raw) > maxWorkerResourceManifestBytes {
		return WorkerResourceManifestV1{}, ErrWorkerResourceManifestInvalid
	}
	if _, err := exactWorkerJSONObject(raw, []string{"schema_version", "worker_binary_digest", "catalog_digest", "runtime_identity"}); err != nil {
		return WorkerResourceManifestV1{}, ErrWorkerResourceManifestInvalid
	}
	var manifest WorkerResourceManifestV1
	if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Validate() != nil {
		return WorkerResourceManifestV1{}, ErrWorkerResourceManifestInvalid
	}
	return manifest, nil
}

func normalizeWorkerOCICatalog(catalog WorkerOCICatalogV1) (WorkerOCICatalogV1, error) {
	if catalog.SchemaVersion != WorkerOCICatalogV1Schema || len(catalog.Entries) == 0 || len(catalog.Entries) > maxWorkerOCICatalogEntries {
		return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
	}
	normalized := WorkerOCICatalogV1{SchemaVersion: catalog.SchemaVersion, Entries: make([]WorkerOCICatalogEntryV1, len(catalog.Entries))}
	seenArtifacts := make(map[string]struct{}, len(catalog.Entries))
	seenBundles := make(map[string]struct{}, len(catalog.Entries))
	for index, candidate := range catalog.Entries {
		if !validTaskDigest(candidate.ArtifactDigest) || !validTaskDigest(candidate.BundleDigest) || candidate.Descriptor.Validate() != nil || candidate.Descriptor.ArtifactDigest != candidate.ArtifactDigest {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		descriptorDigest, err := candidate.Descriptor.Digest()
		if err != nil || descriptorDigest != candidate.BundleDigest {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		if _, duplicate := seenArtifacts[candidate.ArtifactDigest]; duplicate {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		if _, duplicate := seenBundles[candidate.BundleDigest]; duplicate {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		seenArtifacts[candidate.ArtifactDigest], seenBundles[candidate.BundleDigest] = struct{}{}, struct{}{}

		actionIDs, err := normalizeWorkerActionIDs(candidate.ActionIDs)
		if err != nil || !sameWorkerActionSet(actionIDs, candidate.Descriptor.Actions) {
			return WorkerOCICatalogV1{}, ErrWorkerOCICatalogInvalid
		}
		secretTargets, err := normalizeWorkerSecretTargets(candidate.SecretTargets)
		if err != nil {
			return WorkerOCICatalogV1{}, err
		}
		normalized.Entries[index] = WorkerOCICatalogEntryV1{
			ArtifactDigest: candidate.ArtifactDigest,
			BundleDigest:   candidate.BundleDigest,
			ActionIDs:      actionIDs,
			SecretTargets:  secretTargets,
			Descriptor:     cloneOCIServiceDescriptor(candidate.Descriptor),
		}
	}
	sort.Slice(normalized.Entries, func(i, j int) bool {
		return normalized.Entries[i].ArtifactDigest < normalized.Entries[j].ArtifactDigest
	})
	return normalized, nil
}

func normalizeWorkerActionIDs(actionIDs []string) ([]string, error) {
	if len(actionIDs) == 0 || len(actionIDs) > 32 {
		return nil, ErrWorkerOCICatalogInvalid
	}
	normalized := append([]string(nil), actionIDs...)
	sort.Strings(normalized)
	for index, actionID := range normalized {
		if !validBindingIdentifier(actionID) || index > 0 && normalized[index-1] == actionID {
			return nil, ErrWorkerOCICatalogInvalid
		}
	}
	return normalized, nil
}

func sameWorkerActionSet(actionIDs []string, actions []cloudorchestrator.CompiledRecipeActionV1) bool {
	if len(actionIDs) != len(actions) {
		return false
	}
	descriptorActionIDs := make([]string, len(actions))
	for index, action := range actions {
		descriptorActionIDs[index] = action.ActionID
	}
	sort.Strings(descriptorActionIDs)
	for index := range actionIDs {
		if actionIDs[index] != descriptorActionIDs[index] {
			return false
		}
	}
	return true
}

func normalizeWorkerSecretTargets(targets []SecretTarget) ([]SecretTarget, error) {
	if len(targets) > maxWorkerOCICatalogSecretTargets {
		return nil, ErrWorkerOCICatalogInvalid
	}
	normalized := append([]SecretTarget(nil), targets...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].SlotID < normalized[j].SlotID })
	for index, target := range normalized {
		// The podman-v1 runtime never persists service secrets in container
		// metadata. Every catalog secret must be a fixed read-only file target.
		if !validBindingIdentifier(target.SlotID) || target.EnvironmentKey != "" || target.FileName == "" || !validSecretDestination(target) || index > 0 && normalized[index-1].SlotID == target.SlotID {
			return nil, ErrWorkerOCICatalogInvalid
		}
	}
	return normalized, nil
}

func cloneOCIServiceDescriptor(descriptor cloudorchestrator.OCIServiceBundleV1) cloudorchestrator.OCIServiceBundleV1 {
	clone := descriptor
	clone.Actions = append([]cloudorchestrator.CompiledRecipeActionV1(nil), descriptor.Actions...)
	for index := range clone.Actions {
		clone.Actions[index].CheckpointSequence = append([]string(nil), descriptor.Actions[index].CheckpointSequence...)
	}
	return clone
}

type workerSecretTargetJSON struct {
	SlotID         string `json:"slot_id"`
	FileName       string `json:"file_name"`
	EnvironmentKey string `json:"environment_key"`
}

type workerOCICatalogEntryJSON struct {
	ArtifactDigest string                               `json:"artifact_digest"`
	BundleDigest   string                               `json:"bundle_digest"`
	ActionIDs      []string                             `json:"action_ids"`
	SecretTargets  []workerSecretTargetJSON             `json:"secret_targets"`
	Descriptor     cloudorchestrator.OCIServiceBundleV1 `json:"descriptor"`
}

type workerOCICatalogJSON struct {
	SchemaVersion string                      `json:"schema_version"`
	Entries       []workerOCICatalogEntryJSON `json:"entries"`
}

type workerOCICatalogDigestEntry struct {
	ArtifactDigest string                   `json:"artifact_digest"`
	BundleDigest   string                   `json:"bundle_digest"`
	ActionIDs      []string                 `json:"action_ids"`
	SecretTargets  []workerSecretTargetJSON `json:"secret_targets"`
}

type workerOCICatalogDigestDocument struct {
	SchemaVersion string                        `json:"schema_version"`
	Entries       []workerOCICatalogDigestEntry `json:"entries"`
}

type workerResourceManifestDigestDocument WorkerResourceManifestV1

func (catalog WorkerOCICatalogV1) MarshalJSON() ([]byte, error) {
	wire := workerOCICatalogJSON{SchemaVersion: catalog.SchemaVersion, Entries: make([]workerOCICatalogEntryJSON, len(catalog.Entries))}
	for index, entry := range catalog.Entries {
		wire.Entries[index] = workerOCICatalogEntryJSON{
			ArtifactDigest: entry.ArtifactDigest,
			BundleDigest:   entry.BundleDigest,
			ActionIDs:      append([]string(nil), entry.ActionIDs...),
			SecretTargets:  secretTargetsToJSON(entry.SecretTargets),
			Descriptor:     cloneOCIServiceDescriptor(entry.Descriptor),
		}
	}
	return json.Marshal(wire)
}

func secretTargetsToJSON(targets []SecretTarget) []workerSecretTargetJSON {
	wire := make([]workerSecretTargetJSON, len(targets))
	for index, target := range targets {
		wire[index] = workerSecretTargetJSON{SlotID: target.SlotID, FileName: target.FileName, EnvironmentKey: target.EnvironmentKey}
	}
	return wire
}

func parseWorkerOCICatalogEntry(raw []byte) (WorkerOCICatalogEntryV1, error) {
	fields, err := exactWorkerJSONObject(raw, []string{"artifact_digest", "bundle_digest", "action_ids", "secret_targets", "descriptor"})
	if err != nil {
		return WorkerOCICatalogEntryV1{}, err
	}
	var entry WorkerOCICatalogEntryV1
	if json.Unmarshal(fields["artifact_digest"], &entry.ArtifactDigest) != nil ||
		json.Unmarshal(fields["bundle_digest"], &entry.BundleDigest) != nil ||
		json.Unmarshal(fields["action_ids"], &entry.ActionIDs) != nil {
		return WorkerOCICatalogEntryV1{}, ErrWorkerOCICatalogInvalid
	}
	var rawTargets []json.RawMessage
	if json.Unmarshal(fields["secret_targets"], &rawTargets) != nil {
		return WorkerOCICatalogEntryV1{}, ErrWorkerOCICatalogInvalid
	}
	entry.SecretTargets = make([]SecretTarget, len(rawTargets))
	for index, rawTarget := range rawTargets {
		targetFields, targetErr := exactWorkerJSONObject(rawTarget, []string{"slot_id", "file_name", "environment_key"})
		if targetErr != nil {
			return WorkerOCICatalogEntryV1{}, targetErr
		}
		if json.Unmarshal(targetFields["slot_id"], &entry.SecretTargets[index].SlotID) != nil ||
			json.Unmarshal(targetFields["file_name"], &entry.SecretTargets[index].FileName) != nil ||
			json.Unmarshal(targetFields["environment_key"], &entry.SecretTargets[index].EnvironmentKey) != nil {
			return WorkerOCICatalogEntryV1{}, ErrWorkerOCICatalogInvalid
		}
	}
	descriptor, err := cloudorchestrator.ParseOCIServiceBundleV1(fields["descriptor"])
	if err != nil {
		return WorkerOCICatalogEntryV1{}, ErrWorkerOCICatalogInvalid
	}
	entry.Descriptor = descriptor
	return entry, nil
}

func exactWorkerJSONObject(raw []byte, expected []string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrWorkerOCICatalogInvalid
	}
	values := make(map[string]json.RawMessage, len(expected))
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		key, ok := keyToken.(string)
		if tokenErr != nil || !ok {
			return nil, ErrWorkerOCICatalogInvalid
		}
		if _, duplicate := values[key]; duplicate {
			return nil, ErrWorkerOCICatalogInvalid
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, ErrWorkerOCICatalogInvalid
		}
		values[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrWorkerOCICatalogInvalid
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, ErrWorkerOCICatalogInvalid
	}
	if len(values) != len(expected) {
		return nil, ErrWorkerOCICatalogInvalid
	}
	for _, field := range expected {
		if _, found := values[field]; !found {
			return nil, ErrWorkerOCICatalogInvalid
		}
	}
	return values, nil
}

func digestWorkerDocument(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
