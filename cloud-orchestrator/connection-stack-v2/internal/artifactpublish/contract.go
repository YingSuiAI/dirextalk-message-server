// Package artifactpublish defines the only artifact locations that a
// Connection Stack publisher may create or consume.  It deliberately does
// not accept a caller-provided S3 path, media type, or encryption setting.
package artifactpublish

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strings"
)

const (
	BindingSchema = "dirextalk.immutable-artifact-binding/v1"

	KindBrokerZIP          ArtifactKind = "broker_zip"
	KindWorkerArchive      ArtifactKind = "worker_archive"
	KindConnectionTemplate ArtifactKind = "connection_stack_template"

	BrokerZIPContentType          = "application/zip"
	WorkerArchiveContentType      = "application/x-tar"
	ConnectionTemplateContentType = "application/x-yaml"

	MaxBrokerZIPBytes          int64 = 256 << 20
	MaxWorkerArchiveBytes      int64 = 1 << 30
	MaxConnectionTemplateBytes int64 = 1 << 20
)

var (
	ErrInvalidPolicy     = errors.New("immutable artifact policy is invalid")
	ErrInvalidDescriptor = errors.New("immutable artifact descriptor is invalid")
	ErrInvalidBinding    = errors.New("immutable artifact binding is invalid")
	ErrVerification      = errors.New("immutable artifact verification failed")
	ErrProvider          = errors.New("immutable artifact provider is unavailable")

	bucketPattern    = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])$`)
	kmsKeyPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:/_.-]{1,511}$`)
	regionPattern    = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?$`)
	digestPattern    = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	versionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+/=-]*$`)
	buildVersionRE   = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*$`)
)

// ArtifactKind is intentionally closed: only the Connection Stack broker
// archive and the signed Worker archive may be published through this path.
type ArtifactKind string

// Policy is deployment configuration, not a request shape.  One policy
// fixes the only bucket and KMS key a publisher may use.
type Policy struct {
	Bucket   string `json:"bucket"`
	KMSKeyID string `json:"kms_key_id"`
}

func (policy Policy) Validate() error {
	if !bucketPattern.MatchString(policy.Bucket) || strings.Contains(policy.Bucket, "..") || !kmsKeyPattern.MatchString(policy.KMSKeyID) {
		return ErrInvalidPolicy
	}
	return nil
}

// ArtifactDescriptor contains only immutable release facts.  Content type,
// object key, bucket, and KMS key are derived by the publisher.
type ArtifactDescriptor struct {
	Kind      ArtifactKind
	Version   string
	SHA256    string
	SizeBytes int64
}

func (descriptor ArtifactDescriptor) Validate() error {
	if !validKind(descriptor.Kind) || !validBuildVersion(descriptor.Version) || !digestPattern.MatchString(descriptor.SHA256) || descriptor.SizeBytes < 1 || descriptor.SizeBytes > maxSize(descriptor.Kind) {
		return ErrInvalidDescriptor
	}
	return nil
}

// Binding is the portable, version-pinned reference accepted by a Worker or
// deployment plan.  It has no URL, tag alias, prefix, or arbitrary metadata.
type Binding struct {
	Schema      string       `json:"schema"`
	Kind        ArtifactKind `json:"kind"`
	Version     string       `json:"version"`
	Bucket      string       `json:"bucket"`
	Key         string       `json:"key"`
	VersionID   string       `json:"version_id"`
	SHA256      string       `json:"sha256"`
	SizeBytes   int64        `json:"size_bytes"`
	ContentType string       `json:"content_type"`
	KMSKeyID    string       `json:"kms_key_id"`
}

// BrokerArtifactReference is the exact immutable broker ZIP reference a
// Connection Stack may pass to CloudFormation.  In particular, VersionID is
// mandatory; a bucket/key pair alone is never an executable broker artifact.
type BrokerArtifactReference struct{ Binding }

func NewBrokerArtifactReference(policy Policy, version, sha256 string, sizeBytes int64, versionID string) (BrokerArtifactReference, error) {
	binding, err := NewBinding(policy, ArtifactDescriptor{Kind: KindBrokerZIP, Version: version, SHA256: sha256, SizeBytes: sizeBytes}, versionID)
	if err != nil {
		return BrokerArtifactReference{}, err
	}
	return BrokerArtifactReference{Binding: binding}, nil
}

func ParseBrokerArtifactReference(raw []byte, policy Policy) (BrokerArtifactReference, error) {
	binding, err := ParseBinding(raw, policy)
	if err != nil || binding.Kind != KindBrokerZIP {
		return BrokerArtifactReference{}, ErrInvalidBinding
	}
	return BrokerArtifactReference{Binding: binding}, nil
}

func (reference BrokerArtifactReference) ValidateFor(policy Policy) error {
	if reference.Kind != KindBrokerZIP {
		return ErrInvalidBinding
	}
	return reference.Binding.ValidateFor(policy)
}

// ConnectionTemplateReference pins the reviewed CloudFormation YAML to the
// exact S3 object version CloudFormation must fetch. A bare HTTPS URL would
// leave a mutable time-of-check/time-of-use gap between plan approval and
// CreateStack.
type ConnectionTemplateReference struct{ Binding }

func NewConnectionTemplateReference(policy Policy, version, sha256 string, sizeBytes int64, versionID string) (ConnectionTemplateReference, error) {
	binding, err := NewBinding(policy, ArtifactDescriptor{Kind: KindConnectionTemplate, Version: version, SHA256: sha256, SizeBytes: sizeBytes}, versionID)
	if err != nil {
		return ConnectionTemplateReference{}, err
	}
	return ConnectionTemplateReference{Binding: binding}, nil
}

func ParseConnectionTemplateReference(raw []byte, policy Policy) (ConnectionTemplateReference, error) {
	binding, err := ParseBinding(raw, policy)
	if err != nil || binding.Kind != KindConnectionTemplate {
		return ConnectionTemplateReference{}, ErrInvalidBinding
	}
	return ConnectionTemplateReference{Binding: binding}, nil
}

func (reference ConnectionTemplateReference) ValidateFor(policy Policy) error {
	if reference.Kind != KindConnectionTemplate {
		return ErrInvalidBinding
	}
	return reference.Binding.ValidateFor(policy)
}

// CloudFormationURL returns the path-style S3 HTTPS URL pinned to VersionID.
// Path style keeps bucket names containing dots valid under the S3 TLS
// certificate and is one of the supported CloudFormation S3 URL formats.
func (reference ConnectionTemplateReference) CloudFormationURL(region string) (string, error) {
	if !regionPattern.MatchString(region) || reference.Binding.ValidateFor(Policy{Bucket: reference.Bucket, KMSKeyID: reference.KMSKeyID}) != nil {
		return "", ErrInvalidBinding
	}
	suffix := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		suffix = "amazonaws.com.cn"
	}
	return (&url.URL{Scheme: "https", Host: "s3." + region + "." + suffix, Path: "/" + reference.Bucket + "/" + reference.Key, RawQuery: "versionId=" + url.QueryEscape(reference.VersionID)}).String(), nil
}

func NewBinding(policy Policy, descriptor ArtifactDescriptor, versionID string) (Binding, error) {
	if policy.Validate() != nil || descriptor.Validate() != nil || !validVersionID(versionID) {
		return Binding{}, ErrInvalidBinding
	}
	binding := Binding{
		Schema:      BindingSchema,
		Kind:        descriptor.Kind,
		Version:     descriptor.Version,
		Bucket:      policy.Bucket,
		Key:         objectKey(descriptor),
		VersionID:   versionID,
		SHA256:      descriptor.SHA256,
		SizeBytes:   descriptor.SizeBytes,
		ContentType: contentType(descriptor.Kind),
		KMSKeyID:    policy.KMSKeyID,
	}
	if binding.ValidateFor(policy) != nil {
		return Binding{}, ErrInvalidBinding
	}
	return binding, nil
}

func (binding Binding) Descriptor() ArtifactDescriptor {
	return ArtifactDescriptor{Kind: binding.Kind, Version: binding.Version, SHA256: binding.SHA256, SizeBytes: binding.SizeBytes}
}

func (binding Binding) ValidateFor(policy Policy) error {
	if policy.Validate() != nil || binding.Schema != BindingSchema || binding.Descriptor().Validate() != nil ||
		binding.Bucket != policy.Bucket || binding.KMSKeyID != policy.KMSKeyID || binding.Key != objectKey(binding.Descriptor()) ||
		binding.ContentType != contentType(binding.Kind) || !validVersionID(binding.VersionID) {
		return ErrInvalidBinding
	}
	return nil
}

// ParseBinding rejects both unknown fields and duplicate keys so an artifact
// reference cannot carry a mutable side channel such as a URL or tag.
func ParseBinding(raw []byte, policy Policy) (Binding, error) {
	if policy.Validate() != nil || rejectDuplicateKeys(raw) != nil {
		return Binding{}, ErrInvalidBinding
	}
	var fields map[string]json.RawMessage
	if decodeStrict(raw, &fields) != nil || !exactFields(fields, []string{"schema", "kind", "version", "bucket", "key", "version_id", "sha256", "size_bytes", "content_type", "kms_key_id"}) {
		return Binding{}, ErrInvalidBinding
	}
	var binding Binding
	if decodeStrict(raw, &binding) != nil || binding.ValidateFor(policy) != nil {
		return Binding{}, ErrInvalidBinding
	}
	return binding, nil
}

// ObjectStore is deliberately narrower than the AWS SDK.  Its methods accept
// only an immutable descriptor or a fully validated binding; bucket and key
// are never caller-provided parameters.
type ObjectStore interface {
	PutImmutable(ctx context.Context, descriptor ArtifactDescriptor, body io.Reader) (string, error)
	HeadImmutable(ctx context.Context, binding Binding) (ObjectMetadata, error)
}

type ObjectMetadata struct {
	VersionID   string
	SHA256      string // base64 encoded raw SHA-256, as returned by S3 HeadObject.
	SizeBytes   int64
	ContentType string
	KMSKeyID    string
}

func (metadata ObjectMetadata) Validate() error {
	if !validVersionID(metadata.VersionID) || !validChecksumBase64(metadata.SHA256) || metadata.SizeBytes < 1 || metadata.SizeBytes > MaxWorkerArchiveBytes ||
		(metadata.ContentType != BrokerZIPContentType && metadata.ContentType != WorkerArchiveContentType && metadata.ContentType != ConnectionTemplateContentType) || !kmsKeyPattern.MatchString(metadata.KMSKeyID) {
		return ErrInvalidBinding
	}
	return nil
}

type Publisher struct {
	policy Policy
	store  ObjectStore
}

func NewPublisher(policy Policy, store ObjectStore) (*Publisher, error) {
	if policy.Validate() != nil || store == nil {
		return nil, ErrInvalidPolicy
	}
	return &Publisher{policy: policy, store: store}, nil
}

// Publish writes exactly one versioned object, then reads that exact version
// back through the provider before returning a usable binding.
func (publisher *Publisher) Publish(ctx context.Context, descriptor ArtifactDescriptor, body io.Reader) (Binding, error) {
	if publisher == nil || publisher.policy.Validate() != nil || publisher.store == nil || descriptor.Validate() != nil || body == nil {
		return Binding{}, ErrInvalidDescriptor
	}
	versionID, err := publisher.store.PutImmutable(ctx, descriptor, body)
	if err != nil || !validVersionID(versionID) {
		return Binding{}, ErrProvider
	}
	binding, err := NewBinding(publisher.policy, descriptor, versionID)
	if err != nil {
		return Binding{}, ErrProvider
	}
	return publisher.Verify(ctx, binding)
}

// Verify checks the exact S3 version named by the binding.  It never permits
// a provider to substitute a current object for the pinned version.
func (publisher *Publisher) Verify(ctx context.Context, binding Binding) (Binding, error) {
	if publisher == nil || publisher.policy.Validate() != nil || publisher.store == nil || binding.ValidateFor(publisher.policy) != nil {
		return Binding{}, ErrInvalidBinding
	}
	metadata, err := publisher.store.HeadImmutable(ctx, binding)
	if err != nil || metadata.Validate() != nil || metadata.VersionID != binding.VersionID || metadata.SHA256 != checksumBase64(binding.SHA256) ||
		metadata.SizeBytes != binding.SizeBytes || metadata.ContentType != binding.ContentType || metadata.KMSKeyID != binding.KMSKeyID {
		return Binding{}, ErrVerification
	}
	return binding, nil
}

func contentType(kind ArtifactKind) string {
	if kind == KindBrokerZIP {
		return BrokerZIPContentType
	}
	if kind == KindWorkerArchive {
		return WorkerArchiveContentType
	}
	if kind == KindConnectionTemplate {
		return ConnectionTemplateContentType
	}
	return ""
}

func maxSize(kind ArtifactKind) int64 {
	if kind == KindBrokerZIP {
		return MaxBrokerZIPBytes
	}
	if kind == KindWorkerArchive {
		return MaxWorkerArchiveBytes
	}
	if kind == KindConnectionTemplate {
		return MaxConnectionTemplateBytes
	}
	return 0
}

func objectKey(descriptor ArtifactDescriptor) string {
	name, extension := "worker", "tar"
	if descriptor.Kind == KindBrokerZIP {
		name, extension = "broker", "zip"
	} else if descriptor.Kind == KindConnectionTemplate {
		name, extension = "connection-stack", "yaml"
	}
	digest := strings.TrimPrefix(descriptor.SHA256, "sha256:")
	return "releases/" + name + "/" + descriptor.Version + "/" + name + "-" + descriptor.Version + "-" + digest + "." + extension
}

func validKind(kind ArtifactKind) bool {
	return kind == KindBrokerZIP || kind == KindWorkerArchive || kind == KindConnectionTemplate
}

func validBuildVersion(version string) bool {
	lower := strings.ToLower(version)
	return buildVersionRE.MatchString(version) && lower != "latest" && !strings.Contains(lower, "latest") && version != "v1.0.3" && version != "1.0.3"
}

func validVersionID(versionID string) bool {
	return versionID != "null" && len(versionID) <= 1024 && versionIDPattern.MatchString(versionID)
}

func checksumBase64(digest string) string {
	raw, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if err != nil || len(raw) != sha256.Size {
		return ""
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func validChecksumBase64(value string) bool {
	raw, err := base64.StdEncoding.DecodeString(value)
	return err == nil && len(raw) == sha256.Size
}

func decodeStrict(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidBinding
	}
	return nil
}

func exactFields(fields map[string]json.RawMessage, expected []string) bool {
	if len(fields) != len(expected) {
		return false
	}
	for _, name := range expected {
		if _, ok := fields[name]; !ok {
			return false
		}
	}
	return true
}

func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalidBinding
	}
	return nil
}

func scanJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalidBinding
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidBinding
			}
			seen[key] = struct{}{}
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrInvalidBinding
		}
	case '[':
		for decoder.More() {
			if err := scanJSON(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrInvalidBinding
		}
	default:
		return ErrInvalidBinding
	}
	return nil
}
