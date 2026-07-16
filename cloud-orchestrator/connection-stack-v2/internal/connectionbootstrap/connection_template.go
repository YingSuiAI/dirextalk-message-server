package connectionbootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/artifactpublish"
)

// These wire constants and field names intentionally mirror
// p2p/internal/cloud/connection_template.go. This independent bootstrap
// module cannot import ProductCore's internal package, so the portable JSON
// contract is reproduced here with the same closed two-branch union.
const (
	connectionTemplateReferenceSchema   = "dirextalk.connection-template-reference/v1"
	connectionTemplateModeS3Binding     = "s3_binding"
	connectionTemplateModePublishIntent = "publish_intent"
	maxConnectionTemplateJSON           = 8 << 10
)

// ConnectionTemplateReference is the non-secret, closed template fact carried
// in a role plan and retained with the one-time bootstrap session. A normal
// role plan contains an exact S3 binding. A permitted root plan contains only
// the reviewed bytes it is allowed to publish after Foundation exists.
type ConnectionTemplateReference struct {
	Schema        string                           `json:"schema"`
	Mode          string                           `json:"mode"`
	Binding       *ConnectionTemplateBinding       `json:"binding,omitempty"`
	PublishIntent *ConnectionTemplatePublishIntent `json:"publish_intent,omitempty"`
}

type ConnectionTemplateBinding struct {
	Schema      string `json:"schema"`
	Kind        string `json:"kind"`
	Version     string `json:"version"`
	Bucket      string `json:"bucket"`
	Key         string `json:"key"`
	VersionID   string `json:"version_id"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
	KMSKeyID    string `json:"kms_key_id"`
}

type ConnectionTemplatePublishIntent struct {
	Kind        string `json:"kind"`
	Version     string `json:"version"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	ContentType string `json:"content_type"`
}

func (reference ConnectionTemplateReference) Clone() ConnectionTemplateReference {
	clone := reference
	if reference.Binding != nil {
		value := *reference.Binding
		clone.Binding = &value
	}
	if reference.PublishIntent != nil {
		value := *reference.PublishIntent
		clone.PublishIntent = &value
	}
	return clone
}

func (reference ConnectionTemplateReference) IsZero() bool {
	return reference.Schema == "" && reference.Mode == "" && reference.Binding == nil && reference.PublishIntent == nil
}

func (reference ConnectionTemplateReference) Equal(other ConnectionTemplateReference) bool {
	if reference.Schema != other.Schema || reference.Mode != other.Mode || (reference.Binding == nil) != (other.Binding == nil) || (reference.PublishIntent == nil) != (other.PublishIntent == nil) {
		return false
	}
	return (reference.Binding == nil || *reference.Binding == *other.Binding) && (reference.PublishIntent == nil || *reference.PublishIntent == *other.PublishIntent)
}

func (reference ConnectionTemplateReference) Validate() error {
	if reference.Schema != connectionTemplateReferenceSchema {
		return ErrInvalid
	}
	switch reference.Mode {
	case connectionTemplateModeS3Binding:
		if reference.Binding == nil || reference.PublishIntent != nil || reference.Binding.Validate() != nil {
			return ErrInvalid
		}
	case connectionTemplateModePublishIntent:
		if reference.PublishIntent == nil || reference.Binding != nil || reference.PublishIntent.Validate() != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

func (reference ConnectionTemplateReference) ValidateForRootCredentialBootstrap(allowRootCredentialBootstrap bool) error {
	if reference.Validate() != nil {
		return ErrInvalid
	}
	if allowRootCredentialBootstrap && reference.Mode != connectionTemplateModePublishIntent {
		return ErrInvalid
	}
	if !allowRootCredentialBootstrap && reference.Mode != connectionTemplateModeS3Binding {
		return ErrInvalid
	}
	return nil
}

func (binding ConnectionTemplateBinding) Validate() error {
	reference := artifactpublish.ConnectionTemplateReference{Binding: artifactpublish.Binding{
		Schema: binding.Schema, Kind: artifactpublish.ArtifactKind(binding.Kind), Version: binding.Version,
		Bucket: binding.Bucket, Key: binding.Key, VersionID: binding.VersionID, SHA256: binding.SHA256,
		SizeBytes: binding.SizeBytes, ContentType: binding.ContentType, KMSKeyID: binding.KMSKeyID,
	}}
	if reference.ValidateFor(artifactpublish.Policy{Bucket: binding.Bucket, KMSKeyID: binding.KMSKeyID}) != nil {
		return ErrInvalid
	}
	return nil
}

func (intent ConnectionTemplatePublishIntent) Validate() error {
	descriptor := artifactpublish.ArtifactDescriptor{Kind: artifactpublish.ArtifactKind(intent.Kind), Version: intent.Version, SHA256: intent.SHA256, SizeBytes: intent.SizeBytes}
	if descriptor.Validate() != nil || descriptor.Kind != artifactpublish.KindConnectionTemplate || intent.ContentType != artifactpublish.ConnectionTemplateContentType {
		return ErrInvalid
	}
	return nil
}

func (reference ConnectionTemplateReference) ContentDigest() string {
	if reference.Binding != nil {
		return reference.Binding.SHA256
	}
	if reference.PublishIntent != nil {
		return reference.PublishIntent.SHA256
	}
	return ""
}

// ArtifactReference converts only the S3 binding branch into the nested
// publisher type. No URL can cross this boundary and the policy must exactly
// name the binding's Foundation bucket and KMS key.
func (reference ConnectionTemplateReference) ArtifactReference(policy artifactpublish.Policy) (artifactpublish.ConnectionTemplateReference, error) {
	if reference.Validate() != nil || reference.Mode != connectionTemplateModeS3Binding || reference.Binding == nil || policy.Validate() != nil {
		return artifactpublish.ConnectionTemplateReference{}, ErrInvalid
	}
	binding := *reference.Binding
	if binding.Bucket != policy.Bucket || binding.KMSKeyID != policy.KMSKeyID {
		return artifactpublish.ConnectionTemplateReference{}, ErrInvalid
	}
	result := artifactpublish.ConnectionTemplateReference{Binding: artifactpublish.Binding{
		Schema: binding.Schema, Kind: artifactpublish.ArtifactKind(binding.Kind), Version: binding.Version,
		Bucket: binding.Bucket, Key: binding.Key, VersionID: binding.VersionID, SHA256: binding.SHA256,
		SizeBytes: binding.SizeBytes, ContentType: binding.ContentType, KMSKeyID: binding.KMSKeyID,
	}}
	if result.ValidateFor(policy) != nil {
		return artifactpublish.ConnectionTemplateReference{}, ErrInvalid
	}
	return result, nil
}

func (reference *ConnectionTemplateReference) UnmarshalJSON(raw []byte) error {
	parsed, err := ParseConnectionTemplateReference(raw)
	if err != nil {
		return ErrInvalid
	}
	*reference = parsed
	return nil
}

func ParseConnectionTemplateReference(raw []byte) (ConnectionTemplateReference, error) {
	if len(raw) == 0 || len(raw) > maxConnectionTemplateJSON || rejectDuplicateKeys(raw) != nil || validateConnectionTemplateReferenceJSONShape(raw) != nil {
		return ConnectionTemplateReference{}, ErrInvalid
	}
	type wire ConnectionTemplateReference
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value wire
	if err := decoder.Decode(&value); err != nil {
		return ConnectionTemplateReference{}, ErrInvalid
	}
	if err := decoder.Decode(new(struct{})); !errors.Is(err, io.EOF) {
		return ConnectionTemplateReference{}, ErrInvalid
	}
	reference := ConnectionTemplateReference(value)
	if reference.Validate() != nil {
		return ConnectionTemplateReference{}, ErrInvalid
	}
	return reference, nil
}

func validateConnectionTemplateReferenceJSONShape(raw []byte) error {
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil {
		return ErrInvalid
	}
	var mode string
	if value, ok := top["mode"]; !ok || json.Unmarshal(value, &mode) != nil {
		return ErrInvalid
	}
	switch mode {
	case connectionTemplateModeS3Binding:
		if !exactTemplateJSONFields(top, "schema", "mode", "binding") {
			return ErrInvalid
		}
		return validateConnectionTemplateBindingJSONShape(top["binding"])
	case connectionTemplateModePublishIntent:
		if !exactTemplateJSONFields(top, "schema", "mode", "publish_intent") {
			return ErrInvalid
		}
		return validateConnectionTemplatePublishIntentJSONShape(top["publish_intent"])
	default:
		return ErrInvalid
	}
}

func validateConnectionTemplateBindingJSONShape(raw json.RawMessage) error {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || !exactTemplateJSONFields(value, "schema", "kind", "version", "bucket", "key", "version_id", "sha256", "size_bytes", "content_type", "kms_key_id") {
		return ErrInvalid
	}
	return nil
}

func validateConnectionTemplatePublishIntentJSONShape(raw json.RawMessage) error {
	var value map[string]json.RawMessage
	if json.Unmarshal(raw, &value) != nil || !exactTemplateJSONFields(value, "kind", "version", "sha256", "size_bytes", "content_type") {
		return ErrInvalid
	}
	return nil
}

func exactTemplateJSONFields(value map[string]json.RawMessage, fields ...string) bool {
	if len(value) != len(fields) {
		return false
	}
	for _, field := range fields {
		if _, ok := value[field]; !ok {
			return false
		}
	}
	return true
}
