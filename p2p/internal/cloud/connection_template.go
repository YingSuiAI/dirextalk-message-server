package cloud

import (
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	connectionTemplateReferenceSchema   = "dirextalk.connection-template-reference/v1"
	connectionTemplateModeS3Binding     = "s3_binding"
	connectionTemplateModePublishIntent = "publish_intent"

	immutableArtifactBindingSchema       = "dirextalk.immutable-artifact-binding/v1"
	connectionTemplateArtifactKind       = "connection_stack_template"
	connectionTemplateContentType        = "application/x-yaml"
	maxConnectionTemplateBytes     int64 = 1 << 20
	maxConnectionTemplateJSON            = 8 << 10
)

var (
	connectionTemplateBucketPattern    = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])$`)
	connectionTemplateKMSKeyPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:/_.-]{1,511}$`)
	connectionTemplateVersionIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._~+/=-]*$`)
	connectionTemplateVersionPattern   = regexp.MustCompile(`^v?(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)(?:\.(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*))*$`)
)

// ConnectionTemplateReference is the closed, durable reference passed from
// ProductCore to the independent credential bootstrap. It deliberately has
// no arbitrary URL, tag, prefix, or artifact kind. A normal role plan carries
// a fully version-pinned S3 binding; a root credential bootstrap carries only
// an immutable publish intent because the user's artifact bucket does not
// exist until the bootstrap foundation is created.
type ConnectionTemplateReference struct {
	Schema        string                           `json:"schema"`
	Mode          string                           `json:"mode"`
	Binding       *ConnectionTemplateBinding       `json:"binding,omitempty"`
	PublishIntent *ConnectionTemplatePublishIntent `json:"publish_intent,omitempty"`
}

// ConnectionTemplateBinding mirrors the nested Connection Stack's portable
// immutable artifact binding without importing that independent Go module.
// The JSON field shape is intentionally identical.
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

// ConnectionTemplatePublishIntent identifies the reviewed bytes the root
// bootstrap may publish after it creates the private artifact bucket. It must
// never grow URL, bucket, object-key, object-version, or KMS fields.
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
		binding := *reference.Binding
		clone.Binding = &binding
	}
	if reference.PublishIntent != nil {
		intent := *reference.PublishIntent
		clone.PublishIntent = &intent
	}
	return clone
}

func (reference ConnectionTemplateReference) Validate() error {
	if reference.Schema != connectionTemplateReferenceSchema {
		return errors.New("cloud connection template reference is invalid")
	}
	switch reference.Mode {
	case connectionTemplateModeS3Binding:
		if reference.Binding == nil || reference.PublishIntent != nil || reference.Binding.Validate() != nil {
			return errors.New("cloud connection template reference is invalid")
		}
	case connectionTemplateModePublishIntent:
		if reference.PublishIntent == nil || reference.Binding != nil || reference.PublishIntent.Validate() != nil {
			return errors.New("cloud connection template reference is invalid")
		}
	default:
		return errors.New("cloud connection template reference is invalid")
	}
	return nil
}

// ValidateForRootCredentialBootstrap closes the only two execution paths:
// ordinary role plans may consume a read-back immutable binding, while a
// root credential flow may carry only the later-to-be-published intent.
func (reference ConnectionTemplateReference) ValidateForRootCredentialBootstrap(allowRootCredentialBootstrap bool) error {
	if err := reference.Validate(); err != nil {
		return err
	}
	if allowRootCredentialBootstrap && reference.Mode != connectionTemplateModePublishIntent {
		return errors.New("cloud connection template reference is invalid")
	}
	if !allowRootCredentialBootstrap && reference.Mode != connectionTemplateModeS3Binding {
		return errors.New("cloud connection template reference is invalid")
	}
	return nil
}

func (binding ConnectionTemplateBinding) Validate() error {
	if binding.Schema != immutableArtifactBindingSchema || binding.Kind != connectionTemplateArtifactKind ||
		!validConnectionTemplateVersion(binding.Version) || !connectionTemplateBucketPattern.MatchString(binding.Bucket) || strings.Contains(binding.Bucket, "..") ||
		!validConnectionTemplateVersionID(binding.VersionID) || !namedSHA256Pattern.MatchString(binding.SHA256) ||
		binding.SizeBytes < 1 || binding.SizeBytes > maxConnectionTemplateBytes || binding.ContentType != connectionTemplateContentType ||
		!connectionTemplateKMSKeyPattern.MatchString(binding.KMSKeyID) || binding.Key != connectionTemplateObjectKey(binding.Version, binding.SHA256) {
		return errors.New("cloud connection template binding is invalid")
	}
	return nil
}

func (intent ConnectionTemplatePublishIntent) Validate() error {
	if intent.Kind != connectionTemplateArtifactKind || !validConnectionTemplateVersion(intent.Version) ||
		!namedSHA256Pattern.MatchString(intent.SHA256) || intent.SizeBytes < 1 || intent.SizeBytes > maxConnectionTemplateBytes ||
		intent.ContentType != connectionTemplateContentType {
		return errors.New("cloud connection template publish intent is invalid")
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

// IdentityDigest is a stable, unambiguous digest over every union branch
// field. It is used in request/idempotency facts so a changed publish intent
// cannot reuse a prior root credential bootstrap approval.
func (reference ConnectionTemplateReference) IdentityDigest() string {
	var binding ConnectionTemplateBinding
	var intent ConnectionTemplatePublishIntent
	if reference.Binding != nil {
		binding = *reference.Binding
	}
	if reference.PublishIntent != nil {
		intent = *reference.PublishIntent
	}
	return digestFields(
		reference.Schema, reference.Mode,
		binding.Schema, binding.Kind, binding.Version, binding.Bucket, binding.Key, binding.VersionID, binding.SHA256, int64String(binding.SizeBytes), binding.ContentType, binding.KMSKeyID,
		intent.Kind, intent.Version, intent.SHA256, int64String(intent.SizeBytes), intent.ContentType,
	)
}

func (binding ConnectionTemplateBinding) CloudFormationURL(region string) (string, error) {
	if binding.Validate() != nil || !cloudRegionPattern.MatchString(region) {
		return "", errors.New("cloud connection template URL is invalid")
	}
	suffix := "amazonaws.com"
	if strings.HasPrefix(region, "cn-") {
		suffix = "amazonaws.com.cn"
	}
	return (&url.URL{
		Scheme:   "https",
		Host:     "s3." + region + "." + suffix,
		Path:     "/" + binding.Bucket + "/" + binding.Key,
		RawQuery: "versionId=" + url.QueryEscape(binding.VersionID),
	}).String(), nil
}

func (reference ConnectionTemplateReference) CloudFormationURL(region string) (string, error) {
	if reference.Validate() != nil || reference.Mode != connectionTemplateModeS3Binding || reference.Binding == nil {
		return "", errors.New("cloud connection template URL is invalid")
	}
	return reference.Binding.CloudFormationURL(region)
}

func EncodeConnectionTemplateReference(reference ConnectionTemplateReference) (string, error) {
	if reference.Validate() != nil {
		return "", errors.New("cloud connection template reference is invalid")
	}
	raw, err := json.Marshal(reference)
	if err != nil || len(raw) == 0 || len(raw) > maxConnectionTemplateJSON {
		return "", errors.New("cloud connection template reference is invalid")
	}
	return string(raw), nil
}

func ParseConnectionTemplateReference(raw string) (ConnectionTemplateReference, error) {
	if strings.TrimSpace(raw) == "" || len(raw) > maxConnectionTemplateJSON || rejectDuplicateConnectionTemplateJSONKeys(raw) != nil || validateConnectionTemplateReferenceJSONShape(raw) != nil {
		return ConnectionTemplateReference{}, errors.New("cloud connection template reference is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var reference ConnectionTemplateReference
	if err := decoder.Decode(&reference); err != nil {
		return ConnectionTemplateReference{}, errors.New("cloud connection template reference is invalid")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) || reference.Validate() != nil {
		return ConnectionTemplateReference{}, errors.New("cloud connection template reference is invalid")
	}
	return reference, nil
}

func validateConnectionTemplateReferenceJSONShape(raw string) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &top); err != nil || !exactConnectionTemplateJSONFields(top, "schema", "mode", "binding", "publish_intent") && !exactConnectionTemplateJSONFields(top, "schema", "mode", "binding") && !exactConnectionTemplateJSONFields(top, "schema", "mode", "publish_intent") {
		return errors.New("invalid connection template JSON shape")
	}
	var mode string
	if rawMode, found := top["mode"]; !found || json.Unmarshal(rawMode, &mode) != nil {
		return errors.New("invalid connection template JSON shape")
	}
	switch mode {
	case connectionTemplateModeS3Binding:
		if !exactConnectionTemplateJSONFields(top, "schema", "mode", "binding") {
			return errors.New("invalid connection template JSON shape")
		}
		return validateConnectionTemplateBindingJSONShape(top["binding"])
	case connectionTemplateModePublishIntent:
		if !exactConnectionTemplateJSONFields(top, "schema", "mode", "publish_intent") {
			return errors.New("invalid connection template JSON shape")
		}
		return validateConnectionTemplatePublishIntentJSONShape(top["publish_intent"])
	default:
		return errors.New("invalid connection template JSON shape")
	}
}

func validateConnectionTemplateBindingJSONShape(raw json.RawMessage) error {
	var binding map[string]json.RawMessage
	if err := json.Unmarshal(raw, &binding); err != nil || !exactConnectionTemplateJSONFields(binding, "schema", "kind", "version", "bucket", "key", "version_id", "sha256", "size_bytes", "content_type", "kms_key_id") {
		return errors.New("invalid connection template binding JSON shape")
	}
	return nil
}

func validateConnectionTemplatePublishIntentJSONShape(raw json.RawMessage) error {
	var intent map[string]json.RawMessage
	if err := json.Unmarshal(raw, &intent); err != nil || !exactConnectionTemplateJSONFields(intent, "kind", "version", "sha256", "size_bytes", "content_type") {
		return errors.New("invalid connection template publish intent JSON shape")
	}
	return nil
}

func exactConnectionTemplateJSONFields(fields map[string]json.RawMessage, expected ...string) bool {
	if len(fields) != len(expected) {
		return false
	}
	for _, key := range expected {
		if _, found := fields[key]; !found {
			return false
		}
	}
	return true
}

// ParsePersistedConnectionTemplateReference additionally requires the exact
// canonical JSON emitted by EncodeConnectionTemplateReference. Runtime config
// may be normally formatted JSON, while the durable fact is deliberately one
// canonical representation so a restart cannot reinterpret it.
func ParsePersistedConnectionTemplateReference(raw string) (ConnectionTemplateReference, error) {
	reference, err := ParseConnectionTemplateReference(raw)
	if err != nil {
		return ConnectionTemplateReference{}, err
	}
	canonical, err := EncodeConnectionTemplateReference(reference)
	if err != nil || canonical != raw {
		return ConnectionTemplateReference{}, errors.New("cloud connection template reference is invalid")
	}
	return reference, nil
}

func rejectDuplicateConnectionTemplateJSONKeys(raw string) error {
	decoder := json.NewDecoder(strings.NewReader(raw))
	if err := consumeConnectionTemplateJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected JSON token")
	}
	return nil
}

func consumeConnectionTemplateJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, keyOK := keyToken.(string)
			if err != nil || !keyOK {
				return errors.New("invalid JSON object key")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate JSON object key")
			}
			seen[key] = struct{}{}
			if err := consumeConnectionTemplateJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("invalid JSON object")
		}
	case '[':
		for decoder.More() {
			if err := consumeConnectionTemplateJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("invalid JSON array")
		}
	default:
		return errors.New("invalid JSON delimiter")
	}
	return nil
}

func validTemplateURL(raw string) bool {
	_, err := parsePinnedConnectionTemplateURL(raw)
	return err == nil
}

func validateTemplateURLForBinding(raw, region string, binding ConnectionTemplateBinding) error {
	if !validTemplateURL(raw) {
		return errors.New("cloud connection template URL is invalid")
	}
	expected, err := binding.CloudFormationURL(region)
	if err != nil || raw != expected {
		return errors.New("cloud connection template URL is invalid")
	}
	return nil
}

func parsePinnedConnectionTemplateURL(raw string) (struct{}, error) {
	if raw == "" || len(raw) > 2048 || strings.TrimSpace(raw) != raw {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Port() != "" || parsed.User != nil || parsed.Fragment != "" || parsed.ForceQuery || parsed.RawPath != "" {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	region, ok := connectionTemplateURLRegion(parsed.Host)
	if !ok || !cloudRegionPattern.MatchString(region) {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	bucket, key, ok := splitConnectionTemplateObjectPath(parsed.Path)
	if !ok || !connectionTemplateBucketPattern.MatchString(bucket) || strings.Contains(bucket, "..") || !validConnectionTemplateObjectPath(key) {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil || len(query) != 1 || len(query["versionId"]) != 1 || !validConnectionTemplateVersionID(query.Get("versionId")) {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	canonical := (&url.URL{
		Scheme:   "https",
		Host:     parsed.Host,
		Path:     parsed.Path,
		RawQuery: "versionId=" + url.QueryEscape(query.Get("versionId")),
	}).String()
	if raw != canonical {
		return struct{}{}, errors.New("cloud connection template URL is invalid")
	}
	return struct{}{}, nil
}

func connectionTemplateURLRegion(host string) (string, bool) {
	const prefix = "s3."
	if !strings.HasPrefix(host, prefix) {
		return "", false
	}
	remainder := strings.TrimPrefix(host, prefix)
	suffix := ".amazonaws.com"
	if strings.HasSuffix(remainder, ".amazonaws.com.cn") {
		suffix = ".amazonaws.com.cn"
	}
	if !strings.HasSuffix(remainder, suffix) {
		return "", false
	}
	region := strings.TrimSuffix(remainder, suffix)
	if region == "" || strings.Contains(region, ".") || (strings.HasPrefix(region, "cn-") != (suffix == ".amazonaws.com.cn")) {
		return "", false
	}
	return region, true
}

func splitConnectionTemplateObjectPath(path string) (bucket, key string, ok bool) {
	if !strings.HasPrefix(path, "/") || strings.Contains(path, "//") {
		return "", "", false
	}
	parts := strings.SplitN(strings.TrimPrefix(path, "/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func validConnectionTemplateObjectPath(key string) bool {
	if key == "" || len(key) > 1024 || strings.HasPrefix(key, "/") || strings.HasSuffix(key, "/") || strings.Contains(key, "//") {
		return false
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func connectionTemplateObjectKey(version, sha256 string) string {
	return "releases/connection-stack/" + version + "/connection-stack-" + version + "-" + strings.TrimPrefix(sha256, "sha256:") + ".yaml"
}

func validConnectionTemplateVersion(version string) bool {
	lower := strings.ToLower(version)
	return connectionTemplateVersionPattern.MatchString(version) && lower != "latest" && !strings.Contains(lower, "latest") && version != "v1.0.3" && version != "1.0.3"
}

func validConnectionTemplateVersionID(versionID string) bool {
	return versionID != "null" && len(versionID) <= 1024 && connectionTemplateVersionIDPattern.MatchString(versionID)
}

func int64String(value int64) string {
	return strconv.FormatInt(value, 10)
}
