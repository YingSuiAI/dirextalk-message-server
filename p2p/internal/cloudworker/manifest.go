// Package cloudworker defines the intentionally narrow bootstrap and outbound
// session boundary for one dedicated Cloud Worker VM. It does not contain an
// AWS SDK, shell runner, recipe executor, or cloud-control credential path.
package cloudworker

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	BootstrapManifestV1Schema                = "dirextalk.worker-bootstrap/v1"
	WorkerSessionClaimV1Schema               = "dirextalk.worker-session-claim/v1"
	WorkerSessionClaimResponseV1Schema       = "dirextalk.worker-session-claim-response/v1"
	WorkerEventV1Schema                      = "dirextalk.worker-event/v1"
	WorkerEventReceiptV1Schema               = "dirextalk.worker-event-receipt/v1"
	maxBootstrapManifestLifetime             = 10 * time.Minute
	maxBootstrapManifestBytes          int64 = 64 * 1024
)

var (
	identifierPattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	namedSHA256Pattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	canonicalInstantPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
)

// BootstrapManifest is intentionally byte-for-byte compatible with the
// Connection Stack V2 worker-bootstrap-v1 schema. It has no SSH, IAM, user
// data, secret, credential, or mutable cloud-control field.
type BootstrapManifest struct {
	Schema                 string `json:"schema"`
	ConnectionID           string `json:"connection_id"`
	DeploymentID           string `json:"deployment_id"`
	BootstrapSessionID     string `json:"bootstrap_session_id"`
	BootstrapEndpoint      string `json:"bootstrap_endpoint"`
	WorkerImageDigest      string `json:"worker_image_digest"`
	ArtifactManifestDigest string `json:"artifact_manifest_digest"`
	ExpiresAt              string `json:"expires_at"`
}

// ManifestValidationContext binds a cloud-init supplied manifest to the
// pre-approved connection endpoint and a short one-time bootstrap window.
type ManifestValidationContext struct {
	Now                       time.Time
	MaxLifetime               time.Duration
	ExpectedConnectionID      string
	ExpectedBootstrapEndpoint string
}

// ParseBootstrapManifest rejects unknown and duplicate JSON fields before it
// validates the value against the immutable launch context.
func ParseBootstrapManifest(raw []byte, context ManifestValidationContext) (BootstrapManifest, error) {
	if int64(len(raw)) == 0 || int64(len(raw)) > maxBootstrapManifestBytes {
		return BootstrapManifest{}, errors.New("worker bootstrap manifest is invalid")
	}
	var manifest BootstrapManifest
	if err := decodeStrictObject(raw, &manifest); err != nil {
		return BootstrapManifest{}, errors.New("worker bootstrap manifest is invalid")
	}
	if err := manifest.Validate(context); err != nil {
		return BootstrapManifest{}, err
	}
	return manifest, nil
}

// Validate verifies only non-secret identity and digest bindings. It never
// grants a session: the Connection Stack must still atomically consume the
// bootstrap_session_id after independently validating the instance proof.
func (manifest BootstrapManifest) Validate(context ManifestValidationContext) error {
	if manifest.Schema != BootstrapManifestV1Schema || !validIdentifier(manifest.ConnectionID) ||
		!validIdentifier(manifest.DeploymentID) || !validIdentifier(manifest.BootstrapSessionID) ||
		!validNamedSHA256(manifest.WorkerImageDigest) || !validNamedSHA256(manifest.ArtifactManifestDigest) {
		return errors.New("worker bootstrap manifest is invalid")
	}
	if context.Now.IsZero() || context.MaxLifetime <= 0 || context.MaxLifetime > maxBootstrapManifestLifetime ||
		!validIdentifier(context.ExpectedConnectionID) {
		return errors.New("worker bootstrap validation context is invalid")
	}
	if manifest.ConnectionID != context.ExpectedConnectionID {
		return errors.New("worker bootstrap manifest does not match its connection")
	}
	endpoint, err := canonicalHTTPSURL(manifest.BootstrapEndpoint)
	if err != nil {
		return errors.New("worker bootstrap manifest is invalid")
	}
	expectedEndpoint, err := canonicalHTTPSURL(context.ExpectedBootstrapEndpoint)
	if err != nil || endpoint != expectedEndpoint {
		return errors.New("worker bootstrap manifest does not match its endpoint")
	}
	expiresAt, err := parseCanonicalInstant(manifest.ExpiresAt)
	if err != nil || !expiresAt.After(context.Now.UTC()) || expiresAt.Sub(context.Now.UTC()) > context.MaxLifetime {
		return errors.New("worker bootstrap manifest has expired or exceeds its lifetime")
	}
	return nil
}

func validIdentifier(value string) bool {
	return identifierPattern.MatchString(value)
}

func validNamedSHA256(value string) bool {
	return namedSHA256Pattern.MatchString(value)
}

func parseCanonicalInstant(value string) (time.Time, error) {
	if !canonicalInstantPattern.MatchString(value) {
		return time.Time{}, errors.New("timestamp is invalid")
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000Z", value)
	if err != nil || parsed.UTC().Format("2006-01-02T15:04:05.000Z") != value {
		return time.Time{}, errors.New("timestamp is invalid")
	}
	return parsed.UTC(), nil
}

func canonicalInstant(value time.Time) string {
	return value.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z")
}

func canonicalHTTPSURL(value string) (string, error) {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 2048 {
		return "", errors.New("endpoint is invalid")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return "", errors.New("endpoint is invalid")
	}
	hostname := strings.ToLower(parsed.Hostname())
	if strings.Contains(hostname, ":") {
		hostname = "[" + hostname + "]"
	}
	if port := parsed.Port(); port != "" && port != "443" {
		hostname += ":" + port
	}
	parsed.Scheme = "https"
	parsed.Host = hostname
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	parsed.RawPath = ""
	return parsed.String(), nil
}

func decodeStrictObject(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	first, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := first.(json.Delim)
	if !ok || delimiter != '{' {
		return errors.New("JSON object is required")
	}
	keys := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("JSON object key is invalid")
		}
		if _, exists := keys[key]; exists {
			return errors.New("JSON object contains duplicate keys")
		}
		keys[key] = struct{}{}
		var discarded json.RawMessage
		if err := decoder.Decode(&discarded); err != nil {
			return err
		}
	}
	last, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok = last.(json.Delim)
	if !ok || delimiter != '}' {
		return errors.New("JSON object is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing data")
	}

	strict := json.NewDecoder(bytes.NewReader(raw))
	strict.DisallowUnknownFields()
	if err := strict.Decode(target); err != nil {
		return err
	}
	if err := strict.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing data")
	}
	return nil
}
