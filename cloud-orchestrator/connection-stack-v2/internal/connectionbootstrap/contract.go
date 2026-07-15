package connectionbootstrap

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	CreateRequestSchema  = "dirextalk.aws-bootstrap-session-create/v1"
	CreateResponseSchema = "dirextalk.aws-bootstrap-session/v1"
	UploadEnvelopeSchema = "dirextalk.aws-credential-upload/v1"
	CredentialSchema     = "dirextalk.aws-bootstrap-credentials/v1"
	ReceiptSchema        = "dirextalk.aws-bootstrap-accepted/v1"
	SessionTTL           = 10 * time.Minute
	maxJSONBytes         = 32 << 10
)

var (
	ErrInvalid          = errors.New("connection bootstrap request is invalid")
	ErrUnauthorized     = errors.New("connection bootstrap request is unauthorized")
	ErrExpired          = errors.New("connection bootstrap session expired")
	ErrConflict         = errors.New("connection bootstrap upload conflicts with consumed session")
	ErrConsumed         = errors.New("connection bootstrap upload was already consumed")
	identifierPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	regionPattern       = regexp.MustCompile(`^[a-z]{2}(?:-gov)?-[a-z]+-[1-9][0-9]?$`)
	parameterKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,254}$`)
	accessKeyPattern    = regexp.MustCompile(`^(?:AKIA|ASIA)[A-Z0-9]{16}$`)
	uuidPattern         = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type Config struct {
	Region                        string            `json:"region"`
	TemplateURL                   string            `json:"template_url"`
	TemplateDigest                string            `json:"template_digest"`
	SourceTreeDigest              string            `json:"source_tree_digest"`
	UploadBaseURL                 string            `json:"upload_base_url"`
	BrokerArtifact                string            `json:"broker_artifact"`
	BrokerArtifactBucket          string            `json:"broker_artifact_bucket"`
	BrokerArtifactKey             string            `json:"broker_artifact_key"`
	BrokerArtifactVersion         string            `json:"broker_artifact_version"`
	WorkerAMIID                   string            `json:"worker_ami_id"`
	NetworkID                     string            `json:"network_id"`
	WorkerSubnetID                string            `json:"worker_subnet_id"`
	WorkerAvailabilityZone        string            `json:"worker_availability_zone"`
	WorkerIdentityRSAPublicKeyPEM string            `json:"worker_identity_rsa_public_key_pem"`
	DeploymentCreateEnabled       bool              `json:"deployment_create_enabled"`
	DeploymentDestroyEnabled      bool              `json:"deployment_destroy_enabled"`
	ServiceSecretsEnabled         bool              `json:"service_secrets_enabled"`
	DynamicArtifactsEnabled       bool              `json:"dynamic_artifacts_enabled"`
	ArtifactManifestDigest        string            `json:"artifact_manifest_digest"`
	FixedParameters               map[string]string `json:"fixed_parameters"`
	FixedTags                     map[string]string `json:"fixed_tags"`
}

var connectionStackParameters = map[string]struct{}{
	"ConnectionId": {}, "ConnectionGeneration": {}, "NodeKeyId": {},
	"NodePublicKeySpkiBase64": {}, "DeviceApprovalKeyId": {},
	"DeviceApprovalPublicKeySpkiBase64": {}, "StageName": {},
}

func (config Config) Validate() error {
	if !regionPattern.MatchString(config.Region) || !validHTTPSURL(config.TemplateURL) || !namedDigest(config.TemplateDigest) || !namedDigest(config.SourceTreeDigest) || !validHTTPSURL(config.UploadBaseURL) || !strings.HasPrefix(config.WorkerAMIID, "ami-") || config.BrokerArtifact == "" || config.BrokerArtifactBucket == "" || config.BrokerArtifactKey == "" || config.NetworkID == "" || config.WorkerSubnetID == "" || config.WorkerAvailabilityZone == "" || !namedDigest(config.ArtifactManifestDigest) || len(config.FixedParameters) > 32 || len(config.FixedTags) > 32 || (config.DeploymentCreateEnabled && strings.TrimSpace(config.WorkerIdentityRSAPublicKeyPEM) == "") {
		return ErrInvalid
	}
	for key, value := range config.FixedParameters {
		if !parameterKeyPattern.MatchString(key) || len(value) == 0 || len(value) > 1024 {
			return ErrInvalid
		}
		if _, reserved := connectionStackParameters[key]; reserved {
			return ErrInvalid
		}
	}
	for key, value := range config.FixedTags {
		if len(key) == 0 || len(key) > 128 || len(value) > 256 || strings.HasPrefix(strings.ToLower(key), "aws:") {
			return ErrInvalid
		}
	}
	return nil
}

type RolePlan struct {
	BootstrapID                  string            `json:"bootstrap_id"`
	ConnectionID                 string            `json:"connection_id"`
	Region                       string            `json:"region"`
	StackName                    string            `json:"stack_name"`
	TemplateURL                  string            `json:"template_url"`
	TemplateDigest               string            `json:"template_digest"`
	SourceTreeDigest             string            `json:"source_tree_digest"`
	FixedParameters              map[string]string `json:"fixed_parameters"`
	NodeKeyID                    string            `json:"node_key_id"`
	NodeEd25519PublicKey         string            `json:"node_ed25519_public_key"`
	DeviceKeyID                  string            `json:"device_key_id"`
	DeviceEd25519PublicKey       string            `json:"device_ed25519_public_key"`
	AllowRootCredentialBootstrap bool              `json:"allow_root_credential_bootstrap"`
	ExpiresAt                    string            `json:"expires_at"`
}
type CreateRequest struct {
	Schema    string   `json:"schema"`
	RequestID string   `json:"request_id"`
	RolePlan  RolePlan `json:"role_plan"`
}
type CreateResponse struct {
	Schema                string   `json:"schema"`
	Status                string   `json:"status"`
	RequestID             string   `json:"request_id"`
	SessionID             string   `json:"session_id"`
	ConnectionID          string   `json:"connection_id"`
	ServerX25519PublicKey string   `json:"server_x25519_public_key"`
	UploadBearer          string   `json:"upload_bearer"`
	UploadURL             string   `json:"upload_url"`
	ExpiresAt             string   `json:"expires_at"`
	HKDF                  string   `json:"hkdf"`
	AAD                   string   `json:"aad"`
	Receipt               *Receipt `json:"receipt,omitempty"`
}
type UploadEnvelope struct {
	Schema                string `json:"schema"`
	SessionID             string `json:"session_id"`
	ClientX25519PublicKey string `json:"client_x25519_public_key"`
	Nonce                 string `json:"nonce"`
	Ciphertext            string `json:"ciphertext"`
	ExpiresAt             string `json:"expires_at"`
}
type credentialWire struct {
	Schema          string `json:"schema"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}
type Receipt struct {
	Schema       string `json:"schema"`
	Status       string `json:"status"`
	StackID      string `json:"stack_id"`
	ConnectionID string `json:"connection_id"`
	AcceptedAt   string `json:"accepted_at"`
}
type Identity struct {
	BootstrapID, ConnectionID, NodeKeyID, NodeEd25519PublicKey string
	DeviceKeyID, DeviceEd25519PublicKey, StackName             string
	AllowRootCredentialBootstrap                               bool
	FixedParameters                                            map[string]string
}
type Credentials struct{ AccessKeyID, SecretAccessKey, SessionToken []byte }

func (credentials *Credentials) Zero() {
	if credentials == nil {
		return
	}
	clear(credentials.AccessKeyID)
	clear(credentials.SecretAccessKey)
	clear(credentials.SessionToken)
	credentials.AccessKeyID = nil
	credentials.SecretAccessKey = nil
	credentials.SessionToken = nil
}

func ParseConfig(raw []byte) (Config, error) {
	var config Config
	if strictDecode(raw, &config) != nil || config.Validate() != nil {
		return Config{}, ErrInvalid
	}
	return config, nil
}
func ParseCreateRequest(raw []byte) (CreateRequest, error) {
	var request CreateRequest
	if strictDecode(raw, &request) != nil || request.Validate() != nil {
		return CreateRequest{}, ErrInvalid
	}
	return request, nil
}
func (request CreateRequest) Validate() error {
	plan := request.RolePlan
	expires, err := time.Parse(time.RFC3339Nano, plan.ExpiresAt)
	if request.Schema != CreateRequestSchema || !uuidPattern.MatchString(request.RequestID) || !identifierPattern.MatchString(plan.BootstrapID) || !identifierPattern.MatchString(plan.ConnectionID) || !identifierPattern.MatchString(plan.NodeKeyID) || !identifierPattern.MatchString(plan.DeviceKeyID) || !validEd25519(plan.NodeEd25519PublicKey) || !validEd25519(plan.DeviceEd25519PublicKey) || plan.StackName != DeterministicStackName(plan.ConnectionID) || !regionPattern.MatchString(plan.Region) || !validHTTPSURL(plan.TemplateURL) || !namedDigest(plan.TemplateDigest) || !namedDigest(plan.SourceTreeDigest) || err != nil || expires.UTC().Format(time.RFC3339Nano) != plan.ExpiresAt || !validConnectionStackParameters(plan) {
		return ErrInvalid
	}
	return nil
}

func validConnectionStackParameters(plan RolePlan) bool {
	if len(plan.FixedParameters) != len(connectionStackParameters) {
		return false
	}
	want := map[string]string{
		"ConnectionId": plan.ConnectionID, "ConnectionGeneration": "1",
		"NodeKeyId": plan.NodeKeyID, "NodePublicKeySpkiBase64": plan.NodeEd25519PublicKey,
		"DeviceApprovalKeyId": plan.DeviceKeyID, "DeviceApprovalPublicKeySpkiBase64": plan.DeviceEd25519PublicKey,
		"StageName": "prod",
	}
	return sameStringMap(plan.FixedParameters, want)
}
func ParseUploadEnvelope(raw []byte) (UploadEnvelope, error) {
	var envelope UploadEnvelope
	if strictDecode(raw, &envelope) != nil || envelope.Validate() != nil {
		return UploadEnvelope{}, ErrInvalid
	}
	return envelope, nil
}
func (envelope UploadEnvelope) Validate() error {
	if envelope.Schema != UploadEnvelopeSchema || !identifierPattern.MatchString(envelope.SessionID) {
		return ErrInvalid
	}
	public, err := base64.StdEncoding.DecodeString(envelope.ClientX25519PublicKey)
	if err != nil || len(public) != 32 {
		return ErrInvalid
	}
	nonce, err := base64.StdEncoding.DecodeString(envelope.Nonce)
	if err != nil || len(nonce) != 12 {
		return ErrInvalid
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil || len(ciphertext) < 17 || len(ciphertext) > 16<<10 {
		return ErrInvalid
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope.ExpiresAt); err != nil {
		return ErrInvalid
	}
	return nil
}
func ParseCredentials(raw []byte) (Credentials, error) {
	var wire credentialWire
	if strictDecode(raw, &wire) != nil || wire.Schema != CredentialSchema || !accessKeyPattern.MatchString(wire.AccessKeyID) || len(wire.SecretAccessKey) < 20 || len(wire.SecretAccessKey) > 128 || len(wire.SessionToken) > 4096 {
		return Credentials{}, ErrInvalid
	}
	for _, value := range []string{wire.AccessKeyID, wire.SecretAccessKey, wire.SessionToken} {
		if strings.ContainsAny(value, "\x00\r\n") {
			return Credentials{}, ErrInvalid
		}
	}
	return Credentials{[]byte(wire.AccessKeyID), []byte(wire.SecretAccessKey), []byte(wire.SessionToken)}, nil
}

func DeterministicStackName(connectionID string) string {
	sum := sha256.Sum256([]byte("dirextalk-connection-stack/v1\x00" + connectionID))
	return "dirextalk-connection-" + hex.EncodeToString(sum[:12])
}
func EnvelopeAAD(sessionID, connectionID, expiresAt string) []byte {
	raw, _ := json.Marshal(struct {
		Schema       string `json:"schema"`
		SessionID    string `json:"session_id"`
		ConnectionID string `json:"connection_id"`
		ExpiresAt    string `json:"expires_at"`
	}{UploadEnvelopeSchema, sessionID, connectionID, expiresAt})
	return raw
}
func envelopeDigest(envelope UploadEnvelope) string {
	raw, _ := json.Marshal(envelope)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func clientRequestToken(sessionID, connectionID, digest string) string {
	sum := sha256.Sum256([]byte("dirextalk-create-stack/v1\x00" + sessionID + "\x00" + connectionID + "\x00" + digest))
	return "dtx-" + hex.EncodeToString(sum[:])
}
func validEd25519(value string) bool {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		return false
	}
	public, err := x509.ParsePKIXPublicKey(raw)
	if err != nil {
		return false
	}
	_, ok := public.(ed25519.PublicKey)
	return ok
}
func namedDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && isLowerHex(value[7:])
}
func isLowerHex(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}
func validHTTPSURL(value string) bool {
	parsed, err := url.Parse(value)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil && parsed.Fragment == "" && parsed.RawQuery == ""
}
func strictDecode(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > maxJSONBytes || rejectDuplicateKeys(raw) != nil {
		return ErrInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalid
	}
	return nil
}
func rejectDuplicateKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := scanJSON(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return ErrInvalid
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
	if delim == '{' {
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			key, ok := keyToken.(string)
			if err != nil || !ok {
				return ErrInvalid
			}
			if _, exists := seen[key]; exists {
				return ErrInvalid
			}
			seen[key] = struct{}{}
			if scanJSON(decoder) != nil {
				return ErrInvalid
			}
		}
	} else if delim == '[' {
		for decoder.More() {
			if scanJSON(decoder) != nil {
				return ErrInvalid
			}
		}
	} else {
		return ErrInvalid
	}
	end, err := decoder.Token()
	if err != nil || (delim == '{' && end != json.Delim('}')) || (delim == '[' && end != json.Delim(']')) {
		return ErrInvalid
	}
	return nil
}
func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
