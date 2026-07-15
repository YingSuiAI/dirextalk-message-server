package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
)

const (
	approvalSchemaVersion  = "cloud-orchestrator/v1"
	approvalPayloadVersion = "approval-signing-payload/v1"
	approvalHashAlgorithm  = "deterministic-cbor-sha256"
	DeploymentCreateSchema = "dirextalk.aws.deployment-create/v1"
)

var (
	approvalIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	approvalSecretRefPattern  = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	approvalSecretPatterns    = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`),
		regexp.MustCompile(`(?i)aws_secret_access_key\s*[:=]`),
		regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`),
		regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
		regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
		regexp.MustCompile(`\b(?:sk|hf)_[A-Za-z0-9_-]{20,}\b`),
	}
)

type ApprovalResourceScope struct {
	Region            string             `json:"region"`
	AvailabilityZones []string           `json:"availability_zones,omitempty"`
	InstanceType      string             `json:"instance_type"`
	Architecture      string             `json:"architecture"`
	VCPU              uint16             `json:"vcpu"`
	MemoryMiB         uint32             `json:"memory_mib"`
	GPUCount          uint16             `json:"gpu_count,omitempty"`
	GPUMemoryMiB      uint32             `json:"gpu_memory_mib,omitempty"`
	DiskGiB           uint32             `json:"disk_gib"`
	PurchaseOption    string             `json:"purchase_option"`
	Spot              *ApprovalSpotScope `json:"spot,omitempty"`
}

type ApprovalSpotScope struct {
	CheckpointRequired bool   `json:"checkpoint_required"`
	MaxRetries         uint16 `json:"max_retries"`
}

type ApprovalNetworkScope struct {
	PublicIngress          bool                  `json:"public_ingress"`
	EntryPoint             string                `json:"entry_point"`
	TLSRequired            bool                  `json:"tls_required"`
	AuthenticationRequired bool                  `json:"authentication_required"`
	Ingress                []ApprovalIngressRule `json:"ingress,omitempty"`
}

type ApprovalIngressRule struct {
	Protocol string `json:"protocol"`
	Port     uint16 `json:"port"`
	Purpose  string `json:"purpose"`
}

type ApprovalSecretReference struct {
	SecretRef string `json:"secret_ref"`
	Purpose   string `json:"purpose"`
	Delivery  string `json:"delivery"`
}

type ApprovalIntegrationScope struct {
	Kind string `json:"kind"`
	Name string `json:"name"`
}

// ApprovalProof is byte-compatible with the ProductCore ApprovalV1. It exists
// in the user-owned Stack solely to verify a device signature immediately
// before one-time consumption; it contains references, never secret values.
type ApprovalProof struct {
	SchemaVersion     string                     `json:"schema_version"`
	ApprovalID        string                     `json:"approval_id"`
	ChallengeID       string                     `json:"challenge_id"`
	SignerKeyID       string                     `json:"signer_key_id"`
	PlanID            string                     `json:"plan_id"`
	PlanHash          string                     `json:"plan_hash"`
	PlanRevision      uint64                     `json:"plan_revision"`
	QuoteID           string                     `json:"quote_id"`
	QuoteDigest       string                     `json:"quote_digest"`
	QuoteValidUntil   time.Time                  `json:"quote_valid_until"`
	CloudConnectionID string                     `json:"cloud_connection_id"`
	RecipeDigest      string                     `json:"recipe_digest"`
	ResourceScope     ApprovalResourceScope      `json:"resource_scope"`
	NetworkScope      ApprovalNetworkScope       `json:"network_scope"`
	SecretScope       []ApprovalSecretReference  `json:"secret_scope"`
	IntegrationScope  []ApprovalIntegrationScope `json:"integration_scope"`
	ExpiresAt         time.Time                  `json:"expires_at"`
	Signature         string                     `json:"signature,omitempty"`
}

type approvalSigningPayload struct {
	SchemaVersion     string                     `json:"schema_version"`
	PayloadVersion    string                     `json:"payload_version"`
	HashAlgorithm     string                     `json:"hash_algorithm"`
	ApprovalID        string                     `json:"approval_id"`
	ChallengeID       string                     `json:"challenge_id"`
	SignerKeyID       string                     `json:"signer_key_id"`
	PlanID            string                     `json:"plan_id"`
	PlanHash          string                     `json:"plan_hash"`
	PlanRevision      uint64                     `json:"plan_revision"`
	QuoteID           string                     `json:"quote_id"`
	QuoteDigest       string                     `json:"quote_digest"`
	QuoteValidUntil   time.Time                  `json:"quote_valid_until"`
	CloudConnectionID string                     `json:"cloud_connection_id"`
	RecipeDigest      string                     `json:"recipe_digest"`
	ResourceScope     ApprovalResourceScope      `json:"resource_scope"`
	NetworkScope      ApprovalNetworkScope       `json:"network_scope"`
	SecretScope       []ApprovalSecretReference  `json:"secret_scope"`
	IntegrationScope  []ApprovalIntegrationScope `json:"integration_scope"`
	ExpiresAt         time.Time                  `json:"expires_at"`
}

func ParseApprovalProof(raw []byte) (ApprovalProof, error) {
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, []string{"schema_version", "approval_id", "challenge_id", "signer_key_id", "plan_id", "plan_hash", "plan_revision", "quote_id", "quote_digest", "quote_valid_until", "cloud_connection_id", "recipe_digest", "resource_scope", "network_scope", "secret_scope", "integration_scope", "expires_at", "signature"}) {
		return ApprovalProof{}, errCode("invalid_approval_proof")
	}
	if err := validateApprovalJSONShape(fields); err != nil {
		return ApprovalProof{}, err
	}
	var proof ApprovalProof
	if err := decodeSingle(raw, &proof); err != nil || proof.validate() != nil {
		return ApprovalProof{}, errCode("invalid_approval_proof")
	}
	return proof, nil
}

func validateApprovalJSONShape(fields map[string]json.RawMessage) error {
	resource, err := exactJSONObject(fields["resource_scope"])
	if err != nil || !requiredAllowedFields(resource, []string{"region", "availability_zones", "instance_type", "architecture", "vcpu", "memory_mib", "disk_gib", "purchase_option"}, []string{"gpu_count", "gpu_memory_mib", "spot"}) {
		return errCode("invalid_approval_proof")
	}
	if raw, ok := resource["spot"]; ok {
		spot, spotErr := exactJSONObject(raw)
		if spotErr != nil || !exactFields(spot, []string{"checkpoint_required", "max_retries"}) {
			return errCode("invalid_approval_proof")
		}
	}
	network, err := exactJSONObject(fields["network_scope"])
	if err != nil || !requiredAllowedFields(network, []string{"public_ingress", "entry_point", "tls_required", "authentication_required"}, []string{"ingress"}) {
		return errCode("invalid_approval_proof")
	}
	if raw, ok := network["ingress"]; ok {
		if err := validateObjectArray(raw, []string{"protocol", "port", "purpose"}); err != nil {
			return err
		}
	}
	if err := validateNullableObjectArray(fields["secret_scope"], []string{"secret_ref", "purpose", "delivery"}); err != nil {
		return err
	}
	return validateNullableObjectArray(fields["integration_scope"], []string{"kind", "name"})
}

func requiredAllowedFields(fields map[string]json.RawMessage, required, optional []string) bool {
	allowed := make(map[string]struct{}, len(required)+len(optional))
	for _, name := range required {
		allowed[name] = struct{}{}
		if _, ok := fields[name]; !ok {
			return false
		}
	}
	for _, name := range optional {
		allowed[name] = struct{}{}
	}
	for name := range fields {
		if _, ok := allowed[name]; !ok {
			return false
		}
	}
	return true
}

func validateNullableObjectArray(raw json.RawMessage, fields []string) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	return validateObjectArray(raw, fields)
}

func validateObjectArray(raw json.RawMessage, fields []string) error {
	var values []json.RawMessage
	if err := decodeSingle(raw, &values); err != nil {
		return errCode("invalid_approval_proof")
	}
	for _, value := range values {
		object, err := exactJSONObject(value)
		if err != nil || !exactFields(object, fields) {
			return errCode("invalid_approval_proof")
		}
	}
	return nil
}

func (p ApprovalProof) validate() error {
	if p.SchemaVersion != approvalSchemaVersion || !approvalIdentifierPattern.MatchString(p.ApprovalID) || !approvalIdentifierPattern.MatchString(p.ChallengeID) || !approvalIdentifierPattern.MatchString(p.SignerKeyID) || !approvalIdentifierPattern.MatchString(p.PlanID) || !namedSHA256Pattern.MatchString(p.PlanHash) || p.PlanRevision == 0 || p.PlanRevision > uint64(maxSafeInteger) || !approvalIdentifierPattern.MatchString(p.QuoteID) || !namedSHA256Pattern.MatchString(p.QuoteDigest) || !approvalIdentifierPattern.MatchString(p.CloudConnectionID) || !namedSHA256Pattern.MatchString(p.RecipeDigest) {
		return errCode("invalid_approval_proof")
	}
	if p.QuoteValidUntil.IsZero() || p.ExpiresAt.IsZero() || p.ExpiresAt.After(p.QuoteValidUntil) || p.QuoteValidUntil.Location() != time.UTC || p.ExpiresAt.Location() != time.UTC {
		return errCode("invalid_approval_proof")
	}
	if !regionPattern.MatchString(p.ResourceScope.Region) || !instanceTypePattern.MatchString(p.ResourceScope.InstanceType) || (p.ResourceScope.Architecture != "amd64" && p.ResourceScope.Architecture != "arm64") || p.ResourceScope.VCPU == 0 || p.ResourceScope.MemoryMiB == 0 || p.ResourceScope.DiskGiB == 0 || (p.ResourceScope.GPUCount == 0) != (p.ResourceScope.GPUMemoryMiB == 0) || (p.ResourceScope.PurchaseOption != "on_demand" && p.ResourceScope.PurchaseOption != "spot") || !canonicalZones(p.ResourceScope.AvailabilityZones, p.ResourceScope.Region) {
		return errCode("invalid_approval_proof")
	}
	if p.ResourceScope.PurchaseOption == "on_demand" && p.ResourceScope.Spot != nil || p.ResourceScope.PurchaseOption == "spot" && (p.ResourceScope.Spot == nil || !p.ResourceScope.Spot.CheckpointRequired || p.ResourceScope.Spot.MaxRetries == 0) {
		return errCode("invalid_approval_proof")
	}
	if p.NetworkScope.PublicIngress || p.NetworkScope.EntryPoint != "none" || p.NetworkScope.TLSRequired || p.NetworkScope.AuthenticationRequired || len(p.NetworkScope.Ingress) != 0 {
		return errCode("approval_scope_not_enabled")
	}
	if len(p.SecretScope) > 64 || len(p.IntegrationScope) > 16 {
		return errCode("invalid_approval_proof")
	}
	for _, secret := range p.SecretScope {
		if !approvalSecretRefPattern.MatchString(secret.SecretRef) || !safeApprovalText(secret.SecretRef, 256) || !safeApprovalText(secret.Purpose, 160) || (secret.Delivery != "file" && secret.Delivery != "environment") {
			return errCode("invalid_approval_proof")
		}
	}
	for _, integration := range p.IntegrationScope {
		if !safeApprovalText(integration.Name, 160) || (integration.Kind != "mcp" && integration.Kind != "acp" && integration.Kind != "dirextalk_connector" && integration.Kind != "web") {
			return errCode("invalid_approval_proof")
		}
	}
	if !uniqueApprovalScopes(p.SecretScope, p.IntegrationScope) {
		return errCode("invalid_approval_proof")
	}
	signature, err := base64.RawURLEncoding.DecodeString(p.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return errCode("invalid_approval_proof")
	}
	return nil
}

func (p ApprovalProof) SigningPayload() ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	p = normalizeApprovalProof(p)
	return deterministicCBOR(approvalSigningPayload{
		SchemaVersion: p.SchemaVersion, PayloadVersion: approvalPayloadVersion, HashAlgorithm: approvalHashAlgorithm,
		ApprovalID: p.ApprovalID, ChallengeID: p.ChallengeID, SignerKeyID: p.SignerKeyID, PlanID: p.PlanID,
		PlanHash: p.PlanHash, PlanRevision: p.PlanRevision, QuoteID: p.QuoteID, QuoteDigest: p.QuoteDigest,
		QuoteValidUntil: p.QuoteValidUntil, CloudConnectionID: p.CloudConnectionID, RecipeDigest: p.RecipeDigest,
		ResourceScope: p.ResourceScope, NetworkScope: p.NetworkScope, SecretScope: p.SecretScope,
		IntegrationScope: p.IntegrationScope, ExpiresAt: p.ExpiresAt,
	})
}

func (p ApprovalProof) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errCode("invalid_approval_signature")
	}
	if !p.ExpiresAt.After(now.UTC()) || !p.QuoteValidUntil.After(now.UTC()) {
		return errCode("approval_expired")
	}
	payload, err := p.SigningPayload()
	if err != nil {
		return err
	}
	signature, _ := base64.RawURLEncoding.DecodeString(p.Signature)
	if !ed25519.Verify(publicKey, payload, signature) {
		return errCode("invalid_approval_signature")
	}
	return nil
}

func safeApprovalText(value string, max int) bool {
	if value == "" || len(value) > max || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	for _, pattern := range approvalSecretPatterns {
		if pattern.MatchString(value) {
			return false
		}
	}
	return true
}

func uniqueApprovalScopes(secrets []ApprovalSecretReference, integrations []ApprovalIntegrationScope) bool {
	seen := make(map[string]struct{}, len(secrets)+len(integrations))
	for _, item := range secrets {
		key := "s\x00" + item.SecretRef
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
	}
	for _, item := range integrations {
		key := "i\x00" + item.Kind + "\x00" + item.Name
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
	}
	return true
}

func (p ApprovalProof) PayloadSHA256() (string, error) {
	payload, err := p.SigningPayload()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func normalizeApprovalProof(p ApprovalProof) ApprovalProof {
	p.QuoteValidUntil, p.ExpiresAt = p.QuoteValidUntil.UTC(), p.ExpiresAt.UTC()
	p.ResourceScope.AvailabilityZones = append([]string(nil), p.ResourceScope.AvailabilityZones...)
	sort.Strings(p.ResourceScope.AvailabilityZones)
	p.NetworkScope.Ingress = append([]ApprovalIngressRule(nil), p.NetworkScope.Ingress...)
	sort.Slice(p.NetworkScope.Ingress, func(i, j int) bool {
		a, b := p.NetworkScope.Ingress[i], p.NetworkScope.Ingress[j]
		if a.Protocol == b.Protocol {
			if a.Port == b.Port {
				return a.Purpose < b.Purpose
			}
			return a.Port < b.Port
		}
		return a.Protocol < b.Protocol
	})
	p.SecretScope = append([]ApprovalSecretReference(nil), p.SecretScope...)
	sort.Slice(p.SecretScope, func(i, j int) bool {
		a, b := p.SecretScope[i], p.SecretScope[j]
		if a.SecretRef == b.SecretRef {
			if a.Purpose == b.Purpose {
				return a.Delivery < b.Delivery
			}
			return a.Purpose < b.Purpose
		}
		return a.SecretRef < b.SecretRef
	})
	p.IntegrationScope = append([]ApprovalIntegrationScope(nil), p.IntegrationScope...)
	sort.Slice(p.IntegrationScope, func(i, j int) bool {
		a, b := p.IntegrationScope[i], p.IntegrationScope[j]
		if a.Kind == b.Kind {
			return a.Name < b.Name
		}
		return a.Kind < b.Kind
	})
	return p
}

func deterministicCBOR(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var compatible any
	if err := decoder.Decode(&compatible); err != nil {
		return nil, err
	}
	compatible, err = convertCBORValue(compatible)
	if err != nil {
		return nil, err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, err
	}
	return mode.Marshal(compatible)
}

func convertCBORValue(value any) (any, error) {
	switch v := value.(type) {
	case nil, bool, string:
		return v, nil
	case json.Number:
		if strings.ContainsAny(v.String(), ".eE") {
			return nil, errCode("invalid_approval_proof")
		}
		if n, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
			return n, nil
		}
		n, err := strconv.ParseUint(v.String(), 10, 64)
		return n, err
	case []any:
		out := make([]any, len(v))
		for i := range v {
			converted, err := convertCBORValue(v[i])
			if err != nil {
				return nil, err
			}
			out[i] = converted
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			converted, err := convertCBORValue(item)
			if err != nil {
				return nil, err
			}
			out[key] = converted
		}
		return out, nil
	default:
		return nil, errCode("invalid_approval_proof")
	}
}
