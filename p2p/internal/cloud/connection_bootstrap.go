package cloud

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"
)

const (
	connectionStackStageName  = "prod"
	connectionStackGeneration = "1"
)

var (
	cloudIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{7,127}$`)
	cloudKeyIDPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	cloudRegionPattern     = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	namedSHA256Pattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	stackARNPattern        = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):cloudformation:([a-z0-9-]+):([0-9]{12}):stack/([A-Za-z][A-Za-z0-9-]{0,127})/[A-Za-z0-9-]{8,128}$`)
)

// ValidateConnectionStackConfig rejects an incomplete or malformed public
// handoff configuration before it can produce a CloudFormation plan. The
// caller owns where these non-secret values are configured; this module never
// reads a node private key or any AWS credential.
func ValidateConnectionStackConfig(config ConnectionStackConfig) error {
	if !namedSHA256Pattern.MatchString(config.TemplateDigest) {
		return errors.New("cloud connection stack template digest is invalid")
	}
	if !namedSHA256Pattern.MatchString(config.SourceTreeDigest) {
		return errors.New("cloud connection stack source-tree digest is invalid")
	}
	parsed, err := url.ParseRequestURI(strings.TrimSpace(config.TemplateURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || len(config.TemplateURL) > 2048 {
		return errors.New("cloud connection stack template URL is invalid")
	}
	if !cloudKeyIDPattern.MatchString(config.NodeKeyID) {
		return errors.New("cloud connection stack node key id is invalid")
	}
	if err := validateEd25519SPKIBase64(config.NodePublicKeySPKIBase64); err != nil {
		return errors.New("cloud connection stack node public key is invalid")
	}
	if config.RolePlanTTL <= 0 || config.RolePlanTTL > 24*60*60*1000*1000*1000 {
		return errors.New("cloud connection stack role-plan lifetime is invalid")
	}
	return nil
}

func validateConnectionBootstrap(bootstrap ConnectionBootstrap) error {
	if !cloudIdentifierPattern.MatchString(bootstrap.BootstrapID) || !cloudIdentifierPattern.MatchString(bootstrap.ConnectionID) ||
		strings.TrimSpace(bootstrap.OwnerMXID) == "" || bootstrap.Provider != "aws" || !cloudRegionPattern.MatchString(bootstrap.RequestedRegion) ||
		bootstrap.StackName == "" || len(bootstrap.StackName) > 128 || !cloudKeyIDPattern.MatchString(bootstrap.NodeKeyID) ||
		!cloudKeyIDPattern.MatchString(bootstrap.DeviceApprovalKeyID) || bootstrap.Status != ConnectionBootstrapAwaitingStack || bootstrap.Revision != 1 ||
		bootstrap.IdempotencyHash == "" || bootstrap.RequestDigest == "" || bootstrap.ExpiresAt <= bootstrap.CreatedAt || bootstrap.CreatedAt <= 0 || bootstrap.UpdatedAt != bootstrap.CreatedAt || bootstrap.NextNodeCounter != 0 {
		return errors.New("cloud connection bootstrap is invalid")
	}
	// The role-plan lifetime is represented by ExpiresAt here. Validate the
	// immutable template and key identity directly instead of inventing a TTL
	// merely to reuse the public configuration validator.
	if !namedSHA256Pattern.MatchString(bootstrap.TemplateDigest) || !namedSHA256Pattern.MatchString(bootstrap.SourceTreeDigest) || !validTemplateURL(bootstrap.TemplateURL) ||
		!cloudKeyIDPattern.MatchString(bootstrap.NodeKeyID) || validateEd25519SPKIBase64(bootstrap.NodePublicKeySPKIBase64) != nil {
		return errors.New("cloud connection bootstrap stack identity is invalid")
	}
	if err := validateEd25519SPKIBase64(bootstrap.DeviceApprovalPublicKeySPKIBase64); err != nil {
		return errors.New("cloud connection bootstrap device approval key is invalid")
	}
	return nil
}

func validTemplateURL(raw string) bool {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.Hostname() != "" && parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" && parsed.RawPath == "" && len(raw) <= 2048
}

func validateEd25519SPKIBase64(value string) error {
	if value == "" || strings.TrimSpace(value) != value || len(value) > 256 {
		return errors.New("public key encoding is invalid")
	}
	der, err := base64.StdEncoding.DecodeString(value)
	if err != nil || len(der) == 0 {
		return errors.New("public key encoding is invalid")
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	key, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok || len(key) != ed25519.PublicKeySize {
		return errors.New("public key encoding is invalid")
	}
	return nil
}

func (bootstrap ConnectionBootstrap) RolePlan() ConnectionRolePlan {
	return ConnectionRolePlan{
		BootstrapID:                  bootstrap.BootstrapID,
		CloudConnectionID:            bootstrap.ConnectionID,
		Provider:                     bootstrap.Provider,
		Region:                       bootstrap.RequestedRegion,
		Status:                       bootstrap.Status,
		Revision:                     bootstrap.Revision,
		ExpiresAt:                    bootstrap.ExpiresAt,
		TemplateURL:                  bootstrap.TemplateURL,
		TemplateDigest:               bootstrap.TemplateDigest,
		SourceTreeDigest:             bootstrap.SourceTreeDigest,
		StackName:                    bootstrap.StackName,
		AllowRootCredentialBootstrap: bootstrap.AllowRootCredentialBootstrap,
		CloudFormationParams: map[string]string{
			"ConnectionId":                      bootstrap.ConnectionID,
			"ConnectionGeneration":              connectionStackGeneration,
			"NodeKeyId":                         bootstrap.NodeKeyID,
			"NodePublicKeySpkiBase64":           bootstrap.NodePublicKeySPKIBase64,
			"DeviceApprovalKeyId":               bootstrap.DeviceApprovalKeyID,
			"DeviceApprovalPublicKeySpkiBase64": bootstrap.DeviceApprovalPublicKeySPKIBase64,
			"StageName":                         connectionStackStageName,
		},
	}
}

func (bootstrap ConnectionBootstrap) Registration() ConnectionRegistration {
	return ConnectionRegistration{
		BootstrapID: bootstrap.BootstrapID, CloudConnectionID: bootstrap.ConnectionID,
		Status: bootstrap.Status, Revision: bootstrap.Revision, JobID: bootstrap.JobID,
	}
}

// ValidateConnectionRegistrationEndpoint accepts only the exact regional API
// Gateway hostname and /prod/v2/commands path emitted by Connection Stack V2.
// This is deliberately stricter than the quote transport because it guards the
// first persisted endpoint against client-controlled SSRF.
func ValidateConnectionRegistrationEndpoint(raw, requestedRegion string) error {
	if !cloudRegionPattern.MatchString(requestedRegion) || len(raw) == 0 || len(raw) > 2048 || strings.TrimSpace(raw) != raw {
		return errors.New("cloud broker endpoint is invalid")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.Port() != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Path != "/"+connectionStackStageName+"/v2/commands" {
		return errors.New("cloud broker endpoint is invalid")
	}
	hostname := strings.ToLower(parsed.Hostname())
	suffix := ".amazonaws.com"
	if strings.HasPrefix(requestedRegion, "cn-") {
		suffix = ".amazonaws.com.cn"
	}
	pattern := regexp.MustCompile(`^[a-z0-9]{10}\.execute-api\.` + regexp.QuoteMeta(requestedRegion) + regexp.QuoteMeta(suffix) + `$`)
	if !pattern.MatchString(hostname) {
		return errors.New("cloud broker endpoint is invalid")
	}
	return nil
}

// ValidateConnectionRegistrationStackARN constrains the untrusted stack output
// to the same requested Region. The Broker later derives and compares the
// account/stack facts from its own CloudFormation execution environment.
func ValidateConnectionRegistrationStackARN(raw, requestedRegion string) error {
	if len(raw) == 0 || len(raw) > 512 || strings.TrimSpace(raw) != raw || strings.ContainsFunc(raw, unicode.IsControl) {
		return errors.New("cloud stack ARN is invalid")
	}
	matches := stackARNPattern.FindStringSubmatch(raw)
	if matches == nil || matches[2] != requestedRegion {
		return errors.New("cloud stack ARN is invalid")
	}
	partition := matches[1]
	if strings.HasPrefix(requestedRegion, "cn-") && partition != "aws-cn" {
		return errors.New("cloud stack ARN is invalid")
	}
	if strings.Contains(requestedRegion, "-gov-") && partition != "aws-us-gov" {
		return errors.New("cloud stack ARN is invalid")
	}
	if !strings.HasPrefix(requestedRegion, "cn-") && !strings.Contains(requestedRegion, "-gov-") && partition != "aws" {
		return errors.New("cloud stack ARN is invalid")
	}
	return nil
}

func connectionBootstrapRequestDigest(provider, region, deviceKeyID, devicePublicKey string, allowRootCredentialBootstrap bool) string {
	return digestFields(provider, region, deviceKeyID, devicePublicKey, fmt.Sprintf("%t", allowRootCredentialBootstrap))
}

func connectionBootstrapCompletionDigest(bootstrapID, brokerCommandURL, stackARN string) string {
	return digestFields(bootstrapID, brokerCommandURL, stackARN)
}

func connectionStackName(connectionID string) string {
	connectionID = strings.ToLower(strings.TrimSpace(connectionID))
	connectionID = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return '-'
	}, connectionID)
	connectionID = strings.Trim(connectionID, "-")
	if connectionID == "" {
		connectionID = "connection"
	}
	name := "dirextalk-" + connectionID
	if len(name) > 128 {
		name = name[:128]
	}
	return name
}

func bootstrapError(code string) error {
	return fmt.Errorf("cloud connection bootstrap: %s", code)
}
