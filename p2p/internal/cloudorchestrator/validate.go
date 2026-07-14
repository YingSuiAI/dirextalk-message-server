package cloudorchestrator

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	digestPattern     = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	currencyPattern   = regexp.MustCompile(`^[A-Z]{3}$`)
	awsRegionPattern  = regexp.MustCompile(`^(af|ap|ca|cn|eu|il|me|mx|sa|us)(-gov)?-[a-z]+-[0-9]$`)
	secretRefPattern  = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	secretPatterns    = []*regexp.Regexp{
		regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`),
		regexp.MustCompile(`(?i)aws_secret_access_key\s*[:=]`),
		regexp.MustCompile(`-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----`),
		regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`),
		regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
		regexp.MustCompile(`\b(?:sk|hf)_[A-Za-z0-9_-]{20,}\b`),
		regexp.MustCompile(`\bxox[baprs]-[A-Za-z0-9-]{20,}\b`),
	}
)

func (r RecipeV1) Validate() error {
	if err := validateSchema(r.SchemaVersion); err != nil {
		return err
	}
	if err := validateIdentifier("recipe_id", r.RecipeID); err != nil {
		return err
	}
	if err := validateText("recipe name", r.Name, 160); err != nil {
		return err
	}
	if !validRecipeMaturity(r.Maturity) {
		return fmt.Errorf("recipe maturity %q is invalid", r.Maturity)
	}
	if len(r.Sources) == 0 || len(r.Sources) > 16 {
		return errors.New("recipe must declare 1 to 16 sources")
	}
	seenSources := make(map[string]struct{}, len(r.Sources))
	for index, source := range r.Sources {
		if err := source.validate(); err != nil {
			return fmt.Errorf("recipe source %d: %w", index, err)
		}
		key := source.URL + "\x00" + source.Commit + "\x00" + source.ArtifactDigest
		if _, found := seenSources[key]; found {
			return errors.New("recipe sources must not contain duplicates")
		}
		seenSources[key] = struct{}{}
	}
	if err := r.Requirements.validate(); err != nil {
		return fmt.Errorf("recipe requirements: %w", err)
	}
	if err := r.Install.validate(); err != nil {
		return fmt.Errorf("recipe install contract: %w", err)
	}
	if err := r.Health.validate(); err != nil {
		return fmt.Errorf("recipe health contract: %w", err)
	}
	if err := r.Lifecycle.validate(); err != nil {
		return fmt.Errorf("recipe lifecycle contract: %w", err)
	}
	return nil
}

func (s RecipeSourceV1) validate() error {
	parsed, err := url.Parse(s.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("url must be an absolute https URL")
	}
	if parsed.User != nil {
		return errors.New("url must not include user credentials")
	}
	for key := range parsed.Query() {
		if credentialQueryKey(key) {
			return errors.New("url must not include a credential query parameter")
		}
	}
	if err := validateText("source URL", s.URL, 2048); err != nil {
		return err
	}
	if err := validateText("source version", s.Version, 160); err != nil {
		return err
	}
	if err := validateText("source commit", s.Commit, 160); err != nil {
		return err
	}
	if err := validateDigest("source artifact_digest", s.ArtifactDigest); err != nil {
		return err
	}
	if err := validateText("source license", s.License, 160); err != nil {
		return err
	}
	if s.RetrievedAt.IsZero() {
		return errors.New("retrieved_at is required")
	}
	return nil
}

func (r ResourceRequirementsV1) validate() error {
	if r.MinVCPU == 0 || r.MinMemoryMiB == 0 || r.MinDiskGiB == 0 {
		return errors.New("minimum vcpu, memory, and disk must be positive")
	}
	if !validArchitecture(r.Architecture) {
		return fmt.Errorf("architecture %q is invalid", r.Architecture)
	}
	if r.MinGPUCount == 0 && r.MinGPUMemoryMiB != 0 {
		return errors.New("gpu memory requires a positive gpu count")
	}
	if r.MinGPUCount > 0 && r.MinGPUMemoryMiB == 0 {
		return errors.New("gpu count requires positive gpu memory")
	}
	return nil
}

func (c InstallContractV1) validate() error {
	if c.TimeoutSeconds == 0 || c.TimeoutSeconds > 24*60*60 {
		return errors.New("timeout_seconds must be between 1 and 86400")
	}
	if err := validateStringSet("checkpoint_names", c.CheckpointNames, 1, 32, 96); err != nil {
		return err
	}
	if err := validateStringSet("allowed_adaptations", c.AllowedAdaptations, 0, 64, 160); err != nil {
		return err
	}
	if len(c.Steps) == 0 || len(c.Steps) > 64 {
		return errors.New("steps must contain 1 to 64 entries")
	}
	seen := make(map[string]struct{}, len(c.Steps))
	for index, step := range c.Steps {
		if err := validateIdentifier("step id", step.ID); err != nil {
			return fmt.Errorf("step %d: %w", index, err)
		}
		if err := validateText("step summary", step.Summary, 500); err != nil {
			return fmt.Errorf("step %d: %w", index, err)
		}
		if step.TimeoutSeconds == 0 || step.TimeoutSeconds > c.TimeoutSeconds {
			return fmt.Errorf("step %d: timeout_seconds must be positive and within install timeout", index)
		}
		if _, found := seen[step.ID]; found {
			return errors.New("steps must not contain duplicate ids")
		}
		seen[step.ID] = struct{}{}
	}
	return nil
}

func (h HealthContractV1) validate() error {
	for name, probe := range map[string]ProbeV1{"liveness": h.Liveness, "readiness": h.Readiness, "semantic": h.Semantic} {
		if err := probe.validate(); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func (p ProbeV1) validate() error {
	if p.Kind != ProbeHTTP && p.Kind != ProbeCommand {
		return fmt.Errorf("kind %q is invalid", p.Kind)
	}
	if err := validateText("probe target", p.Target, 512); err != nil {
		return err
	}
	if p.Kind == ProbeHTTP && !strings.HasPrefix(p.Target, "/") {
		return errors.New("http probe target must be an absolute path")
	}
	if p.Kind == ProbeCommand && !identifierPattern.MatchString(p.Target) {
		return errors.New("command probe target must be a recipe action identifier")
	}
	return nil
}

func (l LifecycleContractV1) validate() error {
	for name, identifier := range map[string]string{
		"start": l.Start, "stop": l.Stop, "restart": l.Restart, "upgrade": l.Upgrade,
		"rollback": l.Rollback, "backup": l.Backup, "restore": l.Restore, "destroy": l.Destroy,
	} {
		if err := validateIdentifier(name, identifier); err != nil {
			return err
		}
	}
	return nil
}

func (d ResearchDraftV1) Validate() error {
	if err := validateSchema(d.SchemaVersion); err != nil {
		return err
	}
	if err := validateAWSRegion(d.Region); err != nil {
		return err
	}
	return validateQuoteRequestCandidates("research draft", d.Candidates)
}

func (q QuoteRequestV1) Validate() error {
	if err := validateSchema(q.SchemaVersion); err != nil {
		return err
	}
	if err := validateIdentifier("quote_request_id", q.QuoteRequestID); err != nil {
		return err
	}
	if err := validateIdentifier("plan_id", q.PlanID); err != nil {
		return err
	}
	if q.PlanRevision == 0 {
		return errors.New("plan_revision must be positive")
	}
	if err := validateIdentifier("cloud_connection_id", q.CloudConnectionID); err != nil {
		return err
	}
	if err := validateDigest("recipe_digest", q.RecipeDigest); err != nil {
		return err
	}
	if err := validateAWSRegion(q.Region); err != nil {
		return err
	}
	return validateQuoteRequestCandidates("quote request", q.Candidates)
}

func validateQuoteRequestCandidates(scope string, candidates []QuoteRequestCandidateV1) error {
	if len(candidates) < 1 || len(candidates) > 3 {
		return fmt.Errorf("%s candidates must contain 1 to 3 entries", scope)
	}
	seenIDs := make(map[string]struct{}, len(candidates))
	seenTiers := make(map[QuoteTier]struct{}, len(candidates))
	for index, candidate := range candidates {
		if err := candidate.validate(); err != nil {
			return fmt.Errorf("%s candidate %d: %w", scope, index, err)
		}
		if _, found := seenIDs[candidate.CandidateID]; found {
			return fmt.Errorf("%s candidates must not duplicate candidate_id", scope)
		}
		if _, found := seenTiers[candidate.Tier]; found {
			return fmt.Errorf("%s candidates must not duplicate tier", scope)
		}
		seenIDs[candidate.CandidateID] = struct{}{}
		seenTiers[candidate.Tier] = struct{}{}
	}
	return nil
}

func validateAWSRegion(value string) error {
	if !awsRegionPattern.MatchString(value) {
		return errors.New("region must be an AWS region")
	}
	return nil
}

func (c QuoteRequestCandidateV1) validate() error {
	if err := validateIdentifier("candidate_id", c.CandidateID); err != nil {
		return err
	}
	if !validQuoteTier(c.Tier) {
		return fmt.Errorf("tier %q is invalid", c.Tier)
	}
	if err := validateIdentifier("instance_type", c.InstanceType); err != nil {
		return err
	}
	if c.PurchaseOption != PurchaseOnDemand {
		return errors.New("only on_demand quote request candidates are enabled")
	}
	if c.EstimatedDiskGiB < 8 || c.EstimatedDiskGiB > 16384 {
		return errors.New("estimated_disk_gib must be between 8 and 16384")
	}
	return nil
}

func (q QuoteV1) Validate() error {
	if err := validateSchema(q.SchemaVersion); err != nil {
		return err
	}
	if err := validateIdentifier("quote_id", q.QuoteID); err != nil {
		return err
	}
	if err := validateIdentifier("cloud_connection_id", q.CloudConnectionID); err != nil {
		return err
	}
	if err := validateIdentifier("region", q.Region); err != nil {
		return err
	}
	if !currencyPattern.MatchString(q.Currency) {
		return errors.New("currency must be a three-letter ISO code")
	}
	if q.QuotedAt.IsZero() || q.ValidUntil.IsZero() || !q.ValidUntil.After(q.QuotedAt) {
		return errors.New("quoted_at must precede valid_until")
	}
	if q.ValidUntil.Sub(q.QuotedAt) > 15*time.Minute {
		return errors.New("quote validity may not exceed 15 minutes")
	}
	if len(q.Candidates) == 0 || len(q.Candidates) > 3 {
		return errors.New("quote must contain 1 to 3 candidates")
	}
	seenIDs := make(map[string]struct{}, len(q.Candidates))
	seenTiers := make(map[QuoteTier]struct{}, len(q.Candidates))
	for index, candidate := range q.Candidates {
		if err := candidate.validate(); err != nil {
			return fmt.Errorf("quote candidate %d: %w", index, err)
		}
		if _, found := seenIDs[candidate.CandidateID]; found {
			return errors.New("quote candidates must not duplicate candidate_id")
		}
		if _, found := seenTiers[candidate.Tier]; found {
			return errors.New("quote candidates must not duplicate tier")
		}
		seenIDs[candidate.CandidateID] = struct{}{}
		seenTiers[candidate.Tier] = struct{}{}
	}
	if err := validateStringSet("included_items", q.IncludedItems, 0, 64, 500); err != nil {
		return err
	}
	if err := validateStringSet("unincluded_items", q.UnincludedItems, 0, 64, 500); err != nil {
		return err
	}
	included := make(map[string]struct{}, len(q.IncludedItems))
	for _, item := range q.IncludedItems {
		included[item] = struct{}{}
	}
	for _, item := range q.UnincludedItems {
		if _, found := included[item]; found {
			return errors.New("included_items and unincluded_items must not overlap")
		}
	}
	return nil
}

func (c QuoteCandidateV1) validate() error {
	if err := validateIdentifier("candidate_id", c.CandidateID); err != nil {
		return err
	}
	if !validQuoteTier(c.Tier) {
		return fmt.Errorf("tier %q is invalid", c.Tier)
	}
	if err := validateIdentifier("instance_type", c.InstanceType); err != nil {
		return err
	}
	if !validPurchaseOption(c.PurchaseOption) {
		return fmt.Errorf("purchase_option %q is invalid", c.PurchaseOption)
	}
	if !validArchitecture(c.Architecture) {
		return fmt.Errorf("architecture %q is invalid", c.Architecture)
	}
	if c.VCPU == 0 || c.MemoryMiB == 0 {
		return errors.New("vcpu and memory_mib must be positive")
	}
	if c.GPUCount == 0 && c.GPUMemoryMiB != 0 {
		return errors.New("gpu memory requires a positive gpu count")
	}
	if c.GPUCount > 0 && c.GPUMemoryMiB == 0 {
		return errors.New("gpu count requires positive gpu memory")
	}
	if c.HourlyMinor < 0 || c.ThirtyDayMinor < 0 || c.StartupUpperMinor < 0 {
		return errors.New("monetary values may not be negative")
	}
	if c.EstimatedDiskGiB == 0 {
		return errors.New("estimated_disk_gib must be positive")
	}
	if err := validateStringSet("availability_zones", c.AvailabilityZones, 0, 3, 128); err != nil {
		return err
	}
	return nil
}

func (p PlanV1) Validate() error {
	if err := validateSchema(p.SchemaVersion); err != nil {
		return err
	}
	if err := validateIdentifier("plan_id", p.PlanID); err != nil {
		return err
	}
	if p.Revision == 0 {
		return errors.New("plan revision must be positive")
	}
	if !validPlanStatus(p.Status) {
		return fmt.Errorf("plan status %q is invalid", p.Status)
	}
	if err := validateIdentifier("cloud_connection_id", p.CloudConnectionID); err != nil {
		return err
	}
	if err := p.Recipe.validate(); err != nil {
		return fmt.Errorf("recipe binding: %w", err)
	}
	if err := p.Quote.validate(); err != nil {
		return fmt.Errorf("quote binding: %w", err)
	}
	if err := p.ResourceScope.validate(); err != nil {
		return fmt.Errorf("resource scope: %w", err)
	}
	if err := p.NetworkScope.validate(); err != nil {
		return fmt.Errorf("network scope: %w", err)
	}
	if err := validateSecretScope(p.SecretScope); err != nil {
		return err
	}
	if err := validateIntegrationScope(p.IntegrationScope); err != nil {
		return err
	}
	return nil
}

func (b RecipeBindingV1) validate() error {
	if err := validateIdentifier("recipe_id", b.RecipeID); err != nil {
		return err
	}
	if err := validateDigest("recipe digest", b.Digest); err != nil {
		return err
	}
	if !validRecipeMaturity(b.Maturity) {
		return fmt.Errorf("maturity %q is invalid", b.Maturity)
	}
	return nil
}

func (b QuoteBindingV1) validate() error {
	if err := validateIdentifier("quote_id", b.QuoteID); err != nil {
		return err
	}
	if err := validateDigest("quote digest", b.Digest); err != nil {
		return err
	}
	if b.ValidUntil.IsZero() {
		return errors.New("valid_until is required")
	}
	if err := validateIdentifier("candidate_id", b.CandidateID); err != nil {
		return err
	}
	return nil
}

func (r ResourceScopeV1) validate() error {
	if err := validateIdentifier("region", r.Region); err != nil {
		return err
	}
	if err := validateStringSet("availability_zones", r.AvailabilityZones, 0, 3, 128); err != nil {
		return err
	}
	if err := validateIdentifier("instance_type", r.InstanceType); err != nil {
		return err
	}
	if !validArchitecture(r.Architecture) {
		return fmt.Errorf("architecture %q is invalid", r.Architecture)
	}
	if r.VCPU == 0 || r.MemoryMiB == 0 || r.DiskGiB == 0 {
		return errors.New("vcpu, memory_mib, and disk_gib must be positive")
	}
	if r.GPUCount == 0 && r.GPUMemoryMiB != 0 {
		return errors.New("gpu memory requires a positive gpu count")
	}
	if r.GPUCount > 0 && r.GPUMemoryMiB == 0 {
		return errors.New("gpu count requires positive gpu memory")
	}
	if !validPurchaseOption(r.PurchaseOption) {
		return fmt.Errorf("purchase_option %q is invalid", r.PurchaseOption)
	}
	if r.PurchaseOption == PurchaseSpot {
		if r.Spot == nil || !r.Spot.CheckpointRequired || r.Spot.MaxRetries == 0 {
			return errors.New("spot resource requires checkpoint_required and positive max_retries")
		}
	} else if r.Spot != nil {
		return errors.New("spot scope is only valid for spot purchase_option")
	}
	return nil
}

func (n NetworkScopeV1) validate() error {
	if !n.PublicIngress {
		if n.EntryPoint != EntryPointNone || n.TLSRequired || n.AuthenticationRequired || len(n.Ingress) != 0 {
			return errors.New("private-only network scope must not declare ingress or an entry point")
		}
		return nil
	}
	if n.EntryPoint != EntryPointALB && n.EntryPoint != EntryPointCloudFront && n.EntryPoint != EntryPointDirect {
		return errors.New("public ingress requires a concrete entry point")
	}
	if !n.TLSRequired || !n.AuthenticationRequired {
		return errors.New("public ingress requires TLS and authentication")
	}
	if len(n.Ingress) == 0 || len(n.Ingress) > 16 {
		return errors.New("public ingress requires 1 to 16 rules")
	}
	seen := make(map[string]struct{}, len(n.Ingress))
	for index, rule := range n.Ingress {
		if rule.Protocol != "https" || rule.Port != 443 {
			return fmt.Errorf("ingress rule %d must expose HTTPS on port 443", index)
		}
		if err := validateText("ingress purpose", rule.Purpose, 160); err != nil {
			return fmt.Errorf("ingress rule %d: %w", index, err)
		}
		key := rule.Protocol + "\x00" + fmt.Sprint(rule.Port) + "\x00" + rule.Purpose
		if _, found := seen[key]; found {
			return errors.New("ingress rules must not contain duplicates")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateSecretScope(scope []SecretReferenceV1) error {
	if len(scope) > 64 {
		return errors.New("secret_scope may contain at most 64 references")
	}
	seen := make(map[string]struct{}, len(scope))
	for index, reference := range scope {
		if !secretRefPattern.MatchString(reference.SecretRef) {
			return fmt.Errorf("secret scope %d: secret_ref must be an opaque secret_ref: identifier", index)
		}
		if err := rejectSecretMaterial("secret_ref", reference.SecretRef); err != nil {
			return fmt.Errorf("secret scope %d: %w", index, err)
		}
		if err := validateText("secret purpose", reference.Purpose, 160); err != nil {
			return fmt.Errorf("secret scope %d: %w", index, err)
		}
		if reference.Delivery != SecretDeliveryFile && reference.Delivery != SecretDeliveryEnvironment {
			return fmt.Errorf("secret scope %d: delivery %q is invalid", index, reference.Delivery)
		}
		if _, found := seen[reference.SecretRef]; found {
			return errors.New("secret_scope must not contain duplicate secret_ref values")
		}
		seen[reference.SecretRef] = struct{}{}
	}
	return nil
}

func validateIntegrationScope(scope []IntegrationScopeV1) error {
	if len(scope) > 16 {
		return errors.New("integration_scope may contain at most 16 entries")
	}
	seen := make(map[string]struct{}, len(scope))
	for index, integration := range scope {
		if !validIntegrationKind(integration.Kind) {
			return fmt.Errorf("integration scope %d: kind %q is invalid", index, integration.Kind)
		}
		if err := validateText("integration name", integration.Name, 160); err != nil {
			return fmt.Errorf("integration scope %d: %w", index, err)
		}
		key := string(integration.Kind) + "\x00" + integration.Name
		if _, found := seen[key]; found {
			return errors.New("integration_scope must not contain duplicates")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func (a ApprovalV1) Validate() error {
	return a.validate(false)
}

// ValidateAt also rejects an already expired approval. Use this immediately
// before a typed Broker command is accepted.
func (a ApprovalV1) ValidateAt(now time.Time) error {
	if err := a.validate(true); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("approval has expired")
	}
	return nil
}

func (a ApprovalV1) validate(requireSignature bool) error {
	if err := validateSchema(a.SchemaVersion); err != nil {
		return err
	}
	for label, value := range map[string]string{
		"approval_id": a.ApprovalID, "challenge_id": a.ChallengeID, "signer_key_id": a.SignerKeyID,
		"plan_id": a.PlanID, "cloud_connection_id": a.CloudConnectionID, "quote_id": a.QuoteID,
	} {
		if err := validateIdentifier(label, value); err != nil {
			return err
		}
	}
	if err := validateDigest("plan_hash", a.PlanHash); err != nil {
		return err
	}
	if err := validateDigest("quote_digest", a.QuoteDigest); err != nil {
		return err
	}
	if err := validateDigest("recipe_digest", a.RecipeDigest); err != nil {
		return err
	}
	if a.PlanRevision == 0 {
		return errors.New("plan_revision must be positive")
	}
	if a.QuoteValidUntil.IsZero() || a.ExpiresAt.IsZero() || a.ExpiresAt.After(a.QuoteValidUntil) {
		return errors.New("approval expiry must be present and no later than quote validity")
	}
	if err := a.ResourceScope.validate(); err != nil {
		return fmt.Errorf("resource scope: %w", err)
	}
	if err := a.NetworkScope.validate(); err != nil {
		return fmt.Errorf("network scope: %w", err)
	}
	if err := validateSecretScope(a.SecretScope); err != nil {
		return err
	}
	if err := validateIntegrationScope(a.IntegrationScope); err != nil {
		return err
	}
	if requireSignature || a.Signature != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(a.Signature)
		if err != nil || len(decoded) != 64 {
			return errors.New("approval signature must be a base64url Ed25519 signature")
		}
	}
	return nil
}

func validateSchema(version string) error {
	if version != SchemaVersionV1 {
		return fmt.Errorf("schema_version must be %q", SchemaVersionV1)
	}
	return nil
}

func validateIdentifier(label, value string) error {
	if !identifierPattern.MatchString(value) {
		return fmt.Errorf("%s is invalid", label)
	}
	return rejectSecretMaterial(label, value)
}

func validateDigest(label, value string) error {
	if !digestPattern.MatchString(value) {
		return fmt.Errorf("%s must be a sha256 digest", label)
	}
	return nil
}

func validateText(label, value string, maxRunes int) error {
	if value == "" || strings.TrimSpace(value) != value || len([]rune(value)) > maxRunes {
		return fmt.Errorf("%s must contain 1 to %d trimmed characters", label, maxRunes)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("%s must not contain control characters", label)
		}
	}
	return rejectSecretMaterial(label, value)
}

func validateStringSet(label string, values []string, min, max, maxRunes int) error {
	if len(values) < min || len(values) > max {
		return fmt.Errorf("%s must contain %d to %d entries", label, min, max)
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if err := validateText(label, value, maxRunes); err != nil {
			return err
		}
		if _, found := seen[value]; found {
			return fmt.Errorf("%s must not contain duplicates", label)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func rejectSecretMaterial(label, value string) error {
	for _, pattern := range secretPatterns {
		if pattern.MatchString(value) {
			return fmt.Errorf("%s must not contain credential material", label)
		}
	}
	return nil
}

func credentialQueryKey(key string) bool {
	key = strings.ToLower(key)
	if strings.HasPrefix(key, "x-amz-") {
		// Signed AWS URLs are bearer capabilities even when the query does not
		// literally contain a long-lived key. Recipe provenance must instead
		// refer to a stable source and its locked artifact digest.
		return true
	}
	for _, marker := range []string{"credential", "password", "secret", "signature", "token"} {
		if strings.Contains(key, marker) {
			return true
		}
	}
	return key == "key" || strings.HasSuffix(key, "_key") || strings.HasSuffix(key, "-key")
}

func validPlanStatus(status PlanStatus) bool {
	switch status {
	case PlanResearching, PlanQuoting, PlanReadyForConfirmation, PlanApproved, PlanExpired, PlanSuperseded:
		return true
	default:
		return false
	}
}

func validRecipeMaturity(maturity RecipeMaturity) bool {
	switch maturity {
	case RecipeExperimental, RecipeAwaitingManagementAccept, RecipeManaged:
		return true
	default:
		return false
	}
}

func validArchitecture(architecture Architecture) bool {
	return architecture == ArchitectureAMD64 || architecture == ArchitectureARM64
}

func validPurchaseOption(option PurchaseOption) bool {
	return option == PurchaseOnDemand || option == PurchaseSpot
}

func validQuoteTier(tier QuoteTier) bool {
	return tier == QuoteTierEconomy || tier == QuoteTierRecommended || tier == QuoteTierPerformance
}

func validIntegrationKind(kind IntegrationKind) bool {
	switch kind {
	case IntegrationMCP, IntegrationACP, IntegrationDirextalkConnector, IntegrationWeb:
		return true
	default:
		return false
	}
}

// canonicalSet returns a sorted copy after validation has ensured values are
// unique. It is used only for hash/signature encoding and never mutates a
// caller's plan or approval object.
func canonicalSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	copyValues := append([]string(nil), values...)
	sort.Strings(copyValues)
	return copyValues
}
