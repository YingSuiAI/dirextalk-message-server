package cloudorchestrator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// CanonicalRecipeCBOR emits RFC 8949 Core Deterministic CBOR for recipe
// digests. JSON tags are first converted to a JSON-compatible map so the
// cross-language field names remain snake_case rather than Go field names.
func (r RecipeV1) CanonicalRecipeCBOR() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeRecipe(r))
}

// Digest returns a content digest over the complete de-secretsed recipe.
func (r RecipeV1) Digest() (string, error) {
	canonical, err := r.CanonicalRecipeCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

// CanonicalResearchDraftCBOR emits RFC 8949 Core Deterministic CBOR for the
// restricted, non-price-bearing model research output.
func (d ResearchDraftV1) CanonicalResearchDraftCBOR() ([]byte, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeResearchDraft(d))
}

// Digest returns a deterministic-CBOR SHA-256 content digest for stable
// persistence-side quote_request_id derivation. It is not an approval or
// Broker payload binding; QuoteRequestV1.Digest provides that binding.
func (d ResearchDraftV1) Digest() (string, error) {
	canonical, err := d.CanonicalResearchDraftCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

// CanonicalQuoteRequestCBOR emits RFC 8949 Core Deterministic CBOR for the
// immutable pre-price quote request binding.
func (q QuoteRequestV1) CanonicalQuoteRequestCBOR() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeQuoteRequest(q))
}

// Digest returns the deterministic-CBOR SHA-256 digest used as the typed
// Broker quote payload's plan_digest. It intentionally does not call or bind
// PlanV1.Hash: a final approval plan is created only after the Broker returns
// a verified price estimate.
func (q QuoteRequestV1) Digest() (string, error) {
	canonical, err := q.CanonicalQuoteRequestCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

// CanonicalQuoteCBOR emits RFC 8949 Core Deterministic CBOR for quote
// digests. JSON tags are retained as the canonical cross-language names.
func (q QuoteV1) CanonicalQuoteCBOR() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return canonicalCBOR(normalizeQuote(q))
}

// Digest returns a content digest over the full price estimate and validity.
func (q QuoteV1) Digest() (string, error) {
	canonical, err := q.CanonicalQuoteCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

// CanonicalPlanCBOR emits exactly the immutable approval surface using RFC
// 8949 Core Deterministic CBOR. It omits mutable status/execution projections
// because they must not change the signed purchase decision.
func (p PlanV1) CanonicalPlanCBOR() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	normalized := normalizePlan(p)
	document := planHashDocumentV1{
		SchemaVersion:     normalized.SchemaVersion,
		HashAlgorithm:     HashAlgorithmDeterministicCBORSHA256,
		PlanID:            normalized.PlanID,
		Revision:          normalized.Revision,
		CloudConnectionID: normalized.CloudConnectionID,
		Recipe:            normalized.Recipe,
		Quote:             normalized.Quote,
		ResourceScope:     normalized.ResourceScope,
		NetworkScope:      normalized.NetworkScope,
		SecretScope:       normalized.SecretScope,
		IntegrationScope:  normalized.IntegrationScope,
	}
	return canonicalCBOR(document)
}

// Hash returns the deterministic-CBOR SHA-256 digest used for V1 approvals.
func (p PlanV1) Hash() (string, error) {
	canonical, err := p.CanonicalPlanCBOR()
	if err != nil {
		return "", err
	}
	return digestCanonicalCBOR(canonical), nil
}

type planHashDocumentV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	HashAlgorithm     string               `json:"hash_algorithm"`
	PlanID            string               `json:"plan_id"`
	Revision          uint64               `json:"revision"`
	CloudConnectionID string               `json:"cloud_connection_id"`
	Recipe            RecipeBindingV1      `json:"recipe"`
	Quote             QuoteBindingV1       `json:"quote"`
	ResourceScope     ResourceScopeV1      `json:"resource_scope"`
	NetworkScope      NetworkScopeV1       `json:"network_scope"`
	SecretScope       []SecretReferenceV1  `json:"secret_scope"`
	IntegrationScope  []IntegrationScopeV1 `json:"integration_scope"`
}

// canonicalCBOR makes JSON tags the wire schema first, then encodes the
// resulting value with RFC 8949 Core Deterministic CBOR. Decoding JSON with
// UseNumber and converting only integer JSON numbers avoids a float64 round
// trip for revisions, prices, ports, and resource sizes.
func canonicalCBOR(value any) ([]byte, error) {
	if err := rejectNativeFloatingPointValues(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal JSON-compatible CBOR document: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var jsonCompatible any
	if err := decoder.Decode(&jsonCompatible); err != nil {
		return nil, fmt.Errorf("decode JSON-compatible CBOR document: %w", err)
	}
	jsonCompatible, err = convertJSONCompatibleCBOR(jsonCompatible)
	if err != nil {
		return nil, err
	}
	mode, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, fmt.Errorf("configure core deterministic CBOR: %w", err)
	}
	canonical, err := mode.Marshal(jsonCompatible)
	if err != nil {
		return nil, fmt.Errorf("encode core deterministic CBOR: %w", err)
	}
	return canonical, nil
}

var timeValueType = reflect.TypeOf(time.Time{})

// rejectNativeFloatingPointValues prevents an integer-looking float such as
// float64(1) from becoming JSON integer 1 before UseNumber sees it. The V1
// contracts deliberately model all numeric values as integer types.
func rejectNativeFloatingPointValues(value reflect.Value) error {
	if !value.IsValid() || value.Type() == timeValueType {
		return nil
	}
	switch value.Kind() {
	case reflect.Float32, reflect.Float64:
		return fmt.Errorf("floating-point values are not permitted in deterministic CBOR contracts")
	case reflect.Interface, reflect.Pointer:
		if value.IsNil() {
			return nil
		}
		return rejectNativeFloatingPointValues(value.Elem())
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.PkgPath != "" { // encoding/json ignores unexported fields.
				continue
			}
			if err := rejectNativeFloatingPointValues(value.Field(index)); err != nil {
				return err
			}
		}
	case reflect.Array, reflect.Slice:
		for index := 0; index < value.Len(); index++ {
			if err := rejectNativeFloatingPointValues(value.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			if err := rejectNativeFloatingPointValues(iter.Key()); err != nil {
				return err
			}
			if err := rejectNativeFloatingPointValues(iter.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func convertJSONCompatibleCBOR(value any) (any, error) {
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		encoded := typed.String()
		if strings.ContainsAny(encoded, ".eE") {
			return nil, fmt.Errorf("JSON-compatible CBOR documents must not contain float values: %q", encoded)
		}
		if signed, err := strconv.ParseInt(encoded, 10, 64); err == nil {
			return signed, nil
		}
		unsigned, err := strconv.ParseUint(encoded, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("JSON-compatible CBOR integer %q is invalid: %w", encoded, err)
		}
		return unsigned, nil
	case []any:
		converted := make([]any, len(typed))
		for index, item := range typed {
			value, err := convertJSONCompatibleCBOR(item)
			if err != nil {
				return nil, err
			}
			converted[index] = value
		}
		return converted, nil
	case map[string]any:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			value, err := convertJSONCompatibleCBOR(item)
			if err != nil {
				return nil, err
			}
			converted[key] = value
		}
		return converted, nil
	default:
		return nil, fmt.Errorf("unsupported JSON-compatible CBOR type %T", value)
	}
}

func digestCanonicalCBOR(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func normalizeRecipe(recipe RecipeV1) RecipeV1 {
	normalized := recipe
	normalized.Sources = append([]RecipeSourceV1(nil), recipe.Sources...)
	for index := range normalized.Sources {
		normalized.Sources[index].RetrievedAt = normalized.Sources[index].RetrievedAt.UTC()
	}
	sort.Slice(normalized.Sources, func(i, j int) bool {
		left, right := normalized.Sources[i], normalized.Sources[j]
		return sourceSortKey(left) < sourceSortKey(right)
	})
	normalized.Install.CheckpointNames = canonicalSet(recipe.Install.CheckpointNames)
	normalized.Install.AllowedAdaptations = canonicalSet(recipe.Install.AllowedAdaptations)
	normalized.Install.Steps = append([]InstallStepV1(nil), recipe.Install.Steps...)
	return normalized
}

func sourceSortKey(source RecipeSourceV1) string {
	return source.URL + "\x00" + source.Commit + "\x00" + source.ArtifactDigest + "\x00" + source.Version
}

func normalizeResearchDraft(draft ResearchDraftV1) ResearchDraftV1 {
	normalized := draft
	normalized.Candidates = normalizeQuoteRequestCandidates(draft.Candidates)
	return normalized
}

func normalizeQuoteRequest(request QuoteRequestV1) QuoteRequestV1 {
	normalized := request
	normalized.Candidates = normalizeQuoteRequestCandidates(request.Candidates)
	return normalized
}

func normalizeQuoteRequestCandidates(candidates []QuoteRequestCandidateV1) []QuoteRequestCandidateV1 {
	if len(candidates) == 0 {
		return nil
	}
	normalized := append([]QuoteRequestCandidateV1(nil), candidates...)
	sort.Slice(normalized, func(i, j int) bool {
		left, right := normalized[i], normalized[j]
		if left.Tier == right.Tier {
			return left.CandidateID < right.CandidateID
		}
		return left.Tier < right.Tier
	})
	return normalized
}

func normalizeQuote(quote QuoteV1) QuoteV1 {
	normalized := quote
	normalized.QuotedAt = quote.QuotedAt.UTC()
	normalized.ValidUntil = quote.ValidUntil.UTC()
	normalized.Candidates = append([]QuoteCandidateV1(nil), quote.Candidates...)
	for index := range normalized.Candidates {
		normalized.Candidates[index].AvailabilityZones = canonicalSet(quote.Candidates[index].AvailabilityZones)
	}
	sort.Slice(normalized.Candidates, func(i, j int) bool {
		left, right := normalized.Candidates[i], normalized.Candidates[j]
		if left.Tier == right.Tier {
			return left.CandidateID < right.CandidateID
		}
		return left.Tier < right.Tier
	})
	normalized.IncludedItems = canonicalSet(quote.IncludedItems)
	normalized.UnincludedItems = canonicalSet(quote.UnincludedItems)
	return normalized
}

func normalizePlan(plan PlanV1) PlanV1 {
	normalized := plan
	normalized.Quote.ValidUntil = plan.Quote.ValidUntil.UTC()
	normalized.ResourceScope.AvailabilityZones = canonicalSet(plan.ResourceScope.AvailabilityZones)
	if plan.ResourceScope.Spot != nil {
		spot := *plan.ResourceScope.Spot
		normalized.ResourceScope.Spot = &spot
	}
	normalized.NetworkScope.Ingress = append([]IngressRuleV1(nil), plan.NetworkScope.Ingress...)
	sort.Slice(normalized.NetworkScope.Ingress, func(i, j int) bool {
		left, right := normalized.NetworkScope.Ingress[i], normalized.NetworkScope.Ingress[j]
		if left.Protocol == right.Protocol {
			if left.Port == right.Port {
				return left.Purpose < right.Purpose
			}
			return left.Port < right.Port
		}
		return left.Protocol < right.Protocol
	})
	normalized.SecretScope = append([]SecretReferenceV1(nil), plan.SecretScope...)
	sort.Slice(normalized.SecretScope, func(i, j int) bool {
		left, right := normalized.SecretScope[i], normalized.SecretScope[j]
		if left.SecretRef == right.SecretRef {
			if left.Purpose == right.Purpose {
				return left.Delivery < right.Delivery
			}
			return left.Purpose < right.Purpose
		}
		return left.SecretRef < right.SecretRef
	})
	normalized.IntegrationScope = append([]IntegrationScopeV1(nil), plan.IntegrationScope...)
	sort.Slice(normalized.IntegrationScope, func(i, j int) bool {
		left, right := normalized.IntegrationScope[i], normalized.IntegrationScope[j]
		if left.Kind == right.Kind {
			return left.Name < right.Name
		}
		return left.Kind < right.Kind
	})
	return normalized
}
