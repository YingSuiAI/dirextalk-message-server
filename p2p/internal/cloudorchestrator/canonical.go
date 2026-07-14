package cloudorchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/matrix-org/gomatrixserverlib"
)

// CanonicalRecipeJSON emits the current canonical-JSON representation used
// for recipe digests. It is not deterministic CBOR.
func (r RecipeV1) CanonicalRecipeJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return canonicalJSON(normalizeRecipe(r))
}

// Digest returns a content digest over the complete de-secretsed recipe.
func (r RecipeV1) Digest() (string, error) {
	canonical, err := r.CanonicalRecipeJSON()
	if err != nil {
		return "", err
	}
	return digestCanonicalJSON(canonical), nil
}

// CanonicalQuoteJSON emits the current canonical-JSON representation used for
// quote digests. It is not deterministic CBOR.
func (q QuoteV1) CanonicalQuoteJSON() ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return canonicalJSON(normalizeQuote(q))
}

// Digest returns a content digest over the full price estimate and validity.
func (q QuoteV1) Digest() (string, error) {
	canonical, err := q.CanonicalQuoteJSON()
	if err != nil {
		return "", err
	}
	return digestCanonicalJSON(canonical), nil
}

// CanonicalPlanJSON emits exactly the immutable approval surface. It omits
// mutable execution/status projections because they must not change the
// signed purchase decision.
func (p PlanV1) CanonicalPlanJSON() ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	normalized := normalizePlan(p)
	document := planHashDocumentV1{
		SchemaVersion:     normalized.SchemaVersion,
		HashAlgorithm:     HashAlgorithmCanonicalJSONSHA256,
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
	return canonicalJSON(document)
}

// Hash returns the canonical-JSON SHA-256 digest used for V1 approvals. The
// prefix makes it impossible to confuse this implementation with the planned
// deterministic-CBOR hash format in a later contract version.
func (p PlanV1) Hash() (string, error) {
	canonical, err := p.CanonicalPlanJSON()
	if err != nil {
		return "", err
	}
	return digestCanonicalJSON(canonical), nil
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

func canonicalJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical document: %w", err)
	}
	canonical, err := gomatrixserverlib.CanonicalJSON(encoded)
	if err != nil {
		return nil, fmt.Errorf("canonicalize JSON document: %w", err)
	}
	return canonical, nil
}

func digestCanonicalJSON(canonical []byte) string {
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
