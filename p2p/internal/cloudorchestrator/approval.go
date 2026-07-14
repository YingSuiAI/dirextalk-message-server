package cloudorchestrator

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"time"
)

// NewApprovalV1 derives an unsigned device-approval challenge from a ready
// plan. The resulting value contains no credential material and no provider
// capability; only a client-held Ed25519 key may sign it.
func NewApprovalV1(plan PlanV1, approvalID, challengeID, signerKeyID string, expiresAt time.Time) (ApprovalV1, error) {
	if err := plan.Validate(); err != nil {
		return ApprovalV1{}, fmt.Errorf("invalid plan: %w", err)
	}
	if plan.Status != PlanReadyForConfirmation {
		return ApprovalV1{}, errors.New("approval challenge requires a ready_for_confirmation plan")
	}
	planHash, err := plan.Hash()
	if err != nil {
		return ApprovalV1{}, fmt.Errorf("hash plan: %w", err)
	}
	normalized := normalizePlan(plan)
	approval := ApprovalV1{
		SchemaVersion:     SchemaVersionV1,
		ApprovalID:        approvalID,
		ChallengeID:       challengeID,
		SignerKeyID:       signerKeyID,
		PlanID:            normalized.PlanID,
		PlanHash:          planHash,
		PlanRevision:      normalized.Revision,
		QuoteID:           normalized.Quote.QuoteID,
		QuoteDigest:       normalized.Quote.Digest,
		QuoteValidUntil:   normalized.Quote.ValidUntil,
		CloudConnectionID: normalized.CloudConnectionID,
		RecipeDigest:      normalized.Recipe.Digest,
		ResourceScope:     normalized.ResourceScope,
		NetworkScope:      normalized.NetworkScope,
		SecretScope:       normalized.SecretScope,
		IntegrationScope:  normalized.IntegrationScope,
		ExpiresAt:         expiresAt.UTC(),
	}
	if err := approval.Validate(); err != nil {
		return ApprovalV1{}, fmt.Errorf("invalid approval challenge: %w", err)
	}
	return approval, nil
}

// SigningPayload returns deterministic-CBOR bytes that exclude
// ApprovalV1.Signature and include every approval-sensitive field. It is safe
// to transfer to the client for inspection and signing because it never
// includes secret values.
func (a ApprovalV1) SigningPayload() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	normalized := normalizeApproval(a)
	payload := approvalSigningPayloadV1{
		SchemaVersion:     normalized.SchemaVersion,
		PayloadVersion:    "approval-signing-payload/v1",
		HashAlgorithm:     HashAlgorithmDeterministicCBORSHA256,
		ApprovalID:        normalized.ApprovalID,
		ChallengeID:       normalized.ChallengeID,
		SignerKeyID:       normalized.SignerKeyID,
		PlanID:            normalized.PlanID,
		PlanHash:          normalized.PlanHash,
		PlanRevision:      normalized.PlanRevision,
		QuoteID:           normalized.QuoteID,
		QuoteDigest:       normalized.QuoteDigest,
		QuoteValidUntil:   normalized.QuoteValidUntil,
		CloudConnectionID: normalized.CloudConnectionID,
		RecipeDigest:      normalized.RecipeDigest,
		ResourceScope:     normalized.ResourceScope,
		NetworkScope:      normalized.NetworkScope,
		SecretScope:       normalized.SecretScope,
		IntegrationScope:  normalized.IntegrationScope,
		ExpiresAt:         normalized.ExpiresAt,
	}
	return canonicalCBOR(payload)
}

// Sign returns a copy with a base64url Ed25519 signature. The caller retains
// the private key; this package never stores it.
func (a ApprovalV1) Sign(privateKey ed25519.PrivateKey, now time.Time) (ApprovalV1, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return ApprovalV1{}, errors.New("approval signing key is not an Ed25519 private key")
	}
	if err := a.validateNotExpired(now); err != nil {
		return ApprovalV1{}, err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return ApprovalV1{}, err
	}
	a.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	return a, nil
}

// Verify validates a signed, non-expired approval with the caller-provided
// public key. It intentionally does not imply that the approval matches a
// current plan; call ValidateAgainstPlan immediately before mutation.
func (a ApprovalV1) Verify(publicKey ed25519.PublicKey, now time.Time) error {
	if len(publicKey) != ed25519.PublicKeySize {
		return errors.New("approval verification key is not an Ed25519 public key")
	}
	if err := a.ValidateAt(now); err != nil {
		return err
	}
	payload, err := a.SigningPayload()
	if err != nil {
		return err
	}
	signature, err := base64.RawURLEncoding.DecodeString(a.Signature)
	if err != nil || !ed25519.Verify(publicKey, payload, signature) {
		return errors.New("approval signature is invalid")
	}
	return nil
}

// ValidateAgainstPlan ensures a challenge/signed approval has not been
// replayed for a different plan revision, quote, connection, recipe, or
// scope. Signature verification is deliberately separate because public-key
// ownership belongs to the caller's device registry.
func (a ApprovalV1) ValidateAgainstPlan(plan PlanV1, now time.Time) error {
	if err := a.validateNotExpired(now); err != nil {
		return err
	}
	if err := plan.Validate(); err != nil {
		return fmt.Errorf("invalid plan: %w", err)
	}
	planHash, err := plan.Hash()
	if err != nil {
		return err
	}
	normalizedPlan := normalizePlan(plan)
	normalizedApproval := normalizeApproval(a)
	if normalizedApproval.PlanID != normalizedPlan.PlanID ||
		normalizedApproval.PlanHash != planHash ||
		normalizedApproval.PlanRevision != normalizedPlan.Revision ||
		normalizedApproval.QuoteID != normalizedPlan.Quote.QuoteID ||
		normalizedApproval.QuoteDigest != normalizedPlan.Quote.Digest ||
		!normalizedApproval.QuoteValidUntil.Equal(normalizedPlan.Quote.ValidUntil) ||
		normalizedApproval.CloudConnectionID != normalizedPlan.CloudConnectionID ||
		normalizedApproval.RecipeDigest != normalizedPlan.Recipe.Digest {
		return errors.New("approval does not bind the current plan identity")
	}
	if !reflect.DeepEqual(normalizedApproval.ResourceScope, normalizedPlan.ResourceScope) ||
		!reflect.DeepEqual(normalizedApproval.NetworkScope, normalizedPlan.NetworkScope) ||
		!reflect.DeepEqual(normalizedApproval.SecretScope, normalizedPlan.SecretScope) ||
		!reflect.DeepEqual(normalizedApproval.IntegrationScope, normalizedPlan.IntegrationScope) {
		return errors.New("approval scope does not match the current plan")
	}
	return nil
}

func (a ApprovalV1) validateNotExpired(now time.Time) error {
	if err := a.Validate(); err != nil {
		return err
	}
	if !a.ExpiresAt.After(now.UTC()) {
		return errors.New("approval has expired")
	}
	return nil
}

type approvalSigningPayloadV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	PayloadVersion    string               `json:"payload_version"`
	HashAlgorithm     string               `json:"hash_algorithm"`
	ApprovalID        string               `json:"approval_id"`
	ChallengeID       string               `json:"challenge_id"`
	SignerKeyID       string               `json:"signer_key_id"`
	PlanID            string               `json:"plan_id"`
	PlanHash          string               `json:"plan_hash"`
	PlanRevision      uint64               `json:"plan_revision"`
	QuoteID           string               `json:"quote_id"`
	QuoteDigest       string               `json:"quote_digest"`
	QuoteValidUntil   time.Time            `json:"quote_valid_until"`
	CloudConnectionID string               `json:"cloud_connection_id"`
	RecipeDigest      string               `json:"recipe_digest"`
	ResourceScope     ResourceScopeV1      `json:"resource_scope"`
	NetworkScope      NetworkScopeV1       `json:"network_scope"`
	SecretScope       []SecretReferenceV1  `json:"secret_scope"`
	IntegrationScope  []IntegrationScopeV1 `json:"integration_scope"`
	ExpiresAt         time.Time            `json:"expires_at"`
}

func normalizeApproval(approval ApprovalV1) ApprovalV1 {
	normalized := approval
	normalized.QuoteValidUntil = approval.QuoteValidUntil.UTC()
	normalized.ExpiresAt = approval.ExpiresAt.UTC()
	scopes := normalizePlan(PlanV1{
		ResourceScope:    approval.ResourceScope,
		NetworkScope:     approval.NetworkScope,
		SecretScope:      approval.SecretScope,
		IntegrationScope: approval.IntegrationScope,
	})
	normalized.ResourceScope = scopes.ResourceScope
	normalized.NetworkScope = scopes.NetworkScope
	normalized.SecretScope = scopes.SecretScope
	normalized.IntegrationScope = scopes.IntegrationScope
	return normalized
}
