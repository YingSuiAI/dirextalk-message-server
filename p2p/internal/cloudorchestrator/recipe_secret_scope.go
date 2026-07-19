package cloudorchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// SecretReferenceForRecipeSlot derives the only Plan reference accepted for a
// Recipe secret requirement. Common identifiers retain the readable
// secret_ref:<plan>/<slot> form; otherwise a stable hash keeps the ref inside
// the closed validator alphabet without weakening Recipe identifier rules.
func SecretReferenceForRecipeSlot(planID string, requirement RecipeSecretSlotRequirementV1) (SecretReferenceV1, error) {
	if validateIdentifier("plan_id", planID) != nil || validateRecipeSlotRequirements(nil, nil, []RecipeSecretSlotRequirementV1{requirement}) != nil {
		return SecretReferenceV1{}, errors.New("recipe secret scope input is invalid")
	}
	ref := "secret_ref:" + planID + "/" + requirement.SlotID
	if !secretRefPattern.MatchString(ref) {
		sum := sha256.Sum256([]byte(planID + "\x00" + requirement.SlotID))
		ref = "secret_ref:plan/" + hex.EncodeToString(sum[:])
	}
	result := SecretReferenceV1{SecretRef: ref, Purpose: requirement.Purpose, Delivery: requirement.Delivery}
	if validateSecretScope([]SecretReferenceV1{result}) != nil {
		return SecretReferenceV1{}, errors.New("recipe secret scope is invalid")
	}
	return result, nil
}

func SecretScopeForRecipe(planID string, requirements []RecipeSecretSlotRequirementV1) ([]SecretReferenceV1, error) {
	scope := make([]SecretReferenceV1, 0, len(requirements))
	for _, requirement := range requirements {
		reference, err := SecretReferenceForRecipeSlot(planID, requirement)
		if err != nil {
			return nil, err
		}
		scope = append(scope, reference)
	}
	if err := validateSecretScope(scope); err != nil {
		return nil, err
	}
	return scope, nil
}
