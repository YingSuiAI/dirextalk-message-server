package cloudorchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
)

// VolumeReferenceForRecipeSlot derives the only logical volume reference
// accepted for a Recipe volume requirement. The reference identifies a
// persistent directory inside the deployment's dedicated VM root volume; it
// is never an EC2 volume ID, client path, or authorization for an AWS mutation.
func VolumeReferenceForRecipeSlot(planID string, requirement RecipeVolumeSlotRequirementV1) (VolumeSlotV1, error) {
	if validateIdentifier("plan_id", planID) != nil || validateRecipeSlotRequirements([]RecipeVolumeSlotRequirementV1{requirement}, nil, nil) != nil {
		return VolumeSlotV1{}, errors.New("recipe volume scope input is invalid")
	}
	ref := "volume_ref:" + planID + "/" + requirement.SlotID
	if !volumeRefPattern.MatchString(ref) {
		sum := sha256.Sum256([]byte("dirextalk-recipe-volume-ref/v1\x00" + planID + "\x00" + requirement.SlotID))
		ref = "volume_ref:plan/" + hex.EncodeToString(sum[:])
	}
	result := VolumeSlotV1{SlotID: requirement.SlotID, VolumeRef: ref, ReadOnly: requirement.ReadOnly}
	if validateVolumeSlots([]VolumeSlotV1{result}) != nil {
		return VolumeSlotV1{}, errors.New("recipe volume scope is invalid")
	}
	return result, nil
}

// DataReferenceForRecipeSlot derives the only logical data reference accepted
// for a Recipe data requirement. V1 realizes the reference as a persistent
// directory inside the deployment's dedicated VM root volume.
func DataReferenceForRecipeSlot(planID string, requirement RecipeDataSlotRequirementV1) (DataSlotV1, error) {
	if validateIdentifier("plan_id", planID) != nil || validateRecipeSlotRequirements(nil, []RecipeDataSlotRequirementV1{requirement}, nil) != nil {
		return DataSlotV1{}, errors.New("recipe data scope input is invalid")
	}
	ref := "data_ref:" + planID + "/" + requirement.SlotID
	if !dataRefPattern.MatchString(ref) {
		sum := sha256.Sum256([]byte("dirextalk-recipe-data-ref/v1\x00" + planID + "\x00" + requirement.SlotID))
		ref = "data_ref:plan/" + hex.EncodeToString(sum[:])
	}
	result := DataSlotV1{SlotID: requirement.SlotID, DataRef: ref, ReadOnly: requirement.ReadOnly}
	if validateDataSlots([]DataSlotV1{result}) != nil {
		return DataSlotV1{}, errors.New("recipe data scope is invalid")
	}
	return result, nil
}

func VolumeSlotsForRecipe(planID string, requirements []RecipeVolumeSlotRequirementV1) ([]VolumeSlotV1, error) {
	slots := make([]VolumeSlotV1, 0, len(requirements))
	for _, requirement := range requirements {
		slot, err := VolumeReferenceForRecipeSlot(planID, requirement)
		if err != nil {
			return nil, err
		}
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].SlotID < slots[j].SlotID })
	if err := validateVolumeSlots(slots); err != nil {
		return nil, err
	}
	return slots, nil
}

func DataSlotsForRecipe(planID string, requirements []RecipeDataSlotRequirementV1) ([]DataSlotV1, error) {
	slots := make([]DataSlotV1, 0, len(requirements))
	for _, requirement := range requirements {
		slot, err := DataReferenceForRecipeSlot(planID, requirement)
		if err != nil {
			return nil, err
		}
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i].SlotID < slots[j].SlotID })
	if err := validateDataSlots(slots); err != nil {
		return nil, err
	}
	return slots, nil
}
