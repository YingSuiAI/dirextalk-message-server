package cloud

import (
	"context"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// RecipeDetail is the owner-facing, de-secreted projection of the current
// canonical private Recipe version. It deliberately excludes executable
// artifact content, runtime bindings, provider locations, ownership, and
// Cloud Connection data.
type RecipeDetail struct {
	RecipeID        string                   `json:"recipe_id"`
	Name            string                   `json:"name"`
	Version         string                   `json:"version"`
	Maturity        string                   `json:"maturity"`
	Revision        int64                    `json:"revision"`
	Digest          string                   `json:"digest"`
	Requirements    RecipeDetailRequirements `json:"requirements"`
	OfficialSources []RecipeOfficialSource   `json:"official_sources"`
	Health          RecipeDetailHealth       `json:"health"`
	Lifecycle       RecipeDetailLifecycle    `json:"lifecycle"`
	VolumeSlots     []RecipeDetailVolumeSlot `json:"volume_slots,omitempty"`
	DataSlots       []RecipeDetailDataSlot   `json:"data_slots,omitempty"`
	SecretSlots     []RecipeDetailSecretSlot `json:"secret_slots,omitempty"`
}

// These owner projection types intentionally copy the approved allow-list
// instead of embedding RecipeV1. Future private contract fields therefore do
// not become public response fields by accident.
type RecipeDetailRequirements struct {
	MinVCPU         uint16                      `json:"min_vcpu"`
	MinMemoryMiB    uint32                      `json:"min_memory_mib"`
	MinDiskGiB      uint32                      `json:"min_disk_gib"`
	MinGPUCount     uint16                      `json:"min_gpu_count,omitempty"`
	MinGPUMemoryMiB uint32                      `json:"min_gpu_memory_mib,omitempty"`
	Architecture    cloudcontracts.Architecture `json:"architecture"`
}

type RecipeDetailHealth struct {
	Liveness  RecipeDetailProbe `json:"liveness"`
	Readiness RecipeDetailProbe `json:"readiness"`
	Semantic  RecipeDetailProbe `json:"semantic"`
}

type RecipeDetailProbe struct {
	Kind   cloudcontracts.ProbeKind `json:"kind"`
	Target string                   `json:"target"`
}

type RecipeDetailLifecycle struct {
	Start    string `json:"start"`
	Stop     string `json:"stop"`
	Restart  string `json:"restart"`
	Upgrade  string `json:"upgrade"`
	Rollback string `json:"rollback"`
	Backup   string `json:"backup"`
	Restore  string `json:"restore"`
	Destroy  string `json:"destroy"`
}

type RecipeDetailVolumeSlot struct {
	SlotID   string `json:"slot_id"`
	Purpose  string `json:"purpose"`
	ReadOnly bool   `json:"read_only"`
}

type RecipeDetailDataSlot struct {
	SlotID   string `json:"slot_id"`
	Purpose  string `json:"purpose"`
	ReadOnly bool   `json:"read_only"`
}

type RecipeDetailSecretSlot struct {
	SlotID   string                        `json:"slot_id"`
	Purpose  string                        `json:"purpose"`
	Delivery cloudcontracts.SecretDelivery `json:"delivery"`
}

type RecipeOfficialSource struct {
	Version        string    `json:"version"`
	Commit         string    `json:"commit"`
	ArtifactDigest string    `json:"artifact_digest"`
	License        string    `json:"license"`
	RetrievedAt    time.Time `json:"retrieved_at"`
}

// RecipeDetailStore is separate from the Agent recommendation reader. The
// latter intentionally exposes a smaller DTO and must not inherit this richer
// owner-only projection.
type RecipeDetailStore interface {
	GetCloudRecipeDetail(context.Context, string, string) (RecipeDetail, bool, error)
}
