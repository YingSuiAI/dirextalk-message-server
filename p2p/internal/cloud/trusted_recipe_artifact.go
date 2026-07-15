package cloud

import (
	"context"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

// TrustedRecipeArtifactStore is compiler-only ingress. It is deliberately not
// embedded in Store and has no ProductCore, Agent, or MCP adapter.
type TrustedRecipeArtifactStore interface {
	RegisterTrustedCloudRecipeArtifact(context.Context, RegisterTrustedRecipeArtifactRequest) (RegisterTrustedRecipeArtifactResult, error)
}

type RegisterTrustedRecipeArtifactRequest struct {
	Artifact     cloudcontracts.CompiledRecipeArtifactV1
	RegisteredAt int64
}

type TrustedRecipeArtifact struct {
	ArtifactDigest               string `json:"artifact_digest"`
	DescriptorDigest             string `json:"descriptor_digest"`
	RecipeID                     string `json:"recipe_id"`
	RecipeDigest                 string `json:"recipe_digest"`
	RecipeRevision               uint64 `json:"recipe_revision"`
	WorkerResourceManifestDigest string `json:"worker_resource_manifest_digest"`
	Status                       string `json:"status"`
	Revision                     int64  `json:"revision"`
	CreatedAt                    int64  `json:"created_at"`
}

type RegisterTrustedRecipeArtifactResult struct {
	Artifact TrustedRecipeArtifact
	Created  bool
}
