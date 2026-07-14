package cloudorchestrator

import (
	"strings"
	"testing"
	"time"
)

func TestRecipeV1RejectsSignedOrCredentialBearingSourceURL(t *testing.T) {
	recipe := RecipeV1{
		SchemaVersion: SchemaVersionV1,
		RecipeID:      "recipe-1",
		Name:          "Example recipe",
		Maturity:      RecipeExperimental,
		Sources: []RecipeSourceV1{{
			URL:            "https://artifacts.example.invalid/release?X-Amz-Credential=temporary",
			Version:        "v1",
			Commit:         "0123456789abcdef0123456789abcdef01234567",
			ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			License:        "Apache-2.0",
			RetrievedAt:    time.Date(2026, time.July, 14, 10, 0, 0, 0, time.UTC),
		}},
	}
	if err := recipe.Validate(); err == nil || !strings.Contains(err.Error(), "credential query") {
		t.Fatalf("Validate() error = %v, want credential-bearing source rejection", err)
	}
}
