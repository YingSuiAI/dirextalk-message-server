package recipeexec_test

import (
	"context"
	"errors"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestFixedBundleResolverAcceptsOnlyAnExplicitDigestCatalog(t *testing.T) {
	digest := sha256('a')
	resolver, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{{ArtifactDigest: digest, ActionIDs: []string{"install-service"}}})
	if err != nil {
		t.Fatalf("NewFixedBundleResolver() error = %v", err)
	}
	bundle, err := resolver.Resolve(context.Background(), digest)
	if err != nil || bundle.ArtifactDigest != digest || len(bundle.ActionIDs) != 1 || bundle.ActionIDs[0] != "install-service" {
		t.Fatalf("Resolve() = (%#v, %v)", bundle, err)
	}
	bundle.ActionIDs[0] = "mutated"
	again, err := resolver.Resolve(context.Background(), digest)
	if err != nil || again.ActionIDs[0] != "install-service" {
		t.Fatalf("catalog was mutable: (%#v, %v)", again, err)
	}

	for _, catalog := range [][]recipeexec.Bundle{
		nil,
		{{ArtifactDigest: digest, ActionIDs: nil}},
		{{ArtifactDigest: "https://example.invalid/bundle", ActionIDs: []string{"install-service"}}},
		{{ArtifactDigest: digest, ActionIDs: []string{"/tmp/install"}}},
		{{ArtifactDigest: digest, ActionIDs: []string{"curl https://example.invalid"}}},
		{{ArtifactDigest: digest, ActionIDs: []string{"install-service", "install-service"}}},
		{{ArtifactDigest: digest, ActionIDs: []string{"install-service"}}, {ArtifactDigest: digest, ActionIDs: []string{"restart-service"}}},
	} {
		if _, err := recipeexec.NewFixedBundleResolver(catalog); !errors.Is(err, recipeexec.ErrBundleCatalogInvalid) {
			t.Fatalf("catalog %#v error = %v", catalog, err)
		}
	}
	if _, err := resolver.Resolve(context.Background(), sha256('b')); !errors.Is(err, recipeexec.ErrBundleCatalogInvalid) {
		t.Fatalf("unknown digest error = %v", err)
	}
}

func TestFixedBundleResolverAcceptsOnlyTrustedSecretDestinations(t *testing.T) {
	digest := sha256('a')
	valid := recipeexec.Bundle{ArtifactDigest: digest, ActionIDs: []string{"install-service"}, SecretTargets: []recipeexec.SecretTarget{{SlotID: "model-token", FileName: "model-token"}}}
	if _, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{valid}); err != nil {
		t.Fatalf("trusted secret destination rejected: %v", err)
	}
	for _, target := range []recipeexec.SecretTarget{
		{SlotID: "model-token", FileName: "../escape"},
		{SlotID: "model-token", FileName: "token", EnvironmentKey: "MODEL_TOKEN"},
		{SlotID: "model-token", EnvironmentKey: "AgentChosen"},
		{SlotID: "model-token", FileName: "."},
	} {
		candidate := valid
		candidate.SecretTargets = []recipeexec.SecretTarget{target}
		if _, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{candidate}); !errors.Is(err, recipeexec.ErrBundleCatalogInvalid) {
			t.Fatalf("target %#v error = %v", target, err)
		}
	}
}
