package recipeexec

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
)

var (
	environmentKeyPattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)
)

// ErrBundleCatalogInvalid means the fixed AMI did not explicitly register a
// closed digest-to-action catalog. A missing or malformed catalog never falls
// back to a path, URL, shell command, or dynamic artifact download.
var ErrBundleCatalogInvalid = errors.New("fixed recipe bundle catalog is invalid")

// FixedBundleResolver is an immutable, in-memory catalog compiled or injected
// by the trusted AMI integration. It deliberately has no filesystem or network
// fields, so an untrusted Recipe task cannot choose a local path or URL.
type FixedBundleResolver struct {
	bundles map[string]Bundle
}

func NewFixedBundleResolver(catalog []Bundle) (*FixedBundleResolver, error) {
	if len(catalog) == 0 || len(catalog) > 128 {
		return nil, ErrBundleCatalogInvalid
	}
	bundles := make(map[string]Bundle, len(catalog))
	for _, candidate := range catalog {
		if !validTaskDigest(candidate.ArtifactDigest) || len(candidate.ActionIDs) == 0 || len(candidate.ActionIDs) > 64 {
			return nil, ErrBundleCatalogInvalid
		}
		if _, duplicate := bundles[candidate.ArtifactDigest]; duplicate {
			return nil, ErrBundleCatalogInvalid
		}
		actions := make([]string, len(candidate.ActionIDs))
		seen := make(map[string]struct{}, len(candidate.ActionIDs))
		for index, actionID := range candidate.ActionIDs {
			if !validBindingIdentifier(actionID) {
				return nil, ErrBundleCatalogInvalid
			}
			if _, duplicate := seen[actionID]; duplicate {
				return nil, ErrBundleCatalogInvalid
			}
			seen[actionID] = struct{}{}
			actions[index] = actionID
		}
		secretTargets := make([]SecretTarget, len(candidate.SecretTargets))
		seenSlots := make(map[string]struct{}, len(candidate.SecretTargets))
		for index, target := range candidate.SecretTargets {
			if !validBindingIdentifier(target.SlotID) || !validSecretDestination(target) {
				return nil, ErrBundleCatalogInvalid
			}
			if _, duplicate := seenSlots[target.SlotID]; duplicate {
				return nil, ErrBundleCatalogInvalid
			}
			seenSlots[target.SlotID] = struct{}{}
			secretTargets[index] = target
		}
		bundles[candidate.ArtifactDigest] = Bundle{ArtifactDigest: candidate.ArtifactDigest, ActionIDs: actions, SecretTargets: secretTargets}
	}
	return &FixedBundleResolver{bundles: bundles}, nil
}

func validSecretDestination(target SecretTarget) bool {
	fileTarget := target.FileName != "" && target.FileName != "." && target.FileName != ".." && target.FileName != "environment" && target.EnvironmentKey == "" && filepath.Base(target.FileName) == target.FileName
	envTarget := target.FileName == "" && environmentKeyPattern.MatchString(target.EnvironmentKey)
	return fileTarget || envTarget
}

func (resolver *FixedBundleResolver) Resolve(ctx context.Context, artifactDigest string) (Bundle, error) {
	if ctx == nil || ctx.Err() != nil || resolver == nil || !validTaskDigest(artifactDigest) {
		return Bundle{}, ErrBundleCatalogInvalid
	}
	bundle, ok := resolver.bundles[artifactDigest]
	if !ok {
		return Bundle{}, ErrBundleCatalogInvalid
	}
	return cloneBundle(bundle), nil
}
