//go:build !linux

package cloudworker

import "os"

func trustedRecipeCacheOwner(os.FileInfo) bool { return true }

func trustedRecipeArtifactMode(info os.FileInfo, expected os.FileMode) bool {
	// The production Worker is Linux-only. Other platforms still validate the
	// exact tar header modes but cannot faithfully round-trip Unix execute bits.
	return info.Mode().IsRegular() && expected.Perm() != 0
}
