//go:build linux

package cloudworker

import (
	"os"
	"syscall"
)

func trustedRecipeCacheOwner(info os.FileInfo) bool {
	state, ok := info.Sys().(*syscall.Stat_t)
	return ok && state.Uid == uint32(os.Geteuid())
}

func trustedRecipeArtifactMode(info os.FileInfo, expected os.FileMode) bool {
	return info.Mode().Perm() == expected.Perm()
}
