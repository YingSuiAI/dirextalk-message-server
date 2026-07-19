//go:build linux

package recipeexec

import "golang.org/x/sys/unix"

// VerifyTmpfsRoot is the production verifier for the secret staging root.
func VerifyTmpfsRoot(path string) error {
	var state unix.Statfs_t
	if err := unix.Statfs(path, &state); err != nil || uint64(state.Type) != uint64(unix.TMPFS_MAGIC) {
		return ErrSecretStage
	}
	return nil
}
