//go:build !linux

package recipeexec

// VerifyTmpfsRoot fails closed on unsupported Worker platforms.
func VerifyTmpfsRoot(string) error { return ErrSecretStage }
