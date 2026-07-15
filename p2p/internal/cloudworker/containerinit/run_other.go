//go:build !linux

package containerinit

// Run fails closed because the measured container-side identity and exec path
// is supported only by the Linux cloud-worker artifact.
func Run([]string) error { return ErrContainerInit }
