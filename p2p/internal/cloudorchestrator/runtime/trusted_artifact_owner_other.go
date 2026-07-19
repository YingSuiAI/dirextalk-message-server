//go:build !linux

package runtime

import "os"

// Production Cloud Orchestrator hosts are Linux. Enabling trusted artifact
// registration on another platform fails closed because root ownership cannot
// be established through the supported boundary.
func rootOwnedFile(os.FileInfo) bool { return false }
