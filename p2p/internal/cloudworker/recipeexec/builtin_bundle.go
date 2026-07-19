package recipeexec

import (
	"crypto/sha256"
	"encoding/hex"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

const (
	// FixedProbeActionID is the only action exposed by the first compiled
	// Worker bundle. It is deliberately a product-owned opaque identifier, not
	// a command, executable path, package name, image reference, or URL.
	FixedProbeActionID  = cloudorchestrator.FixedProbeInstallActionID
	FixedProbeStartID   = cloudorchestrator.FixedProbeStartActionID
	FixedProbeStopID    = cloudorchestrator.FixedProbeStopActionID
	FixedProbeRestartID = cloudorchestrator.FixedProbeRestartActionID

	fixedProbeBundleDescriptor        = `{"schema":"dirextalk.fixed-probe-bundle/v1","action_id":"dirextalk_fixed_probe_service_install_v1","version":"1"}`
	fixedProbeManagedBundleDescriptor = `{"schema":"dirextalk.fixed-probe-managed-bundle/v1","action_ids":["dirextalk_fixed_probe_service_install_v1","dirextalk_fixed_probe_service_restart_v1","dirextalk_fixed_probe_service_start_v1","dirextalk_fixed_probe_service_stop_v1"],"version":"1"}`
)

// FixedProbeBundle returns the immutable descriptor compiled into the trusted
// Worker binary. The independently registered Worker image digest authenticates
// the code that implements it; this digest binds the exact reviewed action
// descriptor carried by a private Recipe manifest.
func FixedProbeBundle() Bundle {
	sum := sha256.Sum256([]byte(fixedProbeBundleDescriptor))
	return Bundle{ArtifactDigest: "sha256:" + hex.EncodeToString(sum[:]), ActionIDs: []string{FixedProbeActionID}}
}

func FixedProbeManagedBundle() Bundle {
	sum := sha256.Sum256([]byte(fixedProbeManagedBundleDescriptor))
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if digest != cloudorchestrator.FixedProbeManagedArtifactDigest {
		panic("fixed probe managed artifact contract drift")
	}
	return Bundle{ArtifactDigest: digest, ActionIDs: []string{FixedProbeActionID, FixedProbeRestartID, FixedProbeStartID, FixedProbeStopID}}
}

func FixedProbeManagedBundleDescriptor() []byte {
	return append([]byte(nil), fixedProbeManagedBundleDescriptor...)
}

// FixedProbeBundleDescriptor returns a defensive copy for release tooling and
// golden-vector tests. Runtime tasks never receive or select this content.
func FixedProbeBundleDescriptor() []byte {
	return append([]byte(nil), fixedProbeBundleDescriptor...)
}
