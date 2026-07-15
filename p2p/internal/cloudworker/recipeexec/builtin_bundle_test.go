package recipeexec

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFixedProbeBundleIsImmutableGolden(t *testing.T) {
	bundle := FixedProbeBundle()
	if len(bundle.ActionIDs) != 1 || bundle.ActionIDs[0] != FixedProbeActionID {
		t.Fatalf("actions=%v", bundle.ActionIDs)
	}
	const expected = "sha256:fe5dc7f671d39855466b4a49d74b9ec1ad839316d8258244c7c20cb13790eb92"
	if bundle.ArtifactDigest != expected {
		t.Fatalf("artifact digest=%q", bundle.ArtifactDigest)
	}
	descriptor := FixedProbeBundleDescriptor()
	descriptor[0] ^= 0xff
	if FixedProbeBundle().ArtifactDigest != expected {
		t.Fatal("caller mutation changed the compiled descriptor")
	}
	sum := sha256.Sum256(FixedProbeBundleDescriptor())
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != expected {
		t.Fatalf("descriptor digest=%q", got)
	}
}
