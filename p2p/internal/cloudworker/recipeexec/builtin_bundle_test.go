package recipeexec

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFixedProbeBundleIsImmutableGolden(t *testing.T) {
	bundle := FixedProbeBundle()
	wantActions := []string{FixedProbeActionID}
	if len(bundle.ActionIDs) != len(wantActions) {
		t.Fatalf("actions=%v", bundle.ActionIDs)
	}
	for index := range wantActions {
		if bundle.ActionIDs[index] != wantActions[index] {
			t.Fatalf("actions=%v", bundle.ActionIDs)
		}
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

func TestFixedProbeManagedBundleIsImmutableGolden(t *testing.T) {
	bundle := FixedProbeManagedBundle()
	want := []string{FixedProbeActionID, FixedProbeRestartID, FixedProbeStartID, FixedProbeStopID}
	if len(bundle.ActionIDs) != len(want) {
		t.Fatalf("actions=%v", bundle.ActionIDs)
	}
	for i := range want {
		if bundle.ActionIDs[i] != want[i] {
			t.Fatalf("actions=%v", bundle.ActionIDs)
		}
	}
	const expected = "sha256:ad88e50776ac1b308a0e385dd5f9cbf847ae431d50b20b82b04ec74c75995d93"
	if bundle.ArtifactDigest != expected {
		t.Fatalf("artifact digest=%q", bundle.ArtifactDigest)
	}
	sum := sha256.Sum256(FixedProbeManagedBundleDescriptor())
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != expected {
		t.Fatalf("descriptor digest=%q", got)
	}
}
