package cloudorchestrator_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/fixedprobe"
)

func TestFixedReadinessEvidenceDigestMatchesWorkerBody(t *testing.T) {
	sum := sha256.Sum256([]byte(fixedprobe.ReadinessBody))
	got := "sha256:" + hex.EncodeToString(sum[:])
	if got != cloudorchestrator.FixedReadinessEvidenceDigestV1 {
		t.Fatalf("fixed readiness evidence digest=%q, contract=%q", got, cloudorchestrator.FixedReadinessEvidenceDigestV1)
	}
}
