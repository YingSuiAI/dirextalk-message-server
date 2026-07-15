package cloudorchestrator_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	cloudorchestrator "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
)

func TestServiceOperationApprovalBindsExactActionAndServiceRevision(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	target := operationTarget()
	approval, err := cloudorchestrator.NewServiceOperationApprovalV1(target, "approval-operation-0001", "challenge-operation-0001", "device-operation-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	public, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := approval.Sign(key, now)
	if err != nil || signed.Verify(public, now) != nil {
		t.Fatalf("sign/verify err=%v", err)
	}
	for _, mutate := range []func(*cloudorchestrator.ServiceOperationTargetV1){func(v *cloudorchestrator.ServiceOperationTargetV1) {
		v.Operation = cloudorchestrator.ServiceOperationStop
	}, func(v *cloudorchestrator.ServiceOperationTargetV1) { v.ServiceRevision++ }, func(v *cloudorchestrator.ServiceOperationTargetV1) {
		v.ActionID = "dirextalk_fixed_probe_service_stop_v1"
	}, func(v *cloudorchestrator.ServiceOperationTargetV1) { v.CheckpointSequence = []string{"other"} }} {
		changed := target
		mutate(&changed)
		if !errors.Is(signed.ValidateAgainst(changed, now), cloudorchestrator.ErrServiceOperationApprovalBinding) {
			t.Fatal("mutated operation target was accepted")
		}
	}
}

func TestServiceOperationApprovalGolden(t *testing.T) {
	now := time.Date(2026, time.July, 15, 12, 0, 0, 0, time.UTC)
	approval, err := cloudorchestrator.NewServiceOperationApprovalV1(operationTarget(), "approval-operation-0001", "challenge-operation-0001", "device-operation-0001", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := approval.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	const want = "9f75e71117618a524da731e8dd0d33ad7288ea2a5660c766431f311fb3c65755"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("operation payload digest=%s", got)
	}
}

func operationTarget() cloudorchestrator.ServiceOperationTargetV1 {
	return cloudorchestrator.ServiceOperationTargetV1{Operation: cloudorchestrator.ServiceOperationRestart, ServiceID: "service-operation-0001", ServiceRevision: 3, ExpectedServiceStatus: "active", DeploymentID: "deployment-operation-0001", DeploymentRevision: 8, CloudConnectionID: "connection-operation-0001", RecipeID: "recipe-operation-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InstalledManifestDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", ArtifactDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", ActionID: "dirextalk_fixed_probe_service_restart_v1", RootRequired: true, TimeoutSeconds: 120, CheckpointSequence: []string{"probe_service_restarted", "probe_health_verified"}}
}
