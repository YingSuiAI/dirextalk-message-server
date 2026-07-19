package contract

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"
)

const serviceRestoreApprovalGoldenSHA256 = "80ec3dedccb317676cecbd772607046909d2ee51adf02129243fdab080fa8ad2"

func TestServiceRestoreCommandBindsDeviceApprovedExactVolumeSwap(t *testing.T) {
	now := time.Date(2026, 7, 15, 16, 0, 0, 0, time.UTC)
	proof := ServiceRestoreApprovalProof{
		SchemaVersion:           approvalSchemaVersion,
		Intent:                  serviceRestoreApprovalIntent,
		ApprovalID:              "approval-restore-stack-0001",
		ChallengeID:             "challenge-restore-stack-0001",
		SignerKeyID:             "device-restore-stack-0001",
		RestoreID:               "restore-stack-0001",
		ServiceID:               "service-restore-stack-0001",
		ServiceRevision:         3,
		DeploymentID:            "deployment-restore-stack-0001",
		DeploymentRevision:      6,
		CloudConnectionID:       "connection-restore-stack-0001",
		BackupID:                "backup-restore-stack-0001",
		BackupRevision:          2,
		RecipeID:                "recipe-restore-stack-0001",
		RecipeDigest:            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID:              "i-0123456789abcdef0",
		Region:                  "ap-south-1",
		AvailabilityZone:        "ap-south-1a",
		RestoreMode:             "in_place",
		DowntimeRequired:        true,
		OriginalVolumeRetention: "manual",
		FailurePolicy:           "reattach_original",
		QuoteID:                 "restore-quote-stack-0001",
		Currency:                "USD",
		EstimatedHourlyMinor:    1,
		EstimatedThirtyDayMinor: 640,
		QuoteValidUntil:         now.Add(15 * time.Minute),
		Unincluded:              []string{"taxes"},
		VolumeSwaps: []ServiceRestoreVolumeSwap{{
			OriginalVolumeID:    "vol-0123456789abcdef0",
			SnapshotID:          "snap-0123456789abcdef0",
			DeviceName:          "/dev/xvda",
			VolumeType:          "gp3",
			SizeGiB:             80,
			IOPS:                3000,
			ThroughputMiB:       125,
			Encrypted:           true,
			DeleteOnTermination: true,
		}},
		IssuedAt:  now,
		ExpiresAt: now.Add(5 * time.Minute),
		Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x71}, 32))
	proof.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, payload))
	if err = proof.Verify(key.Public().(ed25519.PublicKey), now); err != nil {
		t.Fatal(err)
	}
	request := ServiceRestoreRequest{
		Schema:                  ServiceRestoreSchema,
		RestoreID:               proof.RestoreID,
		ServiceID:               proof.ServiceID,
		DeploymentID:            proof.DeploymentID,
		BackupID:                proof.BackupID,
		InstanceID:              proof.InstanceID,
		Region:                  proof.Region,
		AvailabilityZone:        proof.AvailabilityZone,
		RestoreMode:             proof.RestoreMode,
		DowntimeRequired:        true,
		OriginalVolumeRetention: proof.OriginalVolumeRetention,
		FailurePolicy:           proof.FailurePolicy,
		QuoteID:                 proof.QuoteID,
		QuoteValidUntil:         "2026-07-15T16:15:00.000Z",
		VolumeSwaps:             proof.VolumeSwaps,
	}
	raw, _ := json.Marshal(request)
	sum := sha256.Sum256(raw)
	proofJSON, _ := json.Marshal(proof)
	command := Command{
		Schema:             CommandSchema,
		ConnectionID:       proof.CloudConnectionID,
		CommandID:          "command-restore-stack-0001",
		NodeKeyID:          "node-restore-stack-0001",
		IssuedAt:           "2026-07-15T16:00:00.000Z",
		ExpiresAt:          "2026-07-15T16:04:00.000Z",
		ExpectedGeneration: 1,
		NodeCounter:        9,
		Action:             ActionServiceRestore,
		PayloadB64:         base64.StdEncoding.EncodeToString(raw),
		PayloadSHA256:      hex.EncodeToString(sum[:]),
		ApprovalProof:      proofJSON,
		SignatureB64:       base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	if err = command.ValidateServiceRestoreBinding(); err != nil {
		t.Fatal(err)
	}
	evidence := ServiceRestoreAWSEvidence{
		RestoreID:        proof.RestoreID,
		ServiceID:        proof.ServiceID,
		DeploymentID:     proof.DeploymentID,
		BackupID:         proof.BackupID,
		InstanceID:       proof.InstanceID,
		Region:           proof.Region,
		AvailabilityZone: proof.AvailabilityZone,
		Outcome:          "restored",
		InstanceState:    "running",
		Replacements: []ServiceRestoreReplacementVolume{{
			OriginalVolumeID:    proof.VolumeSwaps[0].OriginalVolumeID,
			ReplacementVolumeID: "vol-0fedcba9876543210",
			SnapshotID:          proof.VolumeSwaps[0].SnapshotID,
			DeviceName:          proof.VolumeSwaps[0].DeviceName,
			State:               "attached_current",
			Encrypted:           true,
			DeleteOnTermination: true,
		}},
	}
	result, err := MarshalCommittedServiceRestoreResult(command, evidence)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ServiceRestoreResult
	if json.Unmarshal(result, &decoded) != nil || ValidateServiceRestoreResult(command, decoded) != nil {
		t.Fatal("committed restore result invalid")
	}
	tampered := command
	var changed ServiceRestoreApprovalProof
	_ = json.Unmarshal(tampered.ApprovalProof, &changed)
	changed.VolumeSwaps[0].SnapshotID = "snap-0fedcba9876543210"
	tampered.ApprovalProof, _ = json.Marshal(changed)
	if tampered.ValidateServiceRestoreBinding() == nil {
		t.Fatal("snapshot drift must fail closed")
	}
}

func TestServiceRestoreApprovalMatchesOrchestratorGolden(t *testing.T) {
	now := time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)
	proof := ServiceRestoreApprovalProof{
		SchemaVersion: approvalSchemaVersion, Intent: serviceRestoreApprovalIntent,
		ApprovalID: "approval-restore-0001", ChallengeID: "challenge-restore-0001", SignerKeyID: "device-restore-0001",
		RestoreID: "restore-0001", ServiceID: "service-restore-0001", ServiceRevision: 4,
		DeploymentID: "deployment-restore-0001", DeploymentRevision: 8, CloudConnectionID: "connection-restore-0001",
		BackupID: "backup-restore-0001", BackupRevision: 2, RecipeID: "recipe-restore-0001", RecipeDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InstanceID: "i-0123456789abcdef0", Region: "us-east-1", AvailabilityZone: "us-east-1a", RestoreMode: "in_place", DowntimeRequired: true,
		OriginalVolumeRetention: "manual", FailurePolicy: "reattach_original", QuoteID: "quote-restore-0001", Currency: "USD", EstimatedHourlyMinor: 12, EstimatedThirtyDayMinor: 8640,
		QuoteValidUntil: now.Add(15 * time.Minute), Unincluded: []string{"data transfer", "tax"},
		VolumeSwaps: []ServiceRestoreVolumeSwap{{OriginalVolumeID: "vol-0123456789abcdef0", SnapshotID: "snap-0123456789abcdef0", DeviceName: "/dev/xvda", VolumeType: "gp3", SizeGiB: 80, IOPS: 3000, ThroughputMiB: 125, Encrypted: true, DeleteOnTermination: true}},
		IssuedAt:    now, ExpiresAt: now.Add(5 * time.Minute), Signature: base64.RawURLEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)),
	}
	payload, err := proof.SigningPayload()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	if got := hex.EncodeToString(digest[:]); got != serviceRestoreApprovalGoldenSHA256 {
		t.Fatalf("connection stack service restore approval golden digest=%s", got)
	}
}
