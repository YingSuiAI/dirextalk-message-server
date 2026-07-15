package contract

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestArtifactPutContractRejectsUnknownFieldsAndUnboundedArchive(t *testing.T) {
	binding := ArtifactBinding{DeploymentID: "deployment-0001", TaskID: "task-artifact-0001", ExecutionID: "execution-0001", RecipeDigest: "sha256:" + strings.Repeat("1", 64), ArtifactDigest: "sha256:" + strings.Repeat("2", 64), ManifestDigest: "sha256:" + strings.Repeat("3", 64), ArchiveSHA256: strings.Repeat("4", 64), SizeBytes: MaxArtifactSizeBytes, MediaType: ArtifactMediaType}
	payload, _ := json.Marshal(ArtifactPutPrepareRequest{Schema: ArtifactPutPrepareSchema, ArtifactBinding: binding})
	command := fixtureCommand(t, ActionArtifactPut, "command-artifact-0001", "node-key-1", 1, 1, "2026-07-15T01:00:00.000Z", "2026-07-15T01:04:00.000Z", ArtifactPutPrepareRequest{Schema: ArtifactPutPrepareSchema, ArtifactBinding: binding})
	if _, err := ParseArtifactPutPrepare(command); err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{append(payload[:len(payload)-1], []byte(`,"url":"https://evil.invalid"}`)...), []byte(strings.Replace(string(payload), `"size_bytes":268435456`, `"size_bytes":268435457`, 1)), []byte(strings.Replace(string(payload), ArtifactMediaType, "application/octet-stream", 1))} {
		command.PayloadB64 = base64.StdEncoding.EncodeToString(raw)
		sum := sha256.Sum256(raw)
		command.PayloadSHA256 = hex.EncodeToString(sum[:])
		if _, err := ParseArtifactPutPrepare(command); err == nil {
			t.Fatalf("accepted invalid payload %s", raw)
		}
	}
}

func TestRecipeTaskClaimArtifactAccessIsTransientAndGateCompatible(t *testing.T) {
	manifest := recipeManifestFixture(t)
	digest, _ := manifest.Digest()
	task := RecipeTaskV1{Schema: RecipeTaskV1Schema, TaskID: "task-artifact-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: RecipeTaskKindExecution, RecipeExecutionManifestDigest: digest, InputDigest: "sha256:" + strings.Repeat("7", 64), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	access := ArtifactAccess{Method: "GET", URL: "https://s3.example.invalid/get?versionId=version-0001&signature=redacted", ExpiresAt: "2026-07-15T01:10:00.000Z", VersionID: "version-0001", MediaType: ArtifactMediaType, SizeBytes: 1024, ArchiveSHA256: strings.Repeat("4", 64)}
	raw, err := MarshalRecipeTaskClaimResponseWithArtifact(1, &task, &manifest, &access, true)
	if err != nil || !strings.Contains(string(raw), `"artifact_access":{"method":"GET"`) {
		t.Fatalf("artifact claim=%s err=%v", raw, err)
	}
	pending, err := MarshalRecipeTaskArtifactPending(1)
	if err != nil || string(pending) != `{"schema":"dirextalk.recipe-execution-task-claim-response/v1","status":"artifact_pending","lease_epoch":1}` {
		t.Fatalf("pending=%s err=%v", pending, err)
	}
	legacy, err := MarshalRecipeTaskClaimResponse(1, &task, &manifest)
	if err != nil || strings.Contains(string(legacy), "artifact_access") {
		t.Fatalf("legacy claim=%s err=%v", legacy, err)
	}
}
