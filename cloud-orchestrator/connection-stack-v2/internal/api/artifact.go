package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

const artifactURLTTL = 10 * time.Minute

func (b Broker) executeArtifactPut(w http.ResponseWriter, r *http.Request, command contract.Command, now time.Time) {
	if prepare, err := contract.ParseArtifactPutPrepare(command); err == nil {
		b.prepareArtifact(w, r, command, prepare, now)
		return
	}
	complete, err := contract.ParseArtifactPutComplete(command)
	if err != nil {
		writeError(w, http.StatusBadRequest, contract.Code(err))
		return
	}
	b.completeArtifact(w, r, command, complete, now)
}

func (b Broker) prepareArtifact(w http.ResponseWriter, r *http.Request, command contract.Command, request contract.ArtifactPutPrepareRequest, now time.Time) {
	if !b.validArtifactPlan(r, command.ConnectionID, request.ArtifactBinding) {
		writeError(w, http.StatusForbidden, "artifact_scope_mismatch")
		return
	}
	receiptValue, _ := contract.ArtifactReceipt(command)
	expires := now.Add(artifactURLTTL).UTC()
	key := request.ArtifactBinding.ObjectKey()
	state := contract.ArtifactState{ArtifactBinding: request.ArtifactBinding, State: "uploading", ExpiresAt: expires.Format("2006-01-02T15:04:05.000Z")}
	durable, _ := json.Marshal(struct {
		Schema   string                 `json:"schema"`
		Status   string                 `json:"status"`
		Artifact contract.ArtifactState `json:"artifact"`
	}{Schema: contract.ArtifactPutPrepareResultSchema, Status: "uploading", Artifact: state})
	receipt := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: receiptValue.RequestSHA256, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action, ResultJSON: durable}
	record := commandstore.ArtifactRecord{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: receiptValue.RequestSHA256, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Binding: request.ArtifactBinding, ObjectKey: key, State: "uploading", ExpiresAt: state.ExpiresAt}
	_, stored, _, err := b.ArtifactStore.PrepareArtifact(r.Context(), receipt, record)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	url, urlExpiry, err := b.ArtifactProvider.PresignPut(r.Context(), stored.ObjectKey, stored.Binding, artifactURLTTL)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "artifact_provider_unavailable")
		return
	}
	upload := contract.ArtifactUpload{Method: "PUT", URL: url, ExpiresAt: urlExpiry.UTC().Format("2006-01-02T15:04:05.000Z"), Headers: map[string]string{"Content-Type": contract.ArtifactMediaType, "x-amz-checksum-sha256": stored.Binding.ChecksumBase64(), "x-amz-server-side-encryption": "aws:kms"}}
	responseState := artifactState(stored)
	responseState.ExpiresAt = upload.ExpiresAt
	raw, _ := contract.MarshalArtifactPrepareResult(receiptValue, responseState, upload)
	writeRawJSON(w, http.StatusOK, raw)
}

func (b Broker) completeArtifact(w http.ResponseWriter, r *http.Request, command contract.Command, request contract.ArtifactPutCompleteRequest, now time.Time) {
	if !b.validArtifactPlan(r, command.ConnectionID, request.ArtifactBinding) {
		writeError(w, http.StatusForbidden, "artifact_scope_mismatch")
		return
	}
	stored, found, err := b.ArtifactStore.LookupArtifact(r.Context(), command.ConnectionID, request.DeploymentID, request.TaskID)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !found || !stored.Binding.Same(request.ArtifactBinding) || (stored.State != "uploading" && !(stored.State == "verified" && stored.VersionID == request.VersionID)) {
		writeError(w, http.StatusConflict, "artifact_put_conflict")
		return
	}
	if stored.State == "uploading" {
		observed, err := b.ArtifactProvider.Head(r.Context(), stored.ObjectKey, request.VersionID)
		if err != nil {
			writeError(w, http.StatusTooEarly, "artifact_upload_pending")
			return
		}
		if observed.VersionID != request.VersionID || observed.SizeBytes != request.SizeBytes || observed.ContentType != request.MediaType || observed.ChecksumSHA256 != request.ChecksumBase64() || observed.ServerSideEncryption != "aws:kms" {
			writeError(w, http.StatusUnprocessableEntity, "artifact_verification_failed")
			return
		}
	}
	receiptValue, _ := contract.ArtifactReceipt(command)
	stored.CommandID = command.CommandID
	stored.RequestSHA256 = receiptValue.RequestSHA256
	stored.ExpectedGeneration = command.ExpectedGeneration
	stored.NodeCounter = command.NodeCounter
	stored.State = "verified"
	stored.VersionID = request.VersionID
	stored.ExpiresAt = ""
	stored.VerifiedAt = now.UTC().Format("2006-01-02T15:04:05.000Z")
	state := artifactState(stored)
	durable, _ := json.Marshal(struct {
		Schema   string                 `json:"schema"`
		Status   string                 `json:"status"`
		Artifact contract.ArtifactState `json:"artifact"`
	}{Schema: contract.ArtifactPutCompleteResultSchema, Status: "verified", Artifact: state})
	receipt := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: receiptValue.RequestSHA256, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action, ResultJSON: durable}
	_, stored, _, err = b.ArtifactStore.CompleteArtifact(r.Context(), receipt, stored)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	raw, _ := contract.MarshalArtifactCompleteResult(receiptValue, artifactState(stored))
	writeRawJSON(w, http.StatusOK, raw)
}

func (b Broker) validArtifactPlan(r *http.Request, connectionID string, binding contract.ArtifactBinding) bool {
	reservation, found, err := b.DeploymentStore.LookupDeployment(r.Context(), connectionID, binding.DeploymentID)
	if err != nil || !found || reservation.State != "finalized" || reservation.RecipeDigest != binding.RecipeDigest {
		return false
	}
	task, found, err := b.RecipeTasks.LookupRecipeTask(r.Context(), binding.DeploymentID, binding.TaskID)
	if err != nil || !found || task.ConnectionID != connectionID || task.ExecutionID != binding.ExecutionID || task.RecipeExecutionManifestDigest != binding.ManifestDigest || (task.Status != "queued" && task.Status != "running") {
		return false
	}
	manifest, err := contract.ParseRecipeExecutionManifestJSON(task.ManifestJSON)
	if err != nil {
		return false
	}
	digest, err := manifest.Digest()
	return err == nil && digest == binding.ManifestDigest && manifest.DeploymentID == binding.DeploymentID && manifest.ExecutionID == binding.ExecutionID && manifest.PlanHash == reservation.PlanHash && manifest.RecipeDigest == binding.RecipeDigest && manifest.ArtifactDigest == binding.ArtifactDigest && manifest.WorkerResourceManifestDigest == reservation.WorkerSession.ArtifactManifestDigest
}
func artifactState(r commandstore.ArtifactRecord) contract.ArtifactState {
	return contract.ArtifactState{ArtifactBinding: r.Binding, State: r.State, ExpiresAt: r.ExpiresAt, VersionID: r.VersionID, VerifiedAt: r.VerifiedAt}
}
