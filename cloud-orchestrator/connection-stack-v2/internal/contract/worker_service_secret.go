package contract

import (
	"encoding/json"
)

const MaxWorkerServiceSecretRequestBytes = 8 * 1024

type WorkerServiceSecretRequest struct {
	TaskID         string `json:"task_id"`
	ExecutionID    string `json:"execution_id"`
	ManifestDigest string `json:"manifest_digest"`
	ArtifactDigest string `json:"artifact_digest"`
	SlotID         string `json:"slot_id"`
	SecretRef      string `json:"secret_ref"`
}

func ParseWorkerServiceSecretRequest(raw []byte) (WorkerServiceSecretRequest, error) {
	var request WorkerServiceSecretRequest
	if len(raw) == 0 || len(raw) > MaxWorkerServiceSecretRequestBytes {
		return request, errCode("invalid_worker_service_secret_request")
	}
	fields, err := exactJSONObject(raw)
	if err != nil || !exactFields(fields, []string{"task_id", "execution_id", "manifest_digest", "artifact_digest", "slot_id", "secret_ref"}) || decodeSingle(raw, &request) != nil {
		return WorkerServiceSecretRequest{}, errCode("invalid_worker_service_secret_request")
	}
	if !recipeTaskIDPattern.MatchString(request.TaskID) || !recipeBindingIDPattern.MatchString(request.ExecutionID) || !namedSHA256Pattern.MatchString(request.ManifestDigest) || !namedSHA256Pattern.MatchString(request.ArtifactDigest) || !recipeBindingIDPattern.MatchString(request.SlotID) || !approvalSecretRefPattern.MatchString(request.SecretRef) {
		return WorkerServiceSecretRequest{}, errCode("invalid_worker_service_secret_request")
	}
	canonical, _ := json.Marshal(request)
	if string(canonical) != string(raw) {
		return WorkerServiceSecretRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}
