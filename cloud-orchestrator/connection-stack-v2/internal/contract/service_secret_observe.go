package contract

import (
	"bytes"
	"encoding/json"
	"regexp"
)

const ServiceSecretObservationSchema = "dirextalk.service-secret-observation/v1"

var (
	serviceSecretObserveRef       = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
	serviceSecretObserveBindingID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	serviceSecretObserveVersion   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,256}$`)
)

type ServiceSecretObserveRequest struct {
	SessionID      string `json:"session_id"`
	DeploymentID   string `json:"deployment_id"`
	TaskID         string `json:"task_id"`
	ExecutionID    string `json:"execution_id"`
	ManifestDigest string `json:"manifest_digest"`
	SecretRef      string `json:"secret_ref"`
	ContextDigest  string `json:"context_digest"`
}

type ServiceSecretObservation struct {
	Schema          string `json:"schema"`
	SessionID       string `json:"session_id"`
	Status          string `json:"status"`
	ProviderVersion string `json:"provider_version,omitempty"`
	BindingDigest   string `json:"binding_digest"`
	UpdatedMarker   string `json:"updated_marker"`
}

func (c Command) ServiceSecretObserveRequest() (ServiceSecretObserveRequest, error) {
	if c.Action != ActionServiceSecretObserve {
		return ServiceSecretObserveRequest{}, errCode("invalid_service_secret_observe_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return ServiceSecretObserveRequest{}, err
	}
	object, err := exactJSONObject(payload)
	fields := []string{"session_id", "deployment_id", "task_id", "execution_id", "manifest_digest", "secret_ref", "context_digest"}
	if err != nil || !exactFields(object, fields) {
		return ServiceSecretObserveRequest{}, errCode("invalid_service_secret_observe_request")
	}
	var request ServiceSecretObserveRequest
	if decodeSingle(payload, &request) != nil || !ValidID(request.SessionID) || !ValidID(request.DeploymentID) ||
		!ValidRecipeTaskID(request.TaskID) || !serviceSecretObserveBindingID.MatchString(request.ExecutionID) || !namedSHA256Pattern.MatchString(request.ManifestDigest) ||
		!serviceSecretObserveRef.MatchString(request.SecretRef) || !namedSHA256Pattern.MatchString(request.ContextDigest) {
		return ServiceSecretObserveRequest{}, errCode("invalid_service_secret_observe_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(payload, canonical) {
		return ServiceSecretObserveRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func MarshalServiceSecretObservation(command Command, observation ServiceSecretObservation) ([]byte, error) {
	request, err := command.ServiceSecretObserveRequest()
	if err != nil || validateServiceSecretObservation(request, observation) != nil {
		return nil, errCode("invalid_service_secret_observation")
	}
	return json.Marshal(observation)
}

func decodeServiceSecretObservation(raw []byte, observation *ServiceSecretObservation) error {
	object, err := exactJSONObject(raw)
	if err != nil {
		return errCode("receipt_store_invalid")
	}
	fields := []string{"schema", "session_id", "status", "binding_digest", "updated_marker"}
	var status string
	_ = json.Unmarshal(object["status"], &status)
	if status == "uploaded" || status == "completed" {
		fields = append(fields, "provider_version")
	}
	if !exactFields(object, fields) || decodeSingle(raw, observation) != nil {
		return errCode("receipt_store_invalid")
	}
	return nil
}

func validateServiceSecretObservation(request ServiceSecretObserveRequest, observation ServiceSecretObservation) error {
	if observation.Schema != ServiceSecretObservationSchema || observation.SessionID != request.SessionID ||
		observation.BindingDigest != request.ContextDigest || !sha256Pattern.MatchString(observation.UpdatedMarker) {
		return errCode("invalid_service_secret_observation")
	}
	switch observation.Status {
	case "pending_upload", "processing", "expired":
		if observation.ProviderVersion != "" {
			return errCode("invalid_service_secret_observation")
		}
	case "uploaded", "completed":
		if !serviceSecretObserveVersion.MatchString(observation.ProviderVersion) {
			return errCode("invalid_service_secret_observation")
		}
	default:
		return errCode("invalid_service_secret_observation")
	}
	return nil
}
