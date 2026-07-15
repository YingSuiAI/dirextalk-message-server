package contract

import (
	"bytes"
	"encoding/json"
	"time"
)

const DeploymentObservationSchema = "dirextalk.aws.deployment-observation/v1"

type DeploymentObserveRequest struct {
	DeploymentID string `json:"deployment_id"`
}

type DeploymentObserveReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type DeploymentObservationResource struct {
	Status     string `json:"status"`
	InstanceID string `json:"instance_id"`
}

// DeploymentObservationWorker is deliberately de-secreted. The exact wire
// shape excludes the bootstrap session identifier, token or token hash,
// endpoint, instance identity document, and raw Worker events.
type DeploymentObservationWorker struct {
	BootstrapSessionState string  `json:"bootstrap_session_state"`
	LeaseEpoch            int64   `json:"lease_epoch"`
	LeaseExpiresAt        *string `json:"lease_expires_at"`
	LastSequence          int64   `json:"last_sequence"`
	LastEventAt           *string `json:"last_event_at"`
}

type DeploymentObservation struct {
	Schema       string                        `json:"schema"`
	DeploymentID string                        `json:"deployment_id"`
	Resource     DeploymentObservationResource `json:"resource"`
	Worker       DeploymentObservationWorker   `json:"worker"`
	ObservedAt   string                        `json:"observed_at"`
}

type DeploymentObserveResult struct {
	Status      string                   `json:"status"`
	Receipt     DeploymentObserveReceipt `json:"receipt"`
	Observation DeploymentObservation    `json:"observation"`
}

// DeploymentObserveRequest returns the only payload admitted by the signed
// read-only deployment.observe command.
func (c Command) DeploymentObserveRequest() (DeploymentObserveRequest, error) {
	if c.Action != ActionDeploymentObserve {
		return DeploymentObserveRequest{}, errCode("invalid_deployment_observe_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return DeploymentObserveRequest{}, err
	}
	object, err := exactJSONObject(payload)
	if err != nil || !exactFields(object, []string{"deployment_id"}) {
		return DeploymentObserveRequest{}, errCode("invalid_deployment_observe_request")
	}
	var request DeploymentObserveRequest
	if err := decodeSingle(payload, &request); err != nil || !ValidID(request.DeploymentID) {
		return DeploymentObserveRequest{}, errCode("invalid_deployment_observe_request")
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(canonical, payload) {
		return DeploymentObserveRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

// MarshalDeploymentObserveResult emits the precise Broker-to-Orchestrator
// response shape. A replayed receipt still carries a fresh observation.
func MarshalDeploymentObserveResult(command Command, observation DeploymentObservation, idempotent bool) ([]byte, error) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return nil, err
	}
	status, disposition := "deployment_observed", "committed"
	if idempotent {
		status, disposition = "idempotent", "idempotent"
	}
	result := DeploymentObserveResult{
		Status: status,
		Receipt: DeploymentObserveReceipt{
			Schema: ReceiptSchema, Disposition: disposition, ConnectionID: command.ConnectionID,
			ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter,
			CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionDeploymentObserve,
		},
		Observation: observation,
	}
	if err := ValidateDeploymentObserveResult(command, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func ValidateDeploymentObserveResult(command Command, result DeploymentObserveResult) error {
	request, err := command.DeploymentObserveRequest()
	if err != nil {
		return err
	}
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	if result.Status != "deployment_observed" && result.Status != "idempotent" {
		return errCode("invalid_broker_status")
	}
	wantDisposition := "committed"
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	}
	receipt := result.Receipt
	if receipt.Schema != ReceiptSchema || receipt.Disposition != wantDisposition ||
		receipt.ConnectionID != command.ConnectionID || receipt.ExpectedGeneration != command.ExpectedGeneration ||
		receipt.NodeCounter != command.NodeCounter || receipt.CommandID != command.CommandID ||
		receipt.RequestSHA256 != requestSHA || receipt.Action != ActionDeploymentObserve {
		return errCode("invalid_deployment_observe_receipt")
	}
	return validateDeploymentObservation(command, request, result.Observation)
}

func DecodeDeploymentObserveResult(command Command, raw []byte) (DeploymentObserveResult, error) {
	var result DeploymentObserveResult
	if err := decodeDeploymentObserveResult(raw, &result); err != nil {
		return DeploymentObserveResult{}, err
	}
	if err := ValidateDeploymentObserveResult(command, result); err != nil {
		return DeploymentObserveResult{}, err
	}
	return result, nil
}

func validateDeploymentObservation(command Command, request DeploymentObserveRequest, observation DeploymentObservation) error {
	if observation.Schema != DeploymentObservationSchema || observation.DeploymentID != request.DeploymentID ||
		observation.Resource.Status != "provisioning" || !instanceIDPattern.MatchString(observation.Resource.InstanceID) ||
		observation.Worker.LeaseEpoch < 0 || observation.Worker.LeaseEpoch > maxSafeInteger ||
		observation.Worker.LastSequence < 0 || observation.Worker.LastSequence > maxSafeInteger {
		return errCode("invalid_deployment_observation")
	}
	observedAt, err := parseCanonicalInstant(observation.ObservedAt)
	if err != nil {
		return errCode("invalid_deployment_observation")
	}
	issuedAt, issuedErr := parseCanonicalInstant(command.IssuedAt)
	expiresAt, expiresErr := parseCanonicalInstant(command.ExpiresAt)
	if issuedErr != nil || expiresErr != nil || observedAt.Before(issuedAt) || observedAt.After(expiresAt) {
		return errCode("invalid_deployment_observation")
	}
	leaseExpiresAt, hasLease, err := optionalDeploymentObserveInstant(observation.Worker.LeaseExpiresAt)
	if err != nil {
		return errCode("invalid_deployment_observation")
	}
	lastEventAt, hasLastEvent, err := optionalDeploymentObserveInstant(observation.Worker.LastEventAt)
	if err != nil {
		return errCode("invalid_deployment_observation")
	}
	switch observation.Worker.BootstrapSessionState {
	case "issued", "bound":
		if observation.Worker.LeaseEpoch != 0 || hasLease || observation.Worker.LastSequence != 0 || hasLastEvent {
			return errCode("invalid_deployment_observation")
		}
	case "active":
		if observation.Worker.LeaseEpoch <= 0 || !hasLease || !leaseExpiresAt.After(observedAt) ||
			(observation.Worker.LastSequence == 0 && hasLastEvent) ||
			(observation.Worker.LastSequence > 0 && (!hasLastEvent || lastEventAt.After(observedAt))) {
			return errCode("invalid_deployment_observation")
		}
	default:
		return errCode("invalid_deployment_observation")
	}
	return nil
}

func optionalDeploymentObserveInstant(value *string) (time.Time, bool, error) {
	if value == nil {
		return time.Time{}, false, nil
	}
	parsed, err := parseCanonicalInstant(*value)
	return parsed, true, err
}

func decodeDeploymentObserveResult(raw []byte, result *DeploymentObserveResult) error {
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "observation"}) {
		return errCode("invalid_deployment_observation")
	}
	receipt, err := exactJSONObject(object["receipt"])
	if err != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) {
		return errCode("invalid_deployment_observation")
	}
	observation, err := exactJSONObject(object["observation"])
	if err != nil || !exactFields(observation, []string{"schema", "deployment_id", "resource", "worker", "observed_at"}) {
		return errCode("invalid_deployment_observation")
	}
	resource, err := exactJSONObject(observation["resource"])
	if err != nil || !exactFields(resource, []string{"status", "instance_id"}) {
		return errCode("invalid_deployment_observation")
	}
	worker, err := exactJSONObject(observation["worker"])
	if err != nil || !exactFields(worker, []string{"bootstrap_session_state", "lease_epoch", "lease_expires_at", "last_sequence", "last_event_at"}) {
		return errCode("invalid_deployment_observation")
	}
	if err := decodeSingle(raw, result); err != nil {
		return errCode("invalid_deployment_observation")
	}
	return nil
}
