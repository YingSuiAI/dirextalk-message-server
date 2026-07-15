package contract

import (
	"encoding/json"
	"sort"
)

const (
	DeploymentDestroySchema       = "dirextalk.aws.deployment-destroy/v1"
	DeploymentDestroyResultSchema = "dirextalk.aws.deployment-destroy-result/v1"
)

type DeploymentDestroyRequest struct {
	Schema              string   `json:"schema"`
	ServiceID           string   `json:"service_id"`
	DeploymentID        string   `json:"deployment_id"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
	SecretRefs          []string `json:"secret_refs,omitempty"`
}

type DeploymentDestroyEvidence struct {
	DeploymentID        string   `json:"deployment_id"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
	SecretRefs          []string `json:"secret_refs,omitempty"`
}

type DeploymentDestroyResult struct {
	Schema     string                    `json:"schema"`
	Status     string                    `json:"status"`
	Receipt    DeploymentCommandReceipt  `json:"receipt"`
	Deployment DeploymentDestroyEvidence `json:"deployment"`
}

func (command Command) DeploymentDestroyRequest() (DeploymentDestroyRequest, error) {
	if command.Action != ActionDeploymentDestroy {
		return DeploymentDestroyRequest{}, errCode("invalid_payload")
	}
	payload, err := decodeCanonicalBase64(command.PayloadB64)
	if err != nil {
		return DeploymentDestroyRequest{}, errCode("invalid_payload")
	}
	fields, err := exactJSONObject(payload)
	legacyFields := []string{"schema", "service_id", "deployment_id", "instance_id", "volume_ids", "network_interface_ids"}
	if err != nil || (!exactFields(fields, legacyFields) && !exactFields(fields, append(legacyFields, "secret_refs"))) {
		return DeploymentDestroyRequest{}, errCode("invalid_payload")
	}
	var request DeploymentDestroyRequest
	if decodeSingle(payload, &request) != nil || request.validate() != nil {
		return DeploymentDestroyRequest{}, errCode("invalid_payload")
	}
	if _, present := fields["secret_refs"]; present && len(request.SecretRefs) == 0 {
		return DeploymentDestroyRequest{}, errCode("invalid_payload")
	}
	return normalizeDeploymentDestroyRequest(request), nil
}

func (request DeploymentDestroyRequest) validate() error {
	if request.Schema != DeploymentDestroySchema || !approvalIdentifierPattern.MatchString(request.ServiceID) || !approvalIdentifierPattern.MatchString(request.DeploymentID) || !destroyInstanceIDPattern.MatchString(request.InstanceID) || !validDestroyResourceIDs(request.VolumeIDs, destroyVolumeIDPattern) || !validDestroyResourceIDs(request.NetworkInterfaceIDs, destroyNetworkInterfaceIDPattern) || !validOptionalDestroySecretRefs(request.SecretRefs) {
		return errCode("invalid_payload")
	}
	return nil
}

func (request DeploymentDestroyRequest) Validate() error { return request.validate() }

func (command Command) ValidateDeploymentDestroyBinding() error {
	request, err := command.DeploymentDestroyRequest()
	if err != nil {
		return err
	}
	proof, err := command.ServiceDestroyApproval()
	if err != nil {
		return err
	}
	proof = normalizeServiceDestroyApprovalProof(proof)
	if proof.CloudConnectionID != command.ConnectionID || proof.ServiceID != request.ServiceID || proof.DeploymentID != request.DeploymentID || proof.InstanceID != request.InstanceID || !sameStrings(proof.VolumeIDs, request.VolumeIDs) || !sameStrings(proof.NetworkInterfaceIDs, request.NetworkInterfaceIDs) || !sameStrings(proof.SecretRefs, request.SecretRefs) {
		return errCode("approval_scope_mismatch")
	}
	return nil
}

func (command Command) ServiceDestroyApprovalPayloadSHA256() (string, error) {
	if err := command.ValidateDeploymentDestroyBinding(); err != nil {
		return "", err
	}
	proof, _ := command.ServiceDestroyApproval()
	return proof.PayloadSHA256()
}

func MarshalCommittedDeploymentDestroyResult(command Command, evidence DeploymentDestroyEvidence) ([]byte, error) {
	if err := command.ValidateDeploymentDestroyBinding(); err != nil {
		return nil, err
	}
	request, _ := command.DeploymentDestroyRequest()
	evidence.VolumeIDs = append([]string(nil), evidence.VolumeIDs...)
	evidence.NetworkInterfaceIDs = append([]string(nil), evidence.NetworkInterfaceIDs...)
	evidence.SecretRefs = append([]string(nil), evidence.SecretRefs...)
	sort.Strings(evidence.VolumeIDs)
	sort.Strings(evidence.NetworkInterfaceIDs)
	sort.Strings(evidence.SecretRefs)
	if evidence.DeploymentID != request.DeploymentID || evidence.InstanceID != request.InstanceID || !sameStrings(evidence.VolumeIDs, request.VolumeIDs) || !sameStrings(evidence.NetworkInterfaceIDs, request.NetworkInterfaceIDs) || !sameStrings(evidence.SecretRefs, request.SecretRefs) {
		return nil, errCode("provider_readback_invalid")
	}
	requestSHA, _ := command.RequestSHA256()
	result := DeploymentDestroyResult{Schema: DeploymentDestroyResultSchema, Status: "verified_destroyed", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionDeploymentDestroy}, Deployment: evidence}
	return json.Marshal(result)
}

func ValidateDeploymentDestroyResult(command Command, result DeploymentDestroyResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil || result.Schema != DeploymentDestroyResultSchema || result.Status != "verified_destroyed" || result.Receipt.Schema != ReceiptSchema || result.Receipt.Disposition != "committed" || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionDeploymentDestroy {
		return errCode("invalid_result")
	}
	request, err := command.DeploymentDestroyRequest()
	if err != nil || result.Deployment.DeploymentID != request.DeploymentID || result.Deployment.InstanceID != request.InstanceID {
		return errCode("invalid_result")
	}
	volumes := append([]string(nil), result.Deployment.VolumeIDs...)
	interfaces := append([]string(nil), result.Deployment.NetworkInterfaceIDs...)
	secretRefs := append([]string(nil), result.Deployment.SecretRefs...)
	sort.Strings(volumes)
	sort.Strings(interfaces)
	sort.Strings(secretRefs)
	if !sameStrings(volumes, request.VolumeIDs) || !sameStrings(interfaces, request.NetworkInterfaceIDs) || !sameStrings(secretRefs, request.SecretRefs) {
		return errCode("invalid_result")
	}
	return nil
}

func normalizeDeploymentDestroyRequest(request DeploymentDestroyRequest) DeploymentDestroyRequest {
	request.VolumeIDs = append([]string(nil), request.VolumeIDs...)
	request.NetworkInterfaceIDs = append([]string(nil), request.NetworkInterfaceIDs...)
	request.SecretRefs = append([]string(nil), request.SecretRefs...)
	sort.Strings(request.VolumeIDs)
	sort.Strings(request.NetworkInterfaceIDs)
	sort.Strings(request.SecretRefs)
	return request
}
