package contract

import (
	"bytes"
	"encoding/json"
	"regexp"
	"sort"
)

type DeploymentWorkerArtifact struct {
	Kind  string `json:"kind"`
	AMIID string `json:"ami_id"`
}

type DeploymentNetwork struct {
	VPCID            string `json:"vpc_id"`
	SubnetID         string `json:"subnet_id"`
	AvailabilityZone string `json:"availability_zone"`
}

type DeploymentRequest struct {
	Schema                 string                   `json:"schema"`
	DeploymentID           string                   `json:"deployment_id"`
	ConnectionGeneration   int64                    `json:"connection_generation"`
	PlanHash               string                   `json:"plan_hash"`
	PlanRevision           uint64                   `json:"plan_revision"`
	QuoteID                string                   `json:"quote_id"`
	QuoteDigest            string                   `json:"quote_digest"`
	CandidateID            string                   `json:"candidate_id"`
	ResourceManifestDigest string                   `json:"resource_manifest_digest"`
	WorkerArtifact         DeploymentWorkerArtifact `json:"worker_artifact"`
	Network                DeploymentNetwork        `json:"network"`
}

const DeploymentReceiptSchema = "dirextalk.aws.deployment-receipt/v1"

type DeploymentCommandReceipt struct {
	Schema             string `json:"schema"`
	Disposition        string `json:"disposition"`
	ConnectionID       string `json:"connection_id"`
	ExpectedGeneration int64  `json:"expected_generation"`
	NodeCounter        int64  `json:"node_counter"`
	CommandID          string `json:"command_id"`
	RequestSHA256      string `json:"request_sha256"`
	Action             string `json:"action"`
}

type DeploymentReceipt struct {
	Schema              string   `json:"schema"`
	ConnectionID        string   `json:"connection_id"`
	DeploymentID        string   `json:"deployment_id"`
	RequestSHA256       string   `json:"request_sha256"`
	ResourceStatus      string   `json:"resource_status"`
	InstanceID          string   `json:"instance_id"`
	VolumeIDs           []string `json:"volume_ids"`
	NetworkInterfaceIDs []string `json:"network_interface_ids"`
}

type DeploymentResult struct {
	Status     string                   `json:"status"`
	Receipt    DeploymentCommandReceipt `json:"receipt"`
	Deployment DeploymentReceipt        `json:"deployment"`
}

func MarshalCommittedDeploymentResult(command Command, evidence DeploymentReceipt) ([]byte, error) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return nil, err
	}
	request, err := command.DeploymentRequest()
	if err != nil {
		return nil, err
	}
	evidence.Schema = DeploymentReceiptSchema
	evidence.ConnectionID = command.ConnectionID
	evidence.DeploymentID = request.DeploymentID
	evidence.RequestSHA256 = requestSHA
	evidence.ResourceStatus = "provisioning"
	result := DeploymentResult{Status: "deployment_created", Receipt: DeploymentCommandReceipt{Schema: ReceiptSchema, Disposition: "committed", ConnectionID: command.ConnectionID, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, CommandID: command.CommandID, RequestSHA256: requestSHA, Action: ActionDeploymentCreate}, Deployment: evidence}
	if err := ValidateDeploymentResult(command, result); err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

func ValidateDeploymentResult(command Command, result DeploymentResult) error {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		return err
	}
	request, err := command.DeploymentRequest()
	if err != nil {
		return err
	}
	if (result.Status != "deployment_created" && result.Status != "idempotent") || result.Receipt.Schema != ReceiptSchema || result.Receipt.ConnectionID != command.ConnectionID || result.Receipt.ExpectedGeneration != command.ExpectedGeneration || result.Receipt.NodeCounter != command.NodeCounter || result.Receipt.CommandID != command.CommandID || result.Receipt.RequestSHA256 != requestSHA || result.Receipt.Action != ActionDeploymentCreate {
		return errCode("invalid_deployment_receipt")
	}
	wantDisposition := "committed"
	if result.Status == "idempotent" {
		wantDisposition = "idempotent"
	}
	if result.Receipt.Disposition != wantDisposition {
		return errCode("invalid_deployment_receipt")
	}
	d := result.Deployment
	if d.Schema != DeploymentReceiptSchema || d.ConnectionID != command.ConnectionID || d.DeploymentID != request.DeploymentID || d.RequestSHA256 != requestSHA || d.ResourceStatus != "provisioning" || !instanceIDPattern.MatchString(d.InstanceID) || !canonicalResourceIDs(d.VolumeIDs, volumeIDPattern) || !canonicalResourceIDs(d.NetworkInterfaceIDs, interfaceIDPattern) {
		return errCode("invalid_deployment_receipt")
	}
	return nil
}

func IdempotentDeploymentResult(command Command, raw []byte) ([]byte, error) {
	var result DeploymentResult
	if err := decodeDeploymentResult(raw, &result); err != nil || result.Status != "deployment_created" || result.Receipt.Disposition != "committed" || ValidateDeploymentResult(command, result) != nil {
		return nil, errCode("receipt_store_invalid")
	}
	result.Status = "idempotent"
	result.Receipt.Disposition = "idempotent"
	return json.Marshal(result)
}

// StoredDeploymentReceipt returns only a strict committed deployment receipt.
// It is used by the AWS-owned store to bind the Worker session in the same
// transaction that commits the command receipt.
func StoredDeploymentReceipt(raw []byte) (DeploymentReceipt, error) {
	var result DeploymentResult
	if err := decodeDeploymentResult(raw, &result); err != nil || result.Status != "deployment_created" || result.Receipt.Disposition != "committed" {
		return DeploymentReceipt{}, errCode("invalid_deployment_receipt")
	}
	d := result.Deployment
	if d.Schema != DeploymentReceiptSchema || !ValidConnectionID(d.ConnectionID) || !ValidID(d.DeploymentID) || !sha256Pattern.MatchString(d.RequestSHA256) || d.ResourceStatus != "provisioning" || !instanceIDPattern.MatchString(d.InstanceID) || !canonicalResourceIDs(d.VolumeIDs, volumeIDPattern) || !canonicalResourceIDs(d.NetworkInterfaceIDs, interfaceIDPattern) {
		return DeploymentReceipt{}, errCode("invalid_deployment_receipt")
	}
	return d, nil
}

func decodeDeploymentResult(raw []byte, result *DeploymentResult) error {
	object, err := exactJSONObject(raw)
	if err != nil || !exactFields(object, []string{"status", "receipt", "deployment"}) {
		return errCode("receipt_store_invalid")
	}
	receipt, err := exactJSONObject(object["receipt"])
	if err != nil || !exactFields(receipt, []string{"schema", "disposition", "connection_id", "expected_generation", "node_counter", "command_id", "request_sha256", "action"}) {
		return errCode("receipt_store_invalid")
	}
	deployment, err := exactJSONObject(object["deployment"])
	if err != nil || !exactFields(deployment, []string{"schema", "connection_id", "deployment_id", "request_sha256", "resource_status", "instance_id", "volume_ids", "network_interface_ids"}) {
		return errCode("receipt_store_invalid")
	}
	if err := decodeSingle(raw, result); err != nil {
		return errCode("receipt_store_invalid")
	}
	return nil
}

var (
	instanceIDPattern  = regexp.MustCompile(`^i-[0-9a-f]{8,17}$`)
	volumeIDPattern    = regexp.MustCompile(`^vol-[0-9a-f]{8,17}$`)
	interfaceIDPattern = regexp.MustCompile(`^eni-[0-9a-f]{8,17}$`)
)

func canonicalResourceIDs(values []string, pattern *regexp.Regexp) bool {
	if len(values) == 0 || !sort.StringsAreSorted(values) {
		return false
	}
	for i, v := range values {
		if !pattern.MatchString(v) || (i > 0 && values[i-1] == v) {
			return false
		}
	}
	return true
}

func (c Command) DeploymentRequest() (DeploymentRequest, error) {
	if c.Action != ActionDeploymentCreate {
		return DeploymentRequest{}, errCode("invalid_deployment_request")
	}
	payload, err := c.actionPayload()
	if err != nil {
		return DeploymentRequest{}, err
	}
	fields, err := exactJSONObject(payload)
	if err != nil || !exactFields(fields, []string{"schema", "deployment_id", "connection_generation", "plan_hash", "plan_revision", "quote_id", "quote_digest", "candidate_id", "resource_manifest_digest", "worker_artifact", "network"}) {
		return DeploymentRequest{}, errCode("invalid_deployment_request")
	}
	artifact, err := exactJSONObject(fields["worker_artifact"])
	if err != nil || !exactFields(artifact, []string{"kind", "ami_id"}) {
		return DeploymentRequest{}, errCode("invalid_deployment_request")
	}
	network, err := exactJSONObject(fields["network"])
	if err != nil || !exactFields(network, []string{"vpc_id", "subnet_id", "availability_zone"}) {
		return DeploymentRequest{}, errCode("invalid_deployment_request")
	}
	var request DeploymentRequest
	if err := decodeSingle(payload, &request); err != nil || validateDeploymentRequest(request, c.ExpectedGeneration) != nil {
		return DeploymentRequest{}, errCode("invalid_deployment_request")
	}
	canonical, _ := json.Marshal(request)
	if !bytes.Equal(canonical, payload) {
		return DeploymentRequest{}, errCode("noncanonical_payload")
	}
	return request, nil
}

func validateDeploymentRequest(r DeploymentRequest, generation int64) error {
	if r.Schema != DeploymentCreateSchema || !ValidID(r.DeploymentID) || r.ConnectionGeneration != generation || generation < 1 || !namedSHA256Pattern.MatchString(r.PlanHash) || r.PlanRevision == 0 || r.PlanRevision > uint64(maxSafeInteger) || !ValidID(r.QuoteID) || !namedSHA256Pattern.MatchString(r.QuoteDigest) || !ValidID(r.CandidateID) || !namedSHA256Pattern.MatchString(r.ResourceManifestDigest) || r.WorkerArtifact.Kind != "fixed_ami" || !amiIDPattern.MatchString(r.WorkerArtifact.AMIID) || !vpcIDPattern.MatchString(r.Network.VPCID) || !subnetIDPattern.MatchString(r.Network.SubnetID) || !availabilityZonePattern.MatchString(r.Network.AvailabilityZone) {
		return errCode("invalid_deployment_request")
	}
	return nil
}

func (c Command) Approval() (ApprovalProof, error) {
	if c.Action != ActionDeploymentCreate || len(c.ApprovalProof) == 0 {
		return ApprovalProof{}, errCode("invalid_approval_proof")
	}
	return ParseApprovalProof(c.ApprovalProof)
}

func (c Command) ValidateDeploymentBinding() error {
	request, err := c.DeploymentRequest()
	if err != nil {
		return err
	}
	proof, err := c.Approval()
	if err != nil {
		return err
	}
	issued, err := parseCanonicalInstant(c.IssuedAt)
	if err != nil {
		return errCode("invalid_command")
	}
	expires, err := parseCanonicalInstant(c.ExpiresAt)
	if err != nil {
		return errCode("invalid_command")
	}
	if proof.CloudConnectionID != c.ConnectionID || proof.PlanHash != request.PlanHash || proof.PlanRevision != request.PlanRevision || proof.QuoteID != request.QuoteID || proof.QuoteDigest != request.QuoteDigest || !proof.ExpiresAt.After(issued) || proof.ExpiresAt.Before(expires) || !ValidAvailabilityZone(proof.ResourceScope.Region, request.Network.AvailabilityZone) || !containsString(proof.ResourceScope.AvailabilityZones, request.Network.AvailabilityZone) {
		return errCode("approval_proof_mismatch")
	}
	return nil
}

func (c Command) ApprovalProofPayloadSHA256() (string, error) {
	proof, err := c.Approval()
	if err != nil {
		return "", err
	}
	return proof.PayloadSHA256()
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
