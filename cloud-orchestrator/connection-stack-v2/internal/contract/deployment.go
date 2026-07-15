package contract

import (
	"bytes"
	"encoding/json"
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
