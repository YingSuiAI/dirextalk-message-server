package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type ApprovalKeyResolver interface {
	LookupApprovalKey(ctx context.Context, connectionID, signerKeyID string) (ed25519.PublicKey, bool)
}

type StaticApprovalKeyResolver struct {
	ConnectionID, SignerKeyID string
	PublicKey                 ed25519.PublicKey
}

func NewStaticApprovalKeyResolver(connectionID, signerKeyID, publicKeySPKIBase64 string) (*StaticApprovalKeyResolver, error) {
	decoded, err := base64.StdEncoding.DecodeString(publicKeySPKIBase64)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != publicKeySPKIBase64 {
		return nil, NewError("invalid_approval_key", 500)
	}
	parsed, err := x509.ParsePKIXPublicKey(decoded)
	key, ok := parsed.(ed25519.PublicKey)
	if err != nil || !ok || len(key) != ed25519.PublicKeySize || !contract.ValidConnectionID(connectionID) || !contract.ValidNodeKeyID(signerKeyID) {
		return nil, NewError("invalid_approval_key", 500)
	}
	return &StaticApprovalKeyResolver{ConnectionID: connectionID, SignerKeyID: signerKeyID, PublicKey: append(ed25519.PublicKey(nil), key...)}, nil
}

func (r StaticApprovalKeyResolver) LookupApprovalKey(_ context.Context, connectionID, signerKeyID string) (ed25519.PublicKey, bool) {
	if connectionID != r.ConnectionID || signerKeyID != r.SignerKeyID || len(r.PublicKey) != ed25519.PublicKeySize {
		return nil, false
	}
	return append(ed25519.PublicKey(nil), r.PublicKey...), true
}

type DeploymentBoundary struct {
	WorkerArtifact               contract.WorkerArtifactReference
	WorkerNetwork                contract.WorkerNetworkReference
	WorkerResourceManifestDigest string
	WorkerSecurityGroupID        string
	WorkerBootstrapEndpoint      string
}

type DeploymentSpec struct {
	ConnectionID           string `json:"connection_id"`
	DeploymentID           string `json:"deployment_id"`
	ClientToken            string `json:"client_token"`
	AMIId                  string `json:"ami_id"`
	InstanceType           string `json:"instance_type"`
	Architecture           string `json:"architecture"`
	DiskGiB                int64  `json:"disk_gib"`
	VPCID                  string `json:"vpc_id"`
	SubnetID               string `json:"subnet_id"`
	AvailabilityZone       string `json:"availability_zone"`
	SecurityGroupID        string `json:"security_group_id"`
	BootstrapSessionID     string `json:"bootstrap_session_id"`
	BootstrapEndpoint      string `json:"bootstrap_endpoint"`
	WorkerImageDigest      string `json:"worker_image_digest"`
	ArtifactManifestDigest string `json:"artifact_manifest_digest"`
	BootstrapExpiresAt     string `json:"bootstrap_expires_at"`
}

type DeploymentEvidence struct {
	InstanceID                     string
	VolumeIDs, NetworkInterfaceIDs []string
}

type DeploymentProvider interface {
	EnsureCreated(ctx context.Context, spec DeploymentSpec) (instanceID string, err error)
	ReadBack(ctx context.Context, spec DeploymentSpec, instanceID string) (DeploymentEvidence, error)
}

func (b Broker) executeDeployment(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, 400, contract.Code(err))
		return
	}
	identity := commandstore.Record{ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action}
	if existing, found, lookupErr := b.DeploymentStore.Lookup(request.Context(), command.ConnectionID, command.CommandID); lookupErr != nil {
		writeStoreError(response, lookupErr)
		return
	} else if found {
		b.writeDeploymentReplay(response, command, identity, existing)
		return
	}
	deploymentRequest, err := command.DeploymentRequest()
	if err != nil {
		writeError(response, 400, contract.Code(err))
		return
	}
	proof, err := command.Approval()
	if err != nil {
		writeError(response, 400, contract.Code(err))
		return
	}
	reservation, found, err := b.DeploymentStore.LookupDeployment(request.Context(), command.ConnectionID, deploymentRequest.DeploymentID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if found {
		expected, _, specErr := b.deploymentReservation(command, deploymentRequest, proof, commandstore.IssuedQuote{}, nil)
		if specErr != nil || !reservation.SameIdentity(expected) {
			writeError(response, 409, "deployment_id_conflict")
			return
		}
		b.resumeDeployment(response, request, command, identity, reservation)
		return
	}
	if err := command.ValidateAt(now); err != nil {
		status := 400
		if contract.Code(err) == "expired_command" {
			status = 401
		}
		writeError(response, status, contract.Code(err))
		return
	}
	approvalKey, ok := b.ApprovalResolver.LookupApprovalKey(request.Context(), command.ConnectionID, proof.SignerKeyID)
	if !ok {
		writeError(response, 403, "unknown_approval_key")
		return
	}
	if err := proof.Verify(approvalKey, now); err != nil {
		writeError(response, 403, contract.Code(err))
		return
	}
	issuedQuote, found, err := b.DeploymentStore.LookupIssuedQuote(request.Context(), command.ConnectionID, proof.QuoteID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !found {
		writeError(response, 409, "issued_quote_not_found")
		return
	}
	reservation, spec, err := b.deploymentReservation(command, deploymentRequest, proof, issuedQuote, &now)
	if err != nil {
		writeError(response, 409, contract.Code(err))
		return
	}
	stored, _, err := b.DeploymentStore.ReserveDeployment(request.Context(), reservation)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(reservation) {
		writeError(response, 409, "deployment_id_conflict")
		return
	}
	_ = spec
	b.resumeDeployment(response, request, command, identity, stored)
}

func (b Broker) deploymentReservation(command contract.Command, request contract.DeploymentRequest, proof contract.ApprovalProof, issued commandstore.IssuedQuote, now *time.Time) (commandstore.DeploymentReservation, DeploymentSpec, error) {
	if request.WorkerArtifact.Kind != b.DeploymentBoundary.WorkerArtifact.Kind || request.WorkerArtifact.AMIID != b.DeploymentBoundary.WorkerArtifact.AMIID || request.Network.VPCID != b.DeploymentBoundary.WorkerNetwork.VPCID || request.Network.SubnetID != b.DeploymentBoundary.WorkerNetwork.SubnetID || request.Network.AvailabilityZone != b.DeploymentBoundary.WorkerNetwork.AvailabilityZone || request.ResourceManifestDigest != b.DeploymentBoundary.WorkerResourceManifestDigest || b.DeploymentBoundary.WorkerSecurityGroupID == "" || b.DeploymentBoundary.WorkerBootstrapEndpoint == "" {
		return commandstore.DeploymentReservation{}, DeploymentSpec{}, contractError("worker_binding_mismatch")
	}
	requestSHA, _ := command.RequestSHA256()
	clientToken := "dtx-" + requestSHA[:60]
	issuedAt, parseErr := time.Parse("2006-01-02T15:04:05.000Z", command.IssuedAt)
	if parseErr != nil {
		return commandstore.DeploymentReservation{}, DeploymentSpec{}, contractError("invalid_command")
	}
	bootstrapSessionID := "bootstrap-" + requestSHA[:32]
	spec := DeploymentSpec{ConnectionID: command.ConnectionID, DeploymentID: request.DeploymentID, ClientToken: clientToken, AMIId: request.WorkerArtifact.AMIID, InstanceType: proof.ResourceScope.InstanceType, Architecture: proof.ResourceScope.Architecture, DiskGiB: int64(proof.ResourceScope.DiskGiB), VPCID: request.Network.VPCID, SubnetID: request.Network.SubnetID, AvailabilityZone: request.Network.AvailabilityZone, SecurityGroupID: b.DeploymentBoundary.WorkerSecurityGroupID, BootstrapSessionID: bootstrapSessionID, BootstrapEndpoint: b.DeploymentBoundary.WorkerBootstrapEndpoint, WorkerImageDigest: b.DeploymentBoundary.WorkerResourceManifestDigest, ArtifactManifestDigest: request.ResourceManifestDigest, BootstrapExpiresAt: contract.CanonicalInstant(issuedAt.Add(10 * time.Minute))}
	specJSON, _ := json.Marshal(spec)
	workerArchitecture := "x86_64"
	if spec.Architecture == "arm64" {
		workerArchitecture = "arm64"
	}
	workerSession := commandstore.WorkerSession{BootstrapSessionID: bootstrapSessionID, ConnectionID: command.ConnectionID, DeploymentID: request.DeploymentID, RequestSHA256: requestSHA, WorkerImageDigest: spec.WorkerImageDigest, ArtifactManifestDigest: spec.ArtifactManifestDigest, BootstrapEndpoint: spec.BootstrapEndpoint, ExpectedAMIID: spec.AMIId, ExpectedInstanceType: spec.InstanceType, ExpectedArchitecture: workerArchitecture, ExpectedVPCID: spec.VPCID, ExpectedSubnetID: spec.SubnetID, ExpectedAvailabilityZone: spec.AvailabilityZone, ExpectedSecurityGroupID: spec.SecurityGroupID, State: "issued", ExpiresAt: spec.BootstrapExpiresAt}
	secretScope := make([]commandstore.ApprovedSecretReference, len(proof.SecretScope))
	for index, reference := range proof.SecretScope {
		secretScope[index] = commandstore.ApprovedSecretReference{SecretRef: reference.SecretRef, Purpose: reference.Purpose, Delivery: reference.Delivery}
	}
	sort.Slice(secretScope, func(left, right int) bool {
		if secretScope[left].SecretRef != secretScope[right].SecretRef {
			return secretScope[left].SecretRef < secretScope[right].SecretRef
		}
		if secretScope[left].Purpose != secretScope[right].Purpose {
			return secretScope[left].Purpose < secretScope[right].Purpose
		}
		return secretScope[left].Delivery < secretScope[right].Delivery
	})
	reservation := commandstore.DeploymentReservation{ConnectionID: command.ConnectionID, DeploymentID: request.DeploymentID, CommandID: command.CommandID, RequestSHA256: requestSHA, ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, ApprovalID: proof.ApprovalID, ChallengeID: proof.ChallengeID, SignerKeyID: proof.SignerKeyID, QuoteID: proof.QuoteID, PlanHash: proof.PlanHash, RecipeDigest: proof.RecipeDigest, SecretScope: secretScope, ClientToken: clientToken, BootstrapSessionID: bootstrapSessionID, WorkerSession: workerSession, SpecJSON: specJSON, State: "reserved"}
	if now == nil {
		return reservation, spec, nil
	}
	quote, err := contract.ParseStoredQuote(issued.QuoteJSON)
	if err != nil {
		return commandstore.DeploymentReservation{}, DeploymentSpec{}, err
	}
	quoteDigest, err := quote.ApprovalDigest()
	if err != nil || quoteDigest != proof.QuoteDigest || quote.Schema != contract.QuoteSchema || quote.Currency != "USD" || quote.ConnectionID != command.ConnectionID || quote.QuoteID != proof.QuoteID || issued.ConnectionID != quote.ConnectionID || issued.QuoteID != quote.QuoteID || issued.ValidUntil != quote.ValidUntil || quote.ValidUntil != contract.CanonicalInstant(proof.QuoteValidUntil) || !proof.QuoteValidUntil.After(now.UTC()) || quote.Region != proof.ResourceScope.Region {
		return commandstore.DeploymentReservation{}, DeploymentSpec{}, contractError("issued_quote_mismatch")
	}
	var candidate *contract.QuotedCandidate
	for i := range quote.Candidates {
		if quote.Candidates[i].CandidateID == request.CandidateID {
			candidate = &quote.Candidates[i]
			break
		}
	}
	if candidate == nil || candidate.InstanceType != proof.ResourceScope.InstanceType || candidate.PurchaseOption != "on_demand" || proof.ResourceScope.PurchaseOption != "on_demand" || candidate.EstimatedDiskGiB != int64(proof.ResourceScope.DiskGiB) || candidate.Architecture != proof.ResourceScope.Architecture || candidate.VCPU != int64(proof.ResourceScope.VCPU) || candidate.MemoryMiB != int64(proof.ResourceScope.MemoryMiB) || candidate.GPUCount != int64(proof.ResourceScope.GPUCount) || candidate.GPUMemoryMiB != int64(proof.ResourceScope.GPUMemoryMiB) || !contains(candidate.AvailabilityZones, request.Network.AvailabilityZone) {
		return commandstore.DeploymentReservation{}, DeploymentSpec{}, contractError("issued_quote_mismatch")
	}
	return reservation, spec, nil
}

func (b Broker) resumeDeployment(response http.ResponseWriter, request *http.Request, command contract.Command, identity commandstore.Record, reservation commandstore.DeploymentReservation) {
	spec, err := b.storedDeploymentSpec(reservation)
	if err != nil {
		writeError(response, 500, "deployment_store_invalid")
		return
	}
	instanceID, err := b.DeploymentProvider.EnsureCreated(request.Context(), spec)
	if err != nil {
		writeProviderError(response, err)
		return
	}
	evidence, err := b.DeploymentProvider.ReadBack(request.Context(), spec, instanceID)
	if err != nil {
		writeProviderError(response, err)
		return
	}
	if evidence.InstanceID != instanceID {
		writeError(response, http.StatusBadGateway, "provider_readback_invalid")
		return
	}
	sort.Strings(evidence.VolumeIDs)
	sort.Strings(evidence.NetworkInterfaceIDs)
	resultJSON, err := contract.MarshalCommittedDeploymentResult(command, contract.DeploymentReceipt{InstanceID: evidence.InstanceID, VolumeIDs: evidence.VolumeIDs, NetworkInterfaceIDs: evidence.NetworkInterfaceIDs})
	if err != nil {
		writeError(response, 502, "provider_readback_invalid")
		return
	}
	identity.ResultJSON = resultJSON
	stored, created, err := b.DeploymentStore.FinalizeDeployment(request.Context(), reservation, identity)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) {
		writeError(response, 409, "command_id_conflict")
		return
	}
	if !created {
		b.writeDeploymentReplay(response, command, identity, stored)
		return
	}
	if !bytes.Equal(stored.ResultJSON, resultJSON) || contract.ValidateDeploymentResult(command, mustDeploymentResult(stored.ResultJSON)) != nil {
		writeError(response, 500, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, 200, stored.ResultJSON)
}

func (b Broker) storedDeploymentSpec(reservation commandstore.DeploymentReservation) (DeploymentSpec, error) {
	var spec DeploymentSpec
	if json.Unmarshal(reservation.SpecJSON, &spec) != nil {
		return DeploymentSpec{}, contractError("deployment_store_invalid")
	}
	canonical, err := json.Marshal(spec)
	if err != nil || !bytes.Equal(canonical, reservation.SpecJSON) {
		return DeploymentSpec{}, contractError("deployment_store_invalid")
	}
	session := reservation.WorkerSession
	wantArchitecture := "amd64"
	if session.ExpectedArchitecture == "arm64" {
		wantArchitecture = "arm64"
	}
	if spec.ConnectionID != reservation.ConnectionID || spec.DeploymentID != reservation.DeploymentID ||
		spec.ClientToken != reservation.ClientToken || spec.BootstrapSessionID != reservation.BootstrapSessionID ||
		spec.AMIId != session.ExpectedAMIID || spec.InstanceType != session.ExpectedInstanceType || spec.Architecture != wantArchitecture ||
		spec.VPCID != session.ExpectedVPCID || spec.SubnetID != session.ExpectedSubnetID ||
		spec.AvailabilityZone != session.ExpectedAvailabilityZone || spec.SecurityGroupID != session.ExpectedSecurityGroupID ||
		spec.BootstrapEndpoint != session.BootstrapEndpoint || spec.WorkerImageDigest != session.WorkerImageDigest ||
		spec.ArtifactManifestDigest != session.ArtifactManifestDigest || spec.BootstrapExpiresAt != session.ExpiresAt ||
		spec.AMIId != b.DeploymentBoundary.WorkerArtifact.AMIID || spec.VPCID != b.DeploymentBoundary.WorkerNetwork.VPCID ||
		spec.SubnetID != b.DeploymentBoundary.WorkerNetwork.SubnetID || spec.AvailabilityZone != b.DeploymentBoundary.WorkerNetwork.AvailabilityZone ||
		spec.SecurityGroupID != b.DeploymentBoundary.WorkerSecurityGroupID || spec.BootstrapEndpoint != b.DeploymentBoundary.WorkerBootstrapEndpoint ||
		spec.WorkerImageDigest != b.DeploymentBoundary.WorkerResourceManifestDigest {
		return DeploymentSpec{}, contractError("deployment_store_invalid")
	}
	return spec, nil
}

func (b Broker) writeDeploymentReplay(response http.ResponseWriter, command contract.Command, identity, stored commandstore.Record) {
	if !stored.SameIdentity(identity) {
		writeError(response, 409, "command_id_conflict")
		return
	}
	raw, err := contract.IdempotentDeploymentResult(command, stored.ResultJSON)
	if err != nil {
		writeError(response, 500, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, 200, raw)
}

func mustDeploymentResult(raw []byte) contract.DeploymentResult {
	var result contract.DeploymentResult
	_ = json.Unmarshal(raw, &result)
	return result
}
func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
func contractError(code string) error { return contract.NewError(code) }
