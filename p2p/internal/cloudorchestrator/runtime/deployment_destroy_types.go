package runtime

import (
	"context"
	"errors"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

const DeploymentDestroyRequested = "cloud.deployment.destroy.requested"

type DeploymentDestroyClaim struct {
	OutboxID, Kind, AggregateType, AggregateID string
	DeploymentID, PlanID, JobID                string
	DeploymentExecution, DeploymentOutcome     string
	ConnectionID, Region, BrokerEndpoint       string
	NodeKeyID, LeaseToken                      string
	ExpectedGeneration, DeploymentRevision     int64
	Attempt                                    int
	Approval                                   cloudcontracts.DeploymentDestroyApprovalV1
	Request                                    broker.DeploymentDestroyRequest
	Command                                    ServiceDestroyCommand
}

type DeploymentDestroyStore interface {
	ClaimDeploymentDestroy(context.Context, string, time.Duration) (DeploymentDestroyClaim, bool, error)
	PersistDeploymentDestroyCommand(context.Context, DeploymentDestroyClaim, SignedServiceDestroyCommand) error
	MarkDeploymentDestroyStarted(context.Context, DeploymentDestroyClaim) error
	CompleteDeploymentDestroy(context.Context, DeploymentDestroyClaim, ServiceDestroyResult) error
	DeferDeploymentDestroy(context.Context, DeploymentDestroyClaim, string, time.Time) error
	FailDeploymentDestroy(context.Context, DeploymentDestroyClaim, string) error
}

type DeploymentDestroyTransport interface {
	BuildDeploymentDestroyCommand(ServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.DeploymentDestroyApprovalV1) (SignedServiceDestroyCommand, error)
	RequestDeploymentDestroy(context.Context, string, ServiceDestroyCommand, SignedServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.DeploymentDestroyApprovalV1) (ServiceDestroyResult, error)
}

func ValidateDeploymentDestroyClaim(claim DeploymentDestroyClaim) error {
	if claim.Kind != DeploymentDestroyRequested || claim.AggregateType != "deployment" || claim.OutboxID == "" || claim.AggregateID != claim.DeploymentID ||
		claim.DeploymentID == "" || claim.PlanID == "" || claim.JobID == "" || claim.DeploymentExecution != "finished" || !deploymentDestroyTerminalOutcome(claim.DeploymentOutcome) || claim.ConnectionID == "" || claim.Region == "" || claim.BrokerEndpoint == "" ||
		claim.NodeKeyID == "" || claim.LeaseToken == "" || claim.ExpectedGeneration <= 0 || claim.DeploymentRevision <= 0 || claim.Attempt <= 0 ||
		claim.Approval.Validate() != nil || claim.Approval.Signature == "" || claim.Request.ServiceID != "" {
		return errors.New("deployment destroy claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return errors.New("deployment destroy endpoint is invalid")
	}
	digest, err := ServiceDestroyRequestDigest(claim.Request)
	target := claim.Approval.Target()
	if err != nil || claim.Command.CommandID == "" || claim.Command.ServiceID != "" || claim.Command.DeploymentID != claim.DeploymentID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration ||
		claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 || claim.Command.RequestDigest != digest || target.DeploymentID != claim.DeploymentID ||
		target.DeploymentRevision != uint64(claim.DeploymentRevision-1) || target.PlanID != claim.PlanID || target.CloudConnectionID != claim.ConnectionID || target.InstanceID != claim.Request.InstanceID ||
		!sameStrings(target.VolumeIDs, claim.Request.VolumeIDs) || !sameStrings(target.NetworkInterfaceIDs, claim.Request.NetworkInterfaceIDs) ||
		!sameStrings(target.SecretRefs, claim.Request.SecretRefs) {
		return errors.New("deployment destroy command does not bind claim")
	}
	return nil
}

func deploymentDestroyTerminalOutcome(outcome string) bool {
	switch outcome {
	case "succeeded", "failed", "canceled", "interrupted":
		return true
	default:
		return false
	}
}

func ValidateDeploymentDestroyResult(claim DeploymentDestroyClaim, signed SignedServiceDestroyCommand, result ServiceDestroyResult) error {
	if ValidateDeploymentDestroyClaim(claim) != nil || ValidateSignedServiceDestroyCommand(signed) != nil || result.Status != "verified_destroyed" ||
		result.DeploymentID != claim.DeploymentID || result.InstanceID != claim.Request.InstanceID || result.CommandID != claim.Command.CommandID ||
		result.RequestSHA256 != signed.RequestSHA256 || result.ReceiptJSON == "" || !sameStrings(result.VolumeIDs, claim.Request.VolumeIDs) ||
		!sameStrings(result.NetworkInterfaceIDs, claim.Request.NetworkInterfaceIDs) || !sameStrings(result.SecretRefs, claim.Request.SecretRefs) {
		return errors.New("deployment destroy result does not bind claim")
	}
	return nil
}
