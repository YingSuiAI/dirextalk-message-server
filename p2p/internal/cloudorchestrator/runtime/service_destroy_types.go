package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

const ServiceDestroyRequested = cloudmodule.OutboxKindServiceDestroyRequested

type ServiceDestroyClaim struct {
	OutboxID, Kind, AggregateType, AggregateID string
	ServiceID, DeploymentID, PlanID, JobID     string
	ConnectionID, Region, BrokerEndpoint       string
	NodeKeyID, LeaseToken                      string
	ExpectedGeneration, ServiceRevision        int64
	DeploymentRevision                         int64
	Attempt                                    int
	Approval                                   cloudcontracts.ServiceDestroyApprovalV1
	Request                                    broker.DeploymentDestroyRequest
	Command                                    ServiceDestroyCommand
}

type ServiceDestroyCommand struct {
	CommandID, ServiceID, DeploymentID, ConnectionID string
	NodeKeyID, RequestDigest, PayloadJSON            string
	PayloadSHA256, RequestSHA256, SignedEnvelope     string
	ExpectedGeneration, NodeCounter                  int64
	Attempt                                          int
	IssuedAt, ExpiresAt                              time.Time
	State                                            string
}

type SignedServiceDestroyCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}

type ServiceDestroyResult struct {
	Status                                string
	DeploymentID, InstanceID, ReceiptJSON string
	VolumeIDs, NetworkInterfaceIDs        []string
	CommandID, RequestSHA256              string
}

type ServiceDestroyStore interface {
	ClaimServiceDestroy(context.Context, string, time.Duration) (ServiceDestroyClaim, bool, error)
	PersistServiceDestroyCommand(context.Context, ServiceDestroyClaim, SignedServiceDestroyCommand) error
	MarkServiceDestroyStarted(context.Context, ServiceDestroyClaim) error
	CompleteServiceDestroy(context.Context, ServiceDestroyClaim, ServiceDestroyResult) error
	DeferServiceDestroy(context.Context, ServiceDestroyClaim, string, time.Time) error
	FailServiceDestroy(context.Context, ServiceDestroyClaim, string) error
}

type ServiceDestroyTransport interface {
	BuildServiceDestroyCommand(ServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.ServiceDestroyApprovalV1) (SignedServiceDestroyCommand, error)
	RequestServiceDestroy(context.Context, string, ServiceDestroyCommand, SignedServiceDestroyCommand, broker.DeploymentDestroyRequest, cloudcontracts.ServiceDestroyApprovalV1) (ServiceDestroyResult, error)
}

func ServiceDestroyRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "service_destroy_retryable"), cause: cause}
}

func ServiceDestroyRequestDigest(request broker.DeploymentDestroyRequest) (string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return "", err
	}
	// The broker owns validation; hashing canonical JSON here gives the command
	// journal a stable identity without duplicating its closed field rules.
	if len(encoded) == 0 {
		return "", errors.New("service destroy request is invalid")
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateServiceDestroyClaim(claim ServiceDestroyClaim) error {
	if claim.Kind != ServiceDestroyRequested || claim.AggregateType != "service" || claim.OutboxID == "" || claim.AggregateID != claim.ServiceID ||
		claim.ServiceID == "" || claim.DeploymentID == "" || claim.PlanID == "" || claim.JobID == "" || claim.ConnectionID == "" ||
		claim.Region == "" || claim.BrokerEndpoint == "" || claim.NodeKeyID == "" || claim.LeaseToken == "" || claim.ExpectedGeneration <= 0 ||
		claim.ServiceRevision <= 0 || claim.DeploymentRevision <= 0 || claim.Attempt <= 0 || claim.Approval.Validate() != nil || claim.Approval.Signature == "" {
		return errors.New("service destroy claim is invalid")
	}
	if err := cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region); err != nil {
		return errors.New("service destroy endpoint is invalid")
	}
	digest, err := ServiceDestroyRequestDigest(claim.Request)
	target := claim.Approval.Target()
	if err != nil || claim.Command.CommandID == "" || claim.Command.ServiceID != claim.ServiceID || claim.Command.DeploymentID != claim.DeploymentID ||
		claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration ||
		claim.Command.NodeCounter <= 0 || claim.Command.Attempt <= 0 || claim.Command.RequestDigest != digest || target.ServiceID != claim.ServiceID ||
		target.DeploymentID != claim.DeploymentID || target.CloudConnectionID != claim.ConnectionID || target.InstanceID != claim.Request.InstanceID ||
		!sameStrings(target.VolumeIDs, claim.Request.VolumeIDs) || !sameStrings(target.NetworkInterfaceIDs, claim.Request.NetworkInterfaceIDs) {
		return errors.New("service destroy command does not bind claim")
	}
	return nil
}

func ValidateSignedServiceDestroyCommand(signed SignedServiceDestroyCommand) error {
	if strings.TrimSpace(signed.EnvelopeJSON) != signed.EnvelopeJSON || signed.EnvelopeJSON == "" || strings.TrimSpace(signed.PayloadJSON) != signed.PayloadJSON ||
		signed.PayloadJSON == "" || !lowerHexSHA256(signed.PayloadSHA256) || !lowerHexSHA256(signed.RequestSHA256) || signed.IssuedAt.IsZero() ||
		signed.ExpiresAt.IsZero() || !signed.ExpiresAt.After(signed.IssuedAt) || signed.ExpiresAt.Sub(signed.IssuedAt) > 5*time.Minute {
		return errors.New("signed service destroy command is invalid")
	}
	return nil
}

func ValidateServiceDestroyResult(claim ServiceDestroyClaim, signed SignedServiceDestroyCommand, result ServiceDestroyResult) error {
	if ValidateServiceDestroyClaim(claim) != nil || ValidateSignedServiceDestroyCommand(signed) != nil || result.Status != "verified_destroyed" ||
		result.DeploymentID != claim.DeploymentID || result.InstanceID != claim.Request.InstanceID || result.CommandID != claim.Command.CommandID ||
		result.RequestSHA256 != signed.RequestSHA256 || result.ReceiptJSON == "" || !sameStrings(result.VolumeIDs, claim.Request.VolumeIDs) ||
		!sameStrings(result.NetworkInterfaceIDs, claim.Request.NetworkInterfaceIDs) {
		return errors.New("service destroy result does not bind claim")
	}
	return nil
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
