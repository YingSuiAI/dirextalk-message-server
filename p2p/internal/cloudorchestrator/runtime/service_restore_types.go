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

const ServiceRestoreRequested = cloudmodule.OutboxKindServiceRestoreRequested

type ServiceRestoreClaim struct {
	OutboxID, Kind, AggregateType, AggregateID                              string
	RestoreID, ServiceID, DeploymentID, BackupID, PlanID, JobID             string
	ConnectionID, Region, BrokerEndpoint, NodeKeyID, LeaseToken             string
	ExpectedGeneration, ServiceRevision, DeploymentRevision, BackupRevision int64
	Attempt                                                                 int
	Approval                                                                cloudcontracts.ServiceRestoreApprovalV1
	Request                                                                 broker.ServiceRestoreRequest
	Command                                                                 ServiceRestoreCommand
}

type ServiceRestoreCommand struct {
	CommandID, RestoreID, ServiceID, DeploymentID, ConnectionID                         string
	NodeKeyID, RequestDigest, PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope string
	ExpectedGeneration, NodeCounter                                                     int64
	Attempt                                                                             int
	IssuedAt, ExpiresAt                                                                 time.Time
	State                                                                               string
}

type SignedServiceRestoreCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}

type ServiceRestoreResult struct {
	Status, CommandID, RequestSHA256, ReceiptJSON string
	Evidence                                      broker.ServiceRestoreAWSEvidence
}

type ServiceRestoreStore interface {
	ClaimServiceRestore(context.Context, string, time.Duration) (ServiceRestoreClaim, bool, error)
	PersistServiceRestoreCommand(context.Context, ServiceRestoreClaim, SignedServiceRestoreCommand) error
	MarkServiceRestoreStarted(context.Context, ServiceRestoreClaim) error
	CompleteServiceRestore(context.Context, ServiceRestoreClaim, ServiceRestoreResult) error
	DeferServiceRestore(context.Context, ServiceRestoreClaim, string, time.Time) error
	FailServiceRestore(context.Context, ServiceRestoreClaim, string) error
}

type ServiceRestoreTransport interface {
	BuildServiceRestoreCommand(ServiceRestoreCommand, broker.ServiceRestoreRequest, cloudcontracts.ServiceRestoreApprovalV1) (SignedServiceRestoreCommand, error)
	RequestServiceRestore(context.Context, string, ServiceRestoreCommand, SignedServiceRestoreCommand, broker.ServiceRestoreRequest, cloudcontracts.ServiceRestoreApprovalV1) (ServiceRestoreResult, error)
}

func ServiceRestoreRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "service_restore_retryable"), cause: cause}
}

func ServiceRestoreRequestDigest(request broker.ServiceRestoreRequest) (string, error) {
	raw, err := json.Marshal(request)
	if err != nil || len(raw) == 0 {
		return "", errors.New("service restore request is invalid")
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func ValidateServiceRestoreClaim(claim ServiceRestoreClaim) error {
	if claim.Kind != ServiceRestoreRequested || claim.AggregateType != "service_restore" || claim.OutboxID == "" || claim.AggregateID != claim.RestoreID || claim.RestoreID == "" || claim.ServiceID == "" || claim.DeploymentID == "" || claim.BackupID == "" || claim.PlanID == "" || claim.JobID == "" || claim.ConnectionID == "" || claim.Region == "" || claim.BrokerEndpoint == "" || claim.NodeKeyID == "" || claim.LeaseToken == "" || claim.ExpectedGeneration <= 0 || claim.ServiceRevision <= 0 || claim.DeploymentRevision <= 0 || claim.BackupRevision <= 0 || claim.Attempt <= 0 || claim.Approval.Validate() != nil || claim.Approval.Signature == "" {
		return errors.New("service restore claim is invalid")
	}
	if cloudmodule.ValidateConnectionRegistrationEndpoint(claim.BrokerEndpoint, claim.Region) != nil {
		return errors.New("service restore endpoint is invalid")
	}
	digest, err := ServiceRestoreRequestDigest(claim.Request)
	if err != nil || claim.Command.CommandID == "" || claim.Command.RestoreID != claim.RestoreID || claim.Command.ServiceID != claim.ServiceID || claim.Command.DeploymentID != claim.DeploymentID || claim.Command.ConnectionID != claim.ConnectionID || claim.Command.NodeKeyID != claim.NodeKeyID || claim.Command.ExpectedGeneration != claim.ExpectedGeneration || claim.Command.NodeCounter <= 0 || claim.Command.RequestDigest != digest {
		return errors.New("service restore command does not bind claim")
	}
	approval := claim.Approval.ServiceRestoreTargetV1
	if claim.Request.Schema != broker.ServiceRestoreSchema || claim.Request.RestoreID != claim.RestoreID || claim.Request.ServiceID != claim.ServiceID || claim.Request.DeploymentID != claim.DeploymentID || claim.Request.BackupID != claim.BackupID || approval.RestoreID != claim.RestoreID || approval.ServiceID != claim.ServiceID || approval.ServiceRevision != uint64(claim.ServiceRevision) || approval.DeploymentID != claim.DeploymentID || approval.DeploymentRevision != uint64(claim.DeploymentRevision) || approval.BackupID != claim.BackupID || approval.BackupRevision != uint64(claim.BackupRevision) || approval.CloudConnectionID != claim.ConnectionID {
		return errors.New("service restore approval does not bind claim")
	}
	return nil
}

func ValidateSignedServiceRestoreCommand(command SignedServiceRestoreCommand) error {
	if strings.TrimSpace(command.EnvelopeJSON) != command.EnvelopeJSON || command.EnvelopeJSON == "" || strings.TrimSpace(command.PayloadJSON) != command.PayloadJSON || command.PayloadJSON == "" || !lowerHexSHA256(command.PayloadSHA256) || !lowerHexSHA256(command.RequestSHA256) || command.IssuedAt.IsZero() || command.ExpiresAt.IsZero() || !command.ExpiresAt.After(command.IssuedAt) || command.ExpiresAt.Sub(command.IssuedAt) > 5*time.Minute {
		return errors.New("signed service restore command is invalid")
	}
	return nil
}

func ValidateServiceRestoreResult(claim ServiceRestoreClaim, signed SignedServiceRestoreCommand, result ServiceRestoreResult) error {
	if ValidateServiceRestoreClaim(claim) != nil || ValidateSignedServiceRestoreCommand(signed) != nil || (result.Status != "aws_restore_applied" && result.Status != "aws_original_restored" && result.Status != "restore_blocked") || result.CommandID != claim.Command.CommandID || result.RequestSHA256 != signed.RequestSHA256 || result.ReceiptJSON == "" || result.Evidence.RestoreID != claim.RestoreID || result.Evidence.ServiceID != claim.ServiceID || result.Evidence.DeploymentID != claim.DeploymentID || result.Evidence.BackupID != claim.BackupID || result.Evidence.InstanceID != claim.Request.InstanceID {
		return errors.New("service restore result does not bind claim")
	}
	return nil
}
