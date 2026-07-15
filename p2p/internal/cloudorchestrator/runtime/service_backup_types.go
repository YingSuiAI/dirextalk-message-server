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

const ServiceBackupRequested = cloudmodule.OutboxKindServiceBackupRequested

type ServiceBackupClaim struct {
	OutboxID, Kind, AggregateType, AggregateID                  string
	BackupID, ServiceID, DeploymentID, PlanID, JobID            string
	ConnectionID, Region, BrokerEndpoint, NodeKeyID, LeaseToken string
	ExpectedGeneration, ServiceRevision, DeploymentRevision     int64
	Attempt                                                     int
	Approval                                                    cloudcontracts.ServiceBackupApprovalV1
	Request                                                     broker.ServiceBackupRequest
	Command                                                     ServiceBackupCommand
}
type ServiceBackupCommand struct {
	CommandID, BackupID, ServiceID, DeploymentID, ConnectionID                          string
	NodeKeyID, RequestDigest, PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope string
	ExpectedGeneration, NodeCounter                                                     int64
	Attempt                                                                             int
	IssuedAt, ExpiresAt                                                                 time.Time
	State                                                                               string
}
type SignedServiceBackupCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}
type ServiceBackupResult struct {
	Status, BackupID, ServiceID, DeploymentID, InstanceID, ImageID, ReceiptJSON string
	Snapshots                                                                   []broker.ServiceBackupSnapshot
	CommandID, RequestSHA256                                                    string
}
type ServiceBackupStore interface {
	ClaimServiceBackup(context.Context, string, time.Duration) (ServiceBackupClaim, bool, error)
	PersistServiceBackupCommand(context.Context, ServiceBackupClaim, SignedServiceBackupCommand) error
	MarkServiceBackupStarted(context.Context, ServiceBackupClaim) error
	CompleteServiceBackup(context.Context, ServiceBackupClaim, ServiceBackupResult) error
	DeferServiceBackup(context.Context, ServiceBackupClaim, string, time.Time) error
	FailServiceBackup(context.Context, ServiceBackupClaim, string) error
}
type ServiceBackupTransport interface {
	BuildServiceBackupCommand(ServiceBackupCommand, broker.ServiceBackupRequest, cloudcontracts.ServiceBackupApprovalV1) (SignedServiceBackupCommand, error)
	RequestServiceBackup(context.Context, string, ServiceBackupCommand, SignedServiceBackupCommand, broker.ServiceBackupRequest, cloudcontracts.ServiceBackupApprovalV1) (ServiceBackupResult, error)
}

func ServiceBackupRetryable(code string, cause error) error {
	return retryableError{code: normalizedErrorCode(code, "service_backup_retryable"), cause: cause}
}
func ServiceBackupRequestDigest(r broker.ServiceBackupRequest) (string, error) {
	raw, e := json.Marshal(r)
	if e != nil || len(raw) == 0 {
		return "", errors.New("service backup request is invalid")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func ValidateServiceBackupClaim(c ServiceBackupClaim) error {
	if c.Kind != ServiceBackupRequested || c.AggregateType != "service_backup" || c.OutboxID == "" || c.AggregateID != c.BackupID || c.BackupID == "" || c.ServiceID == "" || c.DeploymentID == "" || c.PlanID == "" || c.JobID == "" || c.ConnectionID == "" || c.Region == "" || c.BrokerEndpoint == "" || c.NodeKeyID == "" || c.LeaseToken == "" || c.ExpectedGeneration <= 0 || c.ServiceRevision <= 0 || c.DeploymentRevision <= 0 || c.Attempt <= 0 || c.Approval.Validate() != nil || c.Approval.Signature == "" {
		return errors.New("service backup claim is invalid")
	}
	if cloudmodule.ValidateConnectionRegistrationEndpoint(c.BrokerEndpoint, c.Region) != nil {
		return errors.New("service backup endpoint is invalid")
	}
	d, e := ServiceBackupRequestDigest(c.Request)
	if e != nil || c.Command.CommandID == "" || c.Command.BackupID != c.BackupID || c.Command.ServiceID != c.ServiceID || c.Command.DeploymentID != c.DeploymentID || c.Command.ConnectionID != c.ConnectionID || c.Command.NodeKeyID != c.NodeKeyID || c.Command.ExpectedGeneration != c.ExpectedGeneration || c.Command.NodeCounter <= 0 || c.Command.RequestDigest != d ||
		c.Request.Schema != broker.ServiceBackupSchema || c.Request.BackupID != c.BackupID || c.Request.ServiceID != c.ServiceID || c.Request.DeploymentID != c.DeploymentID || c.Request.RetentionPolicy != cloudcontracts.ServiceBackupRetentionManual ||
		c.Approval.BackupID != c.BackupID || c.Approval.ServiceID != c.ServiceID || c.Approval.ServiceRevision != uint64(c.ServiceRevision) || c.Approval.DeploymentID != c.DeploymentID || c.Approval.DeploymentRevision != uint64(c.DeploymentRevision) || c.Approval.CloudConnectionID != c.ConnectionID || c.Approval.InstanceID != c.Request.InstanceID || c.Approval.RetentionPolicy != c.Request.RetentionPolicy || !sameStrings(c.Approval.VolumeIDs, c.Request.VolumeIDs) {
		return errors.New("service backup command does not bind claim")
	}
	return nil
}
func ValidateSignedServiceBackupCommand(c SignedServiceBackupCommand) error {
	if strings.TrimSpace(c.EnvelopeJSON) != c.EnvelopeJSON || c.EnvelopeJSON == "" || strings.TrimSpace(c.PayloadJSON) != c.PayloadJSON || c.PayloadJSON == "" || !lowerHexSHA256(c.PayloadSHA256) || !lowerHexSHA256(c.RequestSHA256) || c.IssuedAt.IsZero() || c.ExpiresAt.IsZero() || !c.ExpiresAt.After(c.IssuedAt) || c.ExpiresAt.Sub(c.IssuedAt) > 5*time.Minute {
		return errors.New("signed service backup command is invalid")
	}
	return nil
}
func ValidateServiceBackupResult(c ServiceBackupClaim, s SignedServiceBackupCommand, r ServiceBackupResult) error {
	if ValidateServiceBackupClaim(c) != nil || ValidateSignedServiceBackupCommand(s) != nil || r.Status != "backup_available" || r.BackupID != c.BackupID || r.ServiceID != c.ServiceID || r.DeploymentID != c.DeploymentID || r.InstanceID != c.Request.InstanceID || r.ImageID == "" || r.CommandID != c.Command.CommandID || r.RequestSHA256 != s.RequestSHA256 || r.ReceiptJSON == "" || len(r.Snapshots) != len(c.Request.VolumeIDs) {
		return errors.New("service backup result does not bind claim")
	}
	return nil
}
