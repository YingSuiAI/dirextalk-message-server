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
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/broker"
)

const ServiceRestorePlanRequested = cloudmodule.OutboxKindServiceRestorePlanRequested

type ServiceRestorePlanClaim struct {
	OutboxID, Kind, AggregateType, AggregateID                              string
	RestorePlanID, ServiceID, DeploymentID, BackupID, ConnectionID, Region  string
	BrokerEndpoint, NodeKeyID, LeaseToken, JobID, PlanID                    string
	ExpectedGeneration, ServiceRevision, DeploymentRevision, BackupRevision int64
	Attempt                                                                 int
	Request                                                                 broker.ServiceRestorePlanRequest
	Command                                                                 ServiceRestorePlanCommand
}
type ServiceRestorePlanCommand struct {
	CommandID, RestorePlanID, ServiceID, DeploymentID, BackupID, ConnectionID string
	NodeKeyID, RequestDigest                                                  string
	ExpectedGeneration, NodeCounter                                           int64
	Attempt                                                                   int
	PayloadJSON, PayloadSHA256, RequestSHA256, SignedEnvelope, State          string
	IssuedAt, ExpiresAt                                                       time.Time
}
type SignedServiceRestorePlanCommand struct {
	EnvelopeJSON, PayloadJSON, PayloadSHA256, RequestSHA256 string
	IssuedAt, ExpiresAt                                     time.Time
}
type ServiceRestorePlanResult struct {
	Status                                string
	Plan                                  broker.ServiceRestorePlan
	CommandID, RequestSHA256, ReceiptJSON string
}
type ServiceRestorePlanStore interface {
	ClaimServiceRestorePlan(context.Context, string, time.Duration) (ServiceRestorePlanClaim, bool, error)
	PersistServiceRestorePlanCommand(context.Context, ServiceRestorePlanClaim, SignedServiceRestorePlanCommand) error
	MarkServiceRestorePlanStarted(context.Context, ServiceRestorePlanClaim) error
	CompleteServiceRestorePlan(context.Context, ServiceRestorePlanClaim, ServiceRestorePlanResult) error
	DeferServiceRestorePlan(context.Context, ServiceRestorePlanClaim, string, time.Time) error
	ExpireServiceRestorePlanCommand(context.Context, ServiceRestorePlanClaim) error
	FailServiceRestorePlan(context.Context, ServiceRestorePlanClaim, string) error
}
type ServiceRestorePlanTransport interface {
	BuildServiceRestorePlanCommand(ServiceRestorePlanCommand, broker.ServiceRestorePlanRequest) (SignedServiceRestorePlanCommand, error)
	RequestServiceRestorePlan(context.Context, string, ServiceRestorePlanCommand, SignedServiceRestorePlanCommand, broker.ServiceRestorePlanRequest) (ServiceRestorePlanResult, error)
}

func ServiceRestorePlanRequestDigest(r broker.ServiceRestorePlanRequest) (string, error) {
	raw, e := json.Marshal(r)
	if e != nil || len(raw) == 0 {
		return "", errors.New("service restore plan request is invalid")
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
func ValidateSignedServiceRestorePlanCommand(c SignedServiceRestorePlanCommand) error {
	if strings.TrimSpace(c.EnvelopeJSON) != c.EnvelopeJSON || c.EnvelopeJSON == "" || strings.TrimSpace(c.PayloadJSON) != c.PayloadJSON || c.PayloadJSON == "" || !lowerHexSHA256(c.PayloadSHA256) || !lowerHexSHA256(c.RequestSHA256) || c.IssuedAt.IsZero() || c.ExpiresAt.IsZero() || !c.ExpiresAt.After(c.IssuedAt) || c.ExpiresAt.Sub(c.IssuedAt) > 5*time.Minute {
		return errors.New("signed service restore plan command is invalid")
	}
	return nil
}
func ValidateServiceRestorePlanClaim(c ServiceRestorePlanClaim) error {
	if c.Kind != ServiceRestorePlanRequested || c.AggregateType != "service_restore_plan" || c.OutboxID == "" || c.AggregateID != c.RestorePlanID || c.RestorePlanID == "" || c.ServiceID == "" || c.DeploymentID == "" || c.BackupID == "" || c.ConnectionID == "" || c.Region == "" || c.BrokerEndpoint == "" || c.NodeKeyID == "" || c.LeaseToken == "" || c.JobID == "" || c.PlanID == "" || c.ExpectedGeneration <= 0 || c.ServiceRevision <= 0 || c.DeploymentRevision <= 0 || c.BackupRevision <= 0 || c.Attempt <= 0 {
		return errors.New("service restore plan claim is invalid")
	}
	d, e := ServiceRestorePlanRequestDigest(c.Request)
	if e != nil || c.Request.RestorePlanID != c.RestorePlanID || c.Request.ServiceID != c.ServiceID || c.Request.DeploymentID != c.DeploymentID || c.Request.BackupID != c.BackupID || c.Request.Region != c.Region || c.Command.CommandID == "" || c.Command.RestorePlanID != c.RestorePlanID || c.Command.ServiceID != c.ServiceID || c.Command.DeploymentID != c.DeploymentID || c.Command.BackupID != c.BackupID || c.Command.ConnectionID != c.ConnectionID || c.Command.NodeKeyID != c.NodeKeyID || c.Command.ExpectedGeneration != c.ExpectedGeneration || c.Command.NodeCounter <= 0 || c.Command.RequestDigest != d {
		return errors.New("service restore plan command does not bind claim")
	}
	return nil
}
func ValidateServiceRestorePlanResult(c ServiceRestorePlanClaim, s SignedServiceRestorePlanCommand, r ServiceRestorePlanResult) error {
	if ValidateServiceRestorePlanClaim(c) != nil || ValidateSignedServiceRestorePlanCommand(s) != nil || (r.Status != "restore_plan_ready" && r.Status != "idempotent") || r.Plan.RestorePlanID != c.RestorePlanID || r.Plan.ServiceID != c.ServiceID || r.Plan.DeploymentID != c.DeploymentID || r.Plan.BackupID != c.BackupID || r.Plan.InstanceID != c.Request.InstanceID || r.Plan.Region != c.Region || r.CommandID != c.Command.CommandID || r.RequestSHA256 != s.RequestSHA256 || r.ReceiptJSON == "" {
		return errors.New("service restore plan result does not bind claim")
	}
	return nil
}
