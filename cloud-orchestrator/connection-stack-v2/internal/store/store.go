// Package store owns the durable command replay fence used by the standalone
// Connection Stack. It stores only signed-command identity, safe public result
// JSON, counters, and issued quote records; it never stores credentials.
package store

import (
	"context"
	"errors"
)

type Record struct {
	ConnectionID       string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	Action             string
	ResultJSON         []byte
}

func (r Record) SameIdentity(other Record) bool {
	return r.ConnectionID == other.ConnectionID && r.CommandID == other.CommandID &&
		r.RequestSHA256 == other.RequestSHA256 && r.ExpectedGeneration == other.ExpectedGeneration &&
		r.NodeCounter == other.NodeCounter && r.Action == other.Action
}

type IssuedQuote struct {
	ConnectionID  string
	QuoteID       string
	PlanDigest    string
	CommandID     string
	RequestSHA256 string
	ValidUntil    string
	QuoteJSON     []byte
}

type Repository interface {
	Lookup(ctx context.Context, connectionID, commandID string) (Record, bool, error)
	Commit(ctx context.Context, record Record, quote *IssuedQuote) (stored Record, created bool, err error)
}

type DeploymentReservation struct {
	ConnectionID       string
	DeploymentID       string
	CommandID          string
	RequestSHA256      string
	ExpectedGeneration int64
	NodeCounter        int64
	ApprovalID         string
	ChallengeID        string
	SignerKeyID        string
	QuoteID            string
	ClientToken        string
	SpecJSON           []byte
	ResultJSON         []byte
	State              string
}

func (r DeploymentReservation) SameIdentity(other DeploymentReservation) bool {
	return r.ConnectionID == other.ConnectionID && r.DeploymentID == other.DeploymentID && r.CommandID == other.CommandID && r.RequestSHA256 == other.RequestSHA256 && r.ExpectedGeneration == other.ExpectedGeneration && r.NodeCounter == other.NodeCounter && r.ApprovalID == other.ApprovalID && r.ChallengeID == other.ChallengeID && r.SignerKeyID == other.SignerKeyID && r.QuoteID == other.QuoteID && r.ClientToken == other.ClientToken && string(r.SpecJSON) == string(other.SpecJSON)
}

type DeploymentRepository interface {
	Repository
	LookupIssuedQuote(ctx context.Context, connectionID, quoteID string) (IssuedQuote, bool, error)
	LookupDeployment(ctx context.Context, connectionID, deploymentID string) (DeploymentReservation, bool, error)
	ReserveDeployment(ctx context.Context, reservation DeploymentReservation) (stored DeploymentReservation, created bool, err error)
	FinalizeDeployment(ctx context.Context, reservation DeploymentReservation, receipt Record) (stored Record, created bool, err error)
}

type Error struct{ Code string }

func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "connection stack store error"
	}
	return "connection stack store error: " + e.Code
}

func NewError(code string) error { return &Error{Code: code} }

func Code(err error) string {
	var target *Error
	if errors.As(err, &target) && target.Code != "" {
		return target.Code
	}
	return "connection_stack_store_unavailable"
}
