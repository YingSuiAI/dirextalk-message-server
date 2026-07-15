package runtime

import (
	"context"
	"errors"
	"time"
)

type ServiceOperationStore interface {
	ClaimServiceOperation(context.Context, string, time.Duration) (RecipeInstallClaim, bool, error)
	MarkServiceOperationStarted(context.Context, RecipeInstallClaim) error
	PersistServiceOperationCommand(context.Context, RecipeInstallClaim, SignedRecipeInstallCommand) error
	CommitServiceOperation(context.Context, RecipeInstallClaim, RecipeInstallResult) error
	DeferServiceOperation(context.Context, RecipeInstallClaim, string, time.Time) error
	ExpireServiceOperationCommand(context.Context, RecipeInstallClaim) error
	FailServiceOperation(context.Context, RecipeInstallClaim, string) error
}

type ServiceOperationRunner struct {
	store     ServiceOperationStore
	transport RecipeInstallTransport
	cfg       Config
}

func NewServiceOperationRunner(store ServiceOperationStore, transport RecipeInstallTransport, cfg Config) *ServiceOperationRunner {
	return &ServiceOperationRunner{store: store, transport: transport, cfg: cfg}
}

func (r *ServiceOperationRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil || r.transport == nil || r.cfg.WorkerID == "" || r.cfg.Lease <= 0 || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease || r.cfg.RetryDelay <= 0 {
		return false, errors.New("service operation runner is not configured")
	}
	claim, found, err := r.store.ClaimServiceOperation(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if err != nil || !found {
		return found, err
	}
	if err = ValidateRecipeInstallClaim(claim); err != nil {
		return true, r.store.FailServiceOperation(ctx, claim, "service_operation_claim_invalid")
	}
	if err = r.store.MarkServiceOperationStarted(ctx, claim); err != nil {
		return true, err
	}
	now := r.now()
	var signed SignedRecipeInstallCommand
	if claim.Command.SignedEnvelope != "" {
		signed = SignedRecipeInstallCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
		if !signed.ExpiresAt.After(now) {
			if err = r.store.ExpireServiceOperationCommand(ctx, claim); err != nil {
				return true, err
			}
			return true, nil
		}
	} else if claim.Phase == RecipeInstallPhaseIssue {
		signed, err = r.transport.BuildRecipeInstallIssueCommand(claim.Command, claim.IssueRequest, now)
	} else {
		signed, err = r.transport.BuildRecipeInstallObserveCommand(claim.Command, claim.ObserveRequest, now)
	}
	if err != nil {
		return true, r.store.FailServiceOperation(ctx, claim, "service_operation_command_invalid")
	}
	if claim.Command.SignedEnvelope == "" {
		if err = r.store.PersistServiceOperationCommand(ctx, claim, signed); err != nil {
			return true, err
		}
	}
	if ValidateSignedRecipeInstallCommand(claim.Command, signed) != nil {
		return true, r.store.FailServiceOperation(ctx, claim, "service_operation_command_invalid")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	defer cancel()
	var result RecipeInstallResult
	if claim.Phase == RecipeInstallPhaseIssue {
		result, err = r.transport.RequestRecipeInstallIssue(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.IssueRequest)
	} else {
		result, err = r.transport.RequestRecipeInstallObserve(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.ObserveRequest)
	}
	if err != nil {
		if recipeInstallCommandExpired(err) {
			return true, r.store.ExpireServiceOperationCommand(ctx, claim)
		}
		return true, r.store.DeferServiceOperation(ctx, claim, normalizedErrorCode(err.Error(), "service_operation_transport_failed"), now.Add(r.cfg.RetryDelay))
	}
	if ValidateRecipeInstallResult(claim, result, now) != nil {
		return true, r.store.DeferServiceOperation(ctx, claim, "service_operation_result_invalid", now.Add(r.cfg.RetryDelay))
	}
	return true, r.store.CommitServiceOperation(ctx, claim, result)
}

func (r *ServiceOperationRunner) now() time.Time {
	if r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
