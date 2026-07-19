package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RecipeInstallRunner struct {
	store     RecipeInstallStore
	transport RecipeInstallTransport
	artifact  RecipeArtifactEnsurer
	cfg       Config
}

func NewRecipeInstallRunnerWithArtifactTransfer(store RecipeInstallStore, transport RecipeInstallTransport, artifact RecipeArtifactEnsurer, cfg Config) *RecipeInstallRunner {
	return &RecipeInstallRunner{store: store, transport: transport, artifact: artifact, cfg: cfg}
}

func NewRecipeInstallRunner(store RecipeInstallStore, transport RecipeInstallTransport, cfg Config) *RecipeInstallRunner {
	return &RecipeInstallRunner{store: store, transport: transport, cfg: cfg}
}

func (r *RecipeInstallRunner) RunOnce(ctx context.Context) (bool, error) {
	if r == nil || r.store == nil || r.transport == nil {
		return false, errors.New("recipe install runner is unavailable")
	}
	if r.cfg.WorkerID == "" || r.cfg.Lease <= 0 || r.cfg.AttemptTimeout <= 0 || r.cfg.AttemptTimeout >= r.cfg.Lease || r.cfg.RetryDelay <= 0 {
		return false, errors.New("recipe install runner configuration is invalid")
	}
	claim, found, err := r.store.ClaimRecipeInstall(ctx, r.cfg.WorkerID, r.cfg.Lease)
	if err != nil || !found {
		return false, err
	}
	if ValidateRecipeInstallClaim(claim) != nil {
		return true, r.store.FailRecipeInstall(ctx, claim, "invalid_recipe_install_claim")
	}
	if err := r.store.MarkRecipeInstallStarted(ctx, claim); err != nil {
		return true, fmt.Errorf("mark recipe install started: %w", err)
	}
	signed := SignedRecipeInstallCommand{EnvelopeJSON: claim.Command.SignedEnvelope, PayloadJSON: claim.Command.PayloadJSON, PayloadSHA256: claim.Command.PayloadSHA256, RequestSHA256: claim.Command.RequestSHA256, IssuedAt: claim.Command.IssuedAt, ExpiresAt: claim.Command.ExpiresAt}
	if signed.EnvelopeJSON == "" {
		if claim.Phase == RecipeInstallPhaseIssue {
			signed, err = r.transport.BuildRecipeInstallIssueCommand(claim.Command, claim.IssueRequest, r.now())
		} else {
			signed, err = r.transport.BuildRecipeInstallObserveCommand(claim.Command, claim.ObserveRequest, r.now())
		}
		if err != nil {
			return true, r.store.FailRecipeInstall(ctx, claim, "invalid_recipe_install_command")
		}
		if err := r.store.PersistRecipeInstallCommand(ctx, claim, signed); err != nil {
			return true, fmt.Errorf("persist recipe install command: %w", err)
		}
	}
	if ValidateSignedRecipeInstallCommand(claim.Command, signed) != nil {
		return true, r.store.FailRecipeInstall(ctx, claim, "invalid_recipe_install_command")
	}
	attemptCtx, cancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
	defer cancel()
	var result RecipeInstallResult
	if claim.Phase == RecipeInstallPhaseIssue {
		result, err = r.transport.RequestRecipeInstallIssue(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.IssueRequest)
	} else {
		result, err = r.transport.RequestRecipeInstallObserve(attemptCtx, claim.BrokerEndpoint, claim.Command, signed, claim.ObserveRequest)
	}
	attemptErr := attemptCtx.Err()
	if ctx.Err() != nil {
		return true, ctx.Err()
	}
	if errors.Is(attemptErr, context.DeadlineExceeded) {
		return true, r.store.DeferRecipeInstall(ctx, claim, "recipe_install_attempt_timed_out", r.now().Add(r.cfg.RetryDelay))
	}
	if err != nil {
		if recipeInstallCommandExpired(err) {
			return true, r.store.ExpireRecipeInstallCommand(ctx, claim)
		}
		return true, r.store.DeferRecipeInstall(ctx, claim, "recipe_install_transport_failed", r.now().Add(r.cfg.RetryDelay))
	}
	if ValidateRecipeInstallResult(claim, result, r.now()) != nil {
		return true, r.store.DeferRecipeInstall(ctx, claim, "invalid_recipe_install_result", r.now().Add(r.cfg.RetryDelay))
	}
	if claim.Phase == RecipeInstallPhaseIssue && r.artifact != nil && result.Status != "failed" && result.Status != "interrupted" {
		// The accepted issue request and artifact handoff share one deadline so
		// the durable Recipe lease cannot expire between PUT and local commit.
		artifactErr := r.artifact.Ensure(attemptCtx, claim)
		artifactAttemptErr := attemptCtx.Err()
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		if recipeArtifactCommandExpired(artifactErr) {
			return true, r.store.FailRecipeInstall(ctx, claim, "recipe_artifact_command_expired")
		}
		if artifactErr != nil || errors.Is(artifactAttemptErr, context.DeadlineExceeded) {
			return true, r.store.DeferRecipeInstall(ctx, claim, "recipe_artifact_transfer_failed", r.now().Add(r.cfg.RetryDelay))
		}
	}
	return true, r.store.CommitRecipeInstall(ctx, claim, result)
}

func (r *RecipeInstallRunner) now() time.Time {
	if r.cfg.Now != nil {
		return r.cfg.Now().UTC()
	}
	return time.Now().UTC()
}
