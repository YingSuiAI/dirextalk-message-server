package cloudworker

import (
	"context"
	"errors"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

type recipeTaskTransport interface {
	Claim(context.Context) (ClaimedRecipeTask, bool, error)
	Report(context.Context, ClaimedRecipeTask, recipeexec.TaskStatus, string, string, string) error
	RetryPending(context.Context) error
}

type recipeTaskExecutor interface {
	ExecuteTask(context.Context, recipeexec.TaskV1, cloudorchestrator.RecipeExecutionManifestV1) (recipeexec.Result, error)
}

// RecipeTaskLoop is enabled only when both a closed transport and a fully
// configured trusted executor are explicitly injected. A nil catalog, store,
// or driver therefore prevents claiming work rather than failing after claim.
type RecipeTaskLoop struct {
	transport recipeTaskTransport
	executor  recipeTaskExecutor
}

func NewRecipeTaskLoop(transport *RecipeTaskClient, resolver *recipeexec.FixedBundleResolver, store recipeexec.CheckpointStore, driver recipeexec.ActionDriver) (*RecipeTaskLoop, error) {
	executor := recipeexec.Executor{Resolver: resolver, Store: store, Driver: driver}
	if transport == nil || resolver == nil || !executor.Configured() {
		return nil, recipeexec.ErrExecutorConfiguration
	}
	return &RecipeTaskLoop{transport: transport, executor: executor}, nil
}

// ProcessOne consumes at most one Recipe task. It reports only declared
// checkpoint codes and the sealed manifest digest; execution errors collapse
// to a fixed safe code and never enter transport payloads or logs.
func (loop *RecipeTaskLoop) ProcessOne(ctx context.Context) error {
	if loop == nil || loop.transport == nil || loop.executor == nil {
		return recipeexec.ErrExecutorConfiguration
	}
	if err := loop.transport.RetryPending(ctx); err != nil && !errors.Is(err, ErrNoPendingRecipeEvent) {
		return err
	}
	claimed, found, err := loop.transport.Claim(ctx)
	if err != nil || !found {
		return err
	}
	result, executeErr := loop.executor.ExecuteTask(ctx, claimed.Task, claimed.Manifest)
	if result.ManifestDigest != "" && result.ManifestDigest != claimed.Task.RecipeExecutionManifestDigest {
		return errors.New("recipe execution did not complete its sealed checkpoints")
	}
	remoteIndex := recipeCheckpointIndex(claimed.Task.CheckpointSequence, claimed.Task.LastCheckpoint)
	start := remoteIndex + 1
	end := recipeCheckpointIndex(claimed.Task.CheckpointSequence, result.LastCheckpoint)
	if start < 0 || end >= len(claimed.Task.CheckpointSequence) || (result.LastCheckpoint != "" && end < remoteIndex) {
		return errors.New("recipe execution checkpoint result is invalid")
	}
	for index := start; result.LastCheckpoint != "" && index <= end; index++ {
		status := recipeexec.TaskStatusRunning
		if executeErr == nil && result.Completed && index == len(claimed.Task.CheckpointSequence)-1 {
			status = recipeexec.TaskStatusSucceeded
		}
		if err := loop.transport.Report(ctx, claimed, status, claimed.Task.CheckpointSequence[index], "", claimed.Task.RecipeExecutionManifestDigest); err != nil {
			return err
		}
	}
	if executeErr != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if recipeexec.IsPermanentExecutionFailure(executeErr) {
			return loop.transport.Report(ctx, claimed, recipeexec.TaskStatusFailed, "", "recipe_execution_failed", "")
		}
		return executeErr
	}
	if !result.Completed {
		return errors.New("recipe execution did not complete its sealed checkpoints")
	}
	return nil
}

func recipeCheckpointIndex(checkpoints []string, checkpoint string) int {
	if checkpoint == "" {
		return -1
	}
	for index, candidate := range checkpoints {
		if candidate == checkpoint {
			return index
		}
	}
	return -1
}
