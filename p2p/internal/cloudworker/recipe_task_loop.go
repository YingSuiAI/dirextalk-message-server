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
	if executeErr != nil {
		return loop.transport.Report(ctx, claimed, recipeexec.TaskStatusFailed, "", "recipe_execution_failed", "")
	}
	if !result.Completed || result.ManifestDigest != claimed.Task.RecipeExecutionManifestDigest {
		return errors.New("recipe execution did not complete its sealed checkpoints")
	}
	start := recipeCheckpointIndex(claimed.Task.CheckpointSequence, claimed.Task.LastCheckpoint) + 1
	end := recipeCheckpointIndex(claimed.Task.CheckpointSequence, result.LastCheckpoint)
	if start < 0 || end < start || end >= len(claimed.Task.CheckpointSequence) {
		return errors.New("recipe execution checkpoint result is invalid")
	}
	for index := start; index <= end; index++ {
		status := recipeexec.TaskStatusRunning
		if index == len(claimed.Task.CheckpointSequence)-1 {
			status = recipeexec.TaskStatusSucceeded
		}
		if err := loop.transport.Report(ctx, claimed, status, claimed.Task.CheckpointSequence[index], "", claimed.Task.RecipeExecutionManifestDigest); err != nil {
			return err
		}
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
