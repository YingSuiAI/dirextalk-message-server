package cloudworker

import (
	"context"
	"errors"
	"math"
	"sync"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

var (
	ErrRecipeTaskNotClaimed   = errors.New("recipe task is not claimed")
	ErrPendingRecipeTaskEvent = errors.New("recipe task has a pending event")
	ErrNoPendingRecipeEvent   = errors.New("recipe task has no pending event")
)

type ClaimedRecipeTask struct {
	Task     recipeexec.TaskV1
	Manifest cloudorchestrator.RecipeExecutionManifestV1
	Epoch    uint64
}

type pendingRecipeTaskEvent struct {
	claimed ClaimedRecipeTask
	event   recipeexec.EventV1
}

// RecipeTaskClient is a separate, closed transport derived from an active
// Worker session. It never exposes the bearer token and supports only the
// Recipe task claim/event routes.
type RecipeTaskClient struct {
	session *SessionClient

	mu       sync.Mutex
	claimed  *ClaimedRecipeTask
	progress recipeexec.Progress
	pending  *pendingRecipeTaskEvent
}

func (client *SessionClient) NewRecipeTaskClient() (*RecipeTaskClient, error) {
	if client == nil {
		return nil, ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" || client.epoch == 0 {
		return nil, ErrSessionNotClaimed
	}
	return &RecipeTaskClient{session: client}, nil
}

func (client *RecipeTaskClient) Claim(ctx context.Context) (ClaimedRecipeTask, bool, error) {
	if client == nil || client.session == nil {
		return ClaimedRecipeTask{}, false, ErrRecipeTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending != nil {
		return ClaimedRecipeTask{}, false, ErrPendingRecipeTaskEvent
	}
	_, token, epoch, err := client.session.recipeTaskAuthorization()
	if err != nil {
		return ClaimedRecipeTask{}, false, err
	}
	request, err := recipeexec.NewTaskClaimRequestV1(epoch)
	if err != nil {
		return ClaimedRecipeTask{}, false, ErrRecipeTaskNotClaimed
	}
	body, err := client.session.post(ctx, "recipe-tasks/claim", request, token)
	if err != nil {
		return ClaimedRecipeTask{}, false, err
	}
	response, err := recipeexec.ParseTaskClaimResponseV1(body, epoch)
	if err != nil {
		return ClaimedRecipeTask{}, false, errors.New("recipe task claim response is invalid")
	}
	if response.Task == nil || response.Manifest == nil {
		client.claimed = nil
		return ClaimedRecipeTask{}, false, nil
	}
	bootstrap, _, currentEpoch, err := client.session.recipeTaskAuthorization()
	if err != nil || currentEpoch != epoch || response.Task.DeploymentID != bootstrap.DeploymentID ||
		response.Manifest.DeploymentID != bootstrap.DeploymentID || response.Manifest.WorkerResourceManifestDigest != bootstrap.WorkerImageDigest {
		return ClaimedRecipeTask{}, false, errors.New("recipe task claim response is invalid")
	}
	progress, err := recipeexec.NewProgress(*response.Task)
	if err != nil {
		return ClaimedRecipeTask{}, false, errors.New("recipe task claim response is invalid")
	}
	claimed := ClaimedRecipeTask{Task: *response.Task, Manifest: *response.Manifest, Epoch: epoch}
	client.claimed, client.progress = &claimed, progress
	return claimed, true, nil
}

func (client *RecipeTaskClient) Report(ctx context.Context, claimed ClaimedRecipeTask, status recipeexec.TaskStatus, checkpoint, errorCode, evidenceDigest string) error {
	if client == nil || client.session == nil {
		return ErrRecipeTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending != nil {
		return ErrPendingRecipeTaskEvent
	}
	if client.claimed == nil || !sameClaimedRecipeTask(*client.claimed, claimed) || client.progress.LastSequence == math.MaxUint64 {
		return ErrRecipeTaskNotClaimed
	}
	_, _, epoch, err := client.session.recipeTaskAuthorization()
	if err != nil || epoch != claimed.Epoch {
		return ErrRecipeTaskNotClaimed
	}
	event := recipeexec.EventV1{
		Schema:         recipeexec.EventV1Schema,
		TaskID:         claimed.Task.TaskID,
		Attempt:        claimed.Task.Attempt,
		LeaseEpoch:     claimed.Epoch,
		Sequence:       client.progress.LastSequence + 1,
		Status:         status,
		Checkpoint:     optionalRecipeString(checkpoint),
		ErrorCode:      optionalRecipeString(errorCode),
		EvidenceDigest: optionalRecipeString(evidenceDigest),
		OccurredAt:     canonicalInstant(client.session.now()),
	}
	next, err := client.progress.Advance(event, claimed.Epoch)
	if err != nil {
		return err
	}
	client.pending = &pendingRecipeTaskEvent{claimed: claimed, event: event}
	if err := client.sendPending(ctx); err != nil {
		return err
	}
	client.progress = next
	if next.Terminal {
		client.claimed = nil
	}
	return nil
}

func (client *RecipeTaskClient) RetryPending(ctx context.Context) error {
	if client == nil || client.session == nil {
		return ErrRecipeTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending == nil {
		return ErrNoPendingRecipeEvent
	}
	_, _, epoch, err := client.session.recipeTaskAuthorization()
	if err != nil || epoch != client.pending.event.LeaseEpoch {
		return ErrRecipeTaskNotClaimed
	}
	next, err := client.progress.Advance(client.pending.event, epoch)
	if err != nil {
		return err
	}
	if err := client.sendPending(ctx); err != nil {
		return err
	}
	client.progress = next
	if next.Terminal {
		client.claimed = nil
	}
	return nil
}

func (client *RecipeTaskClient) sendPending(ctx context.Context) error {
	pending := client.pending
	if pending == nil {
		return ErrNoPendingRecipeEvent
	}
	_, token, epoch, err := client.session.recipeTaskAuthorization()
	if err != nil || epoch != pending.event.LeaseEpoch {
		return ErrRecipeTaskNotClaimed
	}
	body, err := client.session.post(ctx, "recipe-tasks/"+pending.event.TaskID+"/events", pending.event, token)
	if err != nil {
		return err
	}
	if _, err := recipeexec.ParseEventReceiptV1(body, pending.event); err != nil {
		return err
	}
	client.pending = nil
	return nil
}

func (client *SessionClient) recipeTaskAuthorization() (BootstrapManifest, string, uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" || client.epoch == 0 {
		return BootstrapManifest{}, "", 0, ErrSessionNotClaimed
	}
	return client.manifest, client.access, client.epoch, nil
}

func sameClaimedRecipeTask(left, right ClaimedRecipeTask) bool {
	return left.Epoch == right.Epoch && left.Task.Equal(right.Task) && left.Manifest.ExecutionID == right.Manifest.ExecutionID &&
		left.Manifest.DeploymentID == right.Manifest.DeploymentID && left.Task.RecipeExecutionManifestDigest == right.Task.RecipeExecutionManifestDigest
}

func optionalRecipeString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
