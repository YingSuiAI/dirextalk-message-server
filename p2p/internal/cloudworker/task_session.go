package cloudworker

import (
	"context"
	"errors"
	"math"
	"strconv"
)

type pendingTaskEvent struct {
	task  WorkerTask
	event TaskEvent
}

// ClaimTask pulls at most one closed, digest-only task under the active
// Worker lease. A task can only be reported after this client has claimed it;
// callers cannot fabricate another task descriptor and turn the transport
// into an arbitrary execution channel.
func (client *SessionClient) ClaimTask(ctx context.Context) (WorkerTask, bool, error) {
	if client == nil {
		return WorkerTask{}, false, ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" {
		return WorkerTask{}, false, ErrSessionNotClaimed
	}
	if client.pendingTask != nil {
		return WorkerTask{}, false, ErrPendingTaskEvent
	}
	request, err := NewTaskClaimRequest(client.epoch)
	if err != nil {
		return WorkerTask{}, false, err
	}
	body, err := client.post(ctx, "tasks/claim", request, client.access)
	if err != nil {
		return WorkerTask{}, false, err
	}
	response, err := ParseTaskClaimResponse(body, client.manifest, client.epoch)
	if err != nil {
		return WorkerTask{}, false, err
	}
	if response.Task == nil {
		client.claimedTask = nil
		return WorkerTask{}, false, nil
	}
	task := *response.Task
	if client.taskLastAck == nil {
		client.taskLastAck = map[string]uint64{}
	}
	client.taskLastAck[taskProgressKey(task)] = task.LastSequence
	client.claimedTask = &task
	return task, true, nil
}

// ReportTask emits one bounded task state transition. The first-stage worker
// uses this only for execution_probe transport evidence; it does not execute
// a Recipe, shell command, container, URL, or cloud control action.
func (client *SessionClient) ReportTask(ctx context.Context, task WorkerTask, status TaskStatus, checkpoint, errorCode, evidenceDigest string) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" {
		return ErrSessionNotClaimed
	}
	if client.pendingTask != nil {
		return ErrPendingTaskEvent
	}
	if client.claimedTask == nil || !sameWorkerTask(*client.claimedTask, task) {
		return ErrTaskNotClaimed
	}
	if err := task.ValidateFor(client.manifest); err != nil {
		return err
	}
	key := taskProgressKey(task)
	lastSequence, known := client.taskLastAck[key]
	if !known {
		lastSequence = task.LastSequence
		if client.taskLastAck == nil {
			client.taskLastAck = map[string]uint64{}
		}
		client.taskLastAck[key] = lastSequence
	}
	if lastSequence == math.MaxUint64 {
		return errors.New("worker task sequence is exhausted")
	}
	event := TaskEvent{
		Schema:         WorkerTaskEventV1Schema,
		TaskID:         task.TaskID,
		Attempt:        task.Attempt,
		LeaseEpoch:     client.epoch,
		Sequence:       lastSequence + 1,
		Status:         status,
		Checkpoint:     optionalTaskString(checkpoint),
		ErrorCode:      optionalTaskString(errorCode),
		EvidenceDigest: optionalTaskString(evidenceDigest),
		OccurredAt:     canonicalInstant(client.now()),
	}
	if err := event.ValidateFor(task); err != nil {
		return err
	}
	client.pendingTask = &pendingTaskEvent{task: task, event: event}
	return client.sendPendingTask(ctx)
}

// RetryPendingTask preserves the original epoch, sequence, timestamp and
// nullable fields, making a response loss safe to retry exactly once per
// durable task-event identity.
func (client *SessionClient) RetryPendingTask(ctx context.Context) error {
	if client == nil {
		return ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" {
		return ErrSessionNotClaimed
	}
	if client.pendingTask == nil {
		return ErrNoPendingTaskEvent
	}
	return client.sendPendingTask(ctx)
}

func (client *SessionClient) sendPendingTask(ctx context.Context) error {
	if client.pendingTask == nil || client.access == "" {
		return ErrSessionNotClaimed
	}
	pending := client.pendingTask
	body, err := client.post(ctx, "tasks/"+pending.task.TaskID+"/events", pending.event, client.access)
	if err != nil {
		return err
	}
	var receipt TaskEventReceipt
	if err := decodeStrictObject(body, &receipt); err != nil || receipt.ValidateFor(pending.event) != nil {
		return errors.New("worker task event receipt is invalid")
	}
	if client.taskLastAck == nil {
		client.taskLastAck = map[string]uint64{}
	}
	client.taskLastAck[taskProgressKey(pending.task)] = pending.event.Sequence
	client.pendingTask = nil
	if pending.event.Status == TaskStatusSucceeded || pending.event.Status == TaskStatusFailed || pending.event.Status == TaskStatusInterrupted {
		client.claimedTask = nil
	}
	return nil
}

func optionalTaskString(value string) *string {
	if value == "" {
		return nil
	}
	return taskString(value)
}

func taskProgressKey(task WorkerTask) string {
	return task.TaskID + ":" + strconv.FormatUint(task.Attempt, 10)
}

func sameWorkerTask(left, right WorkerTask) bool {
	return left == right
}
