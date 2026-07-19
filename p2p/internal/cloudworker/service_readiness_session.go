package cloudworker

import (
	"context"
	"errors"
	"math"
	"sync"
)

var (
	ErrServiceReadinessTaskNotClaimed = errors.New("service readiness task is not claimed")
	ErrPendingServiceReadinessEvent   = errors.New("service readiness task has a pending event")
	ErrNoPendingServiceReadinessEvent = errors.New("service readiness task has no pending event")
)

type ClaimedServiceReadinessTask struct {
	Task      ServiceReadinessTaskV1
	Challenge ServiceReadinessChallengeV1
	Epoch     uint64
}

type ServiceReadinessTaskClient struct {
	session *SessionClient

	mu      sync.Mutex
	claimed *ClaimedServiceReadinessTask
	pending *ServiceReadinessTaskEventV1
}

func (client *SessionClient) NewServiceReadinessTaskClient() (*ServiceReadinessTaskClient, error) {
	if client == nil {
		return nil, ErrSessionNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" || client.epoch == 0 {
		return nil, ErrSessionNotClaimed
	}
	return &ServiceReadinessTaskClient{session: client}, nil
}

func (client *ServiceReadinessTaskClient) Claim(ctx context.Context) (ClaimedServiceReadinessTask, bool, error) {
	if client == nil || client.session == nil {
		return ClaimedServiceReadinessTask{}, false, ErrServiceReadinessTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending != nil {
		return ClaimedServiceReadinessTask{}, false, ErrPendingServiceReadinessEvent
	}
	manifest, token, epoch, err := client.session.serviceReadinessAuthorization()
	if err != nil {
		return ClaimedServiceReadinessTask{}, false, err
	}
	request, err := NewServiceReadinessTaskClaimRequestV1(epoch)
	if err != nil {
		return ClaimedServiceReadinessTask{}, false, err
	}
	body, err := client.session.post(ctx, "service-readiness-tasks/claim", request, token)
	if err != nil {
		return ClaimedServiceReadinessTask{}, false, err
	}
	response, err := ParseServiceReadinessTaskClaimResponseV1(body, manifest, epoch, client.session.now().UTC())
	if err != nil {
		return ClaimedServiceReadinessTask{}, false, err
	}
	if response.Task == nil || response.Challenge == nil {
		client.claimed = nil
		return ClaimedServiceReadinessTask{}, false, nil
	}
	_, _, currentEpoch, err := client.session.serviceReadinessAuthorization()
	if err != nil || currentEpoch != epoch {
		return ClaimedServiceReadinessTask{}, false, ErrServiceReadinessTaskNotClaimed
	}
	claimed := ClaimedServiceReadinessTask{Task: *response.Task, Challenge: *response.Challenge, Epoch: epoch}
	client.claimed = &claimed
	return claimed, true, nil
}

func (client *ServiceReadinessTaskClient) Report(ctx context.Context, claimed ClaimedServiceReadinessTask, status ServiceReadinessTaskStatus, errorCode string) error {
	if client == nil || client.session == nil {
		return ErrServiceReadinessTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending != nil {
		return ErrPendingServiceReadinessEvent
	}
	if client.claimed == nil || *client.claimed != claimed || claimed.Task.LastSequence == math.MaxUint64 {
		return ErrServiceReadinessTaskNotClaimed
	}
	_, _, epoch, err := client.session.serviceReadinessAuthorization()
	if err != nil || epoch != claimed.Epoch || claimed.Challenge.validate(client.session.now().UTC()) != nil {
		return ErrServiceReadinessTaskNotClaimed
	}
	event := ServiceReadinessTaskEventV1{
		Schema:     ServiceReadinessTaskEventV1Schema,
		TaskID:     claimed.Task.TaskID,
		Attempt:    claimed.Task.Attempt,
		LeaseEpoch: claimed.Epoch,
		Sequence:   claimed.Task.LastSequence + 1,
		Status:     status,
		ErrorCode:  optionalReadinessString(errorCode),
		OccurredAt: canonicalInstant(client.session.now()),
	}
	if status == ServiceReadinessTaskSucceeded {
		event.ChallengeDigest = optionalReadinessString(claimed.Challenge.ChallengeDigest)
		event.SemanticEvidenceDigest = optionalReadinessString(claimed.Task.SemanticProbe.BodySHA256)
	}
	if err := event.validate(claimed.Task, claimed.Challenge, claimed.Epoch); err != nil {
		return err
	}
	client.pending = &event
	return client.sendPending(ctx)
}

func (client *ServiceReadinessTaskClient) RetryPending(ctx context.Context) error {
	if client == nil || client.session == nil {
		return ErrServiceReadinessTaskNotClaimed
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.pending == nil {
		return ErrNoPendingServiceReadinessEvent
	}
	_, _, epoch, err := client.session.serviceReadinessAuthorization()
	if err != nil {
		return ErrServiceReadinessTaskNotClaimed
	}
	if epoch != client.pending.LeaseEpoch {
		// The Stack fences the old event when the Worker session rotates. Drop
		// only this process-local pending copy so the next claim can obtain a
		// fresh challenge and incremented attempt under the new lease.
		client.pending = nil
		client.claimed = nil
		return nil
	}
	return client.sendPending(ctx)
}

func (client *ServiceReadinessTaskClient) sendPending(ctx context.Context) error {
	if client.pending == nil {
		return ErrNoPendingServiceReadinessEvent
	}
	_, token, epoch, err := client.session.serviceReadinessAuthorization()
	if err != nil || epoch != client.pending.LeaseEpoch {
		return ErrServiceReadinessTaskNotClaimed
	}
	body, err := client.session.post(ctx, "service-readiness-tasks/"+client.pending.TaskID+"/events", *client.pending, token)
	if err != nil {
		return err
	}
	if _, err := ParseServiceReadinessTaskEventReceiptV1(body, *client.pending); err != nil {
		return err
	}
	client.pending = nil
	client.claimed = nil
	return nil
}

func (client *SessionClient) serviceReadinessAuthorization() (BootstrapManifest, string, uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.state != SessionStateActive || client.access == "" || client.epoch == 0 {
		return BootstrapManifest{}, "", 0, ErrSessionNotClaimed
	}
	return client.manifest, client.access, client.epoch, nil
}

func optionalReadinessString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

type serviceReadinessTransport interface {
	Claim(context.Context) (ClaimedServiceReadinessTask, bool, error)
	Report(context.Context, ClaimedServiceReadinessTask, ServiceReadinessTaskStatus, string) error
	RetryPending(context.Context) error
}

type ServiceReadinessProbe interface {
	CheckLoopback(context.Context, ServiceReadinessProbeV1) error
}

type ServiceReadinessTaskLoop struct {
	transport serviceReadinessTransport
	probe     ServiceReadinessProbe
}

func NewServiceReadinessTaskLoop(transport *ServiceReadinessTaskClient, probe ServiceReadinessProbe) (*ServiceReadinessTaskLoop, error) {
	if transport == nil || probe == nil {
		return nil, ErrServiceReadinessTaskNotClaimed
	}
	return &ServiceReadinessTaskLoop{transport: transport, probe: probe}, nil
}

func (loop *ServiceReadinessTaskLoop) ProcessOne(ctx context.Context) error {
	if loop == nil || loop.transport == nil || loop.probe == nil {
		return ErrServiceReadinessTaskNotClaimed
	}
	if err := loop.transport.RetryPending(ctx); err != nil && !errors.Is(err, ErrNoPendingServiceReadinessEvent) {
		return err
	}
	claimed, found, err := loop.transport.Claim(ctx)
	if err != nil || !found {
		return err
	}
	if err := loop.probe.CheckLoopback(ctx, claimed.Task.SemanticProbe); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return loop.transport.Report(ctx, claimed, ServiceReadinessTaskInterrupted, "fixed_probe_interrupted")
		}
		return loop.transport.Report(ctx, claimed, ServiceReadinessTaskFailed, "semantic_probe_not_ready")
	}
	return loop.transport.Report(ctx, claimed, ServiceReadinessTaskSucceeded, "")
}
