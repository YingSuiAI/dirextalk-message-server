package contract

import "encoding/json"

const (
	WorkerHeartbeatEventSchema        = "dirextalk.worker-event/v1"
	WorkerHeartbeatEventReceiptSchema = "dirextalk.worker-event-receipt/v1"
)

// WorkerHeartbeatEvent is the only session event enabled in this stage. Its
// exact shape cannot express checkpoints, reports, logs, output, or secrets.
type WorkerHeartbeatEvent struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	DeploymentID       string `json:"deployment_id"`
	BootstrapSessionID string `json:"bootstrap_session_id"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	Sequence           uint64 `json:"sequence"`
	Kind               string `json:"kind"`
	OccurredAt         string `json:"occurred_at"`
}

type WorkerHeartbeatEventReceipt struct {
	Schema             string `json:"schema"`
	ConnectionID       string `json:"connection_id"`
	DeploymentID       string `json:"deployment_id"`
	BootstrapSessionID string `json:"bootstrap_session_id"`
	LeaseEpoch         uint64 `json:"lease_epoch"`
	Sequence           uint64 `json:"sequence"`
	Disposition        string `json:"disposition"`
}

func ParseWorkerHeartbeatEvent(raw []byte) (WorkerHeartbeatEvent, error) {
	var event WorkerHeartbeatEvent
	fields := []string{"schema", "connection_id", "deployment_id", "bootstrap_session_id", "lease_epoch", "sequence", "kind", "occurred_at"}
	if !strictWorkerTaskObject(raw, fields, &event) || event.Validate() != nil {
		return WorkerHeartbeatEvent{}, errCode("invalid_worker_heartbeat")
	}
	return event, nil
}

func (e WorkerHeartbeatEvent) Validate() error {
	if e.Schema != WorkerHeartbeatEventSchema || !ValidConnectionID(e.ConnectionID) || !ValidID(e.DeploymentID) ||
		!ValidID(e.BootstrapSessionID) || !workerTaskPositive(e.LeaseEpoch) || !workerTaskPositive(e.Sequence) ||
		e.Kind != "heartbeat" || !canonicalTaskInstant(e.OccurredAt) {
		return errCode("invalid_worker_heartbeat")
	}
	return nil
}

func MarshalWorkerHeartbeatEventReceipt(event WorkerHeartbeatEvent, idempotent bool) ([]byte, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	disposition := "accepted"
	if idempotent {
		disposition = "idempotent"
	}
	receipt := WorkerHeartbeatEventReceipt{
		Schema: WorkerHeartbeatEventReceiptSchema, ConnectionID: event.ConnectionID,
		DeploymentID: event.DeploymentID, BootstrapSessionID: event.BootstrapSessionID,
		LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: disposition,
	}
	return json.Marshal(receipt)
}
