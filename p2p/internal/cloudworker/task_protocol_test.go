package cloudworker

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWorkerTaskClaimAndEventAreBoundedToTheVerifiedDeployment(t *testing.T) {
	manifest := validTestManifest("https://broker.example.invalid/v2/worker-sessions")
	task := WorkerTask{
		Schema:                  WorkerTaskV1Schema,
		TaskID:                  "worker-task-v2-001",
		DeploymentID:            manifest.DeploymentID,
		TaskKind:                TaskKindExecutionProbe,
		ExecutionManifestDigest: testDigest,
		InputDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Attempt:                 1,
		LastSequence:            0,
	}
	if err := task.ValidateFor(manifest); err != nil {
		t.Fatalf("WorkerTask.ValidateFor() error = %v", err)
	}
	response, err := ParseTaskClaimResponse(mustMarshalTask(t, TaskClaimResponse{
		Schema:     WorkerTaskClaimResponseV1Schema,
		LeaseEpoch: 7,
		Status:     "claimed",
		Task:       &task,
	}), manifest, 7)
	if err != nil {
		t.Fatalf("ParseTaskClaimResponse() error = %v", err)
	}
	if response.Task == nil || response.Task.TaskKind != TaskKindExecutionProbe || response.LeaseEpoch != 7 {
		t.Fatalf("task claim response = %#v", response)
	}

	event := TaskEvent{
		Schema:         WorkerTaskEventV1Schema,
		TaskID:         task.TaskID,
		Attempt:        task.Attempt,
		LeaseEpoch:     7,
		Sequence:       1,
		Status:         TaskStatusRunning,
		Checkpoint:     taskString("execution_manifest_received"),
		EvidenceDigest: taskString(task.ExecutionManifestDigest),
		OccurredAt:     "2026-07-15T02:00:00.000Z",
	}
	if err := event.ValidateFor(task); err != nil {
		t.Fatalf("TaskEvent.ValidateFor() error = %v", err)
	}
	if err := (TaskEventReceipt{
		Schema:      WorkerTaskEventReceiptV1Schema,
		TaskID:      task.TaskID,
		Attempt:     task.Attempt,
		LeaseEpoch:  event.LeaseEpoch,
		Sequence:    event.Sequence,
		Disposition: "accepted",
	}).ValidateFor(event); err != nil {
		t.Fatalf("TaskEventReceipt.ValidateFor() error = %v", err)
	}
}

func TestWorkerTaskProtocolRejectsArbitraryExecutionMaterial(t *testing.T) {
	manifest := validTestManifest("https://broker.example.invalid/v2/worker-sessions")
	validTask := WorkerTask{
		Schema:                  WorkerTaskV1Schema,
		TaskID:                  "worker-task-v2-001",
		DeploymentID:            manifest.DeploymentID,
		TaskKind:                TaskKindExecutionProbe,
		ExecutionManifestDigest: testDigest,
		InputDigest:             "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Attempt:                 1,
	}
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "unknown task field",
			raw:  `{"schema":"dirextalk.worker-task/v1","task_id":"worker-task-v2-001","deployment_id":"deployment-v2-001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"last_sequence":0,"command":"curl https://example.invalid"}`,
		},
		{
			name: "unknown task kind",
			raw:  `{"schema":"dirextalk.worker-task/v1","task_id":"worker-task-v2-001","deployment_id":"deployment-v2-001","task_kind":"shell","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"last_sequence":0}`,
		},
		{
			name: "arbitrary event field",
			raw:  `{"schema":"dirextalk.worker-task-event/v1","task_id":"worker-task-v2-001","attempt":1,"lease_epoch":1,"sequence":1,"status":"running","checkpoint":"execution_manifest_received","evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurred_at":"2026-07-15T02:00:00.000Z","output":"secret"}`,
		},
		{
			name: "arbitrary transport checkpoint",
			raw:  `{"schema":"dirextalk.worker-task-event/v1","task_id":"worker-task-v2-001","attempt":1,"lease_epoch":1,"sequence":1,"status":"running","checkpoint":"probe_started","error_code":null,"evidence_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","occurred_at":"2026-07-15T02:00:00.000Z"}`,
		},
		{
			name: "success digest does not bind execution manifest",
			raw:  `{"schema":"dirextalk.worker-task-event/v1","task_id":"worker-task-v2-001","attempt":1,"lease_epoch":1,"sequence":1,"status":"succeeded","checkpoint":"task_transport_verified","evidence_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","occurred_at":"2026-07-15T02:00:00.000Z"}`,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if strings.Contains(test.raw, "worker-task/v1") {
				if _, err := ParseWorkerTask([]byte(test.raw), manifest); err == nil {
					t.Fatal("ParseWorkerTask() accepted unbounded task material")
				}
				return
			}
			event, err := ParseTaskEvent([]byte(test.raw))
			if err == nil {
				err = event.ValidateFor(validTask)
			}
			if err == nil {
				t.Fatal("TaskEvent accepted unbounded or mismatched progress material")
			}
		})
	}

	// Recipe execution has its own isolated task schema. The non-root
	// cloud-worker must continue to reject it even after the sealed Recipe
	// executor contract exists in a sibling package.
	if _, err := ParseWorkerTask([]byte(`{"schema":"dirextalk.recipe-execution-task/v1","task_id":"recipe-task-1","execution_id":"execution-worker-1","deployment_id":"deployment-v2-001","task_kind":"recipe_execution","recipe_execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","checkpoint_sequence":["artifact_verified"],"last_checkpoint":"","attempt":1,"last_sequence":0}`), manifest); err == nil {
		t.Fatal("ParseWorkerTask() accepted a sealed recipe executor task")
	}
}

func TestTaskClaimResponseRequiresCurrentLeaseAndTaskShape(t *testing.T) {
	manifest := validTestManifest("https://broker.example.invalid/v2/worker-sessions")
	response := `{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"none"}`
	if _, err := ParseTaskClaimResponse([]byte(response), manifest, 7); err != nil {
		t.Fatalf("ParseTaskClaimResponse(none) error = %v", err)
	}
	for _, raw := range []string{
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":6,"status":"none"}`,
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"claimed"}`,
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"claimed","task":{"schema":"dirextalk.worker-task/v1","task_id":"worker-task-v2-001","deployment_id":"deployment-v2-001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1}}`,
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"claimed","task":{"schema":"dirextalk.worker-task/v1","task_id":"worker-task-v2-001","deployment_id":"deployment-v2-001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"last_sequence":null}}`,
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"claimed","task":{"schema":"dirextalk.worker-task/v1","task_id":"worker-task-v2-001","deployment_id":"deployment-v2-001","task_kind":"execution_probe","execution_manifest_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","input_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","attempt":1,"last_sequence":9007199254740992}}`,
		`{"schema":"dirextalk.worker-task-claim-response/v1","lease_epoch":7,"status":"none","task":null,"token":"not-allowed"}`,
	} {
		if _, err := ParseTaskClaimResponse([]byte(raw), manifest, 7); err == nil {
			t.Fatalf("ParseTaskClaimResponse() accepted %s", raw)
		}
	}
}

func mustMarshalTask(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal task protocol: %v", err)
	}
	return encoded
}
