package cloudworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

func TestRecipeTaskLoopUsesOnlyBoundBearerRoutesAndSealedExecutorInputs(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	accessToken := "short-lived-recipe-worker-token"
	manifest := testRecipeExecutionManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatalf("manifest.Digest() error = %v", err)
	}
	task := recipeexec.TaskV1{
		Schema:                        recipeexec.TaskV1Schema,
		TaskID:                        "recipe-task-0001",
		ExecutionID:                   manifest.ExecutionID,
		DeploymentID:                  manifest.DeploymentID,
		TaskKind:                      recipeexec.TaskKindRecipeExecution,
		RecipeExecutionManifestDigest: manifestDigest,
		InputDigest:                   recipeDigest('e'),
		CheckpointSequence:            append([]string(nil), manifest.CheckpointSequence...),
		Attempt:                       1,
	}
	var (
		mu         sync.Mutex
		claimCalls int
		events     []recipeexec.EventV1
	)
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer "+accessToken {
			http.Error(writer, "rejected", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/recipe-tasks/claim":
			var claim recipeexec.TaskClaimRequestV1
			if err := json.NewDecoder(request.Body).Decode(&claim); err != nil || claim.Schema != recipeexec.TaskClaimV1Schema || claim.LeaseEpoch != 7 {
				http.Error(writer, "claim", http.StatusBadRequest)
				return
			}
			mu.Lock()
			claimCalls++
			mu.Unlock()
			writeWorkerJSON(t, writer, http.StatusOK, recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest})
		case "/v2/worker-sessions/worker-session-v2-01/recipe-tasks/recipe-task-0001/events":
			raw := mustReadAll(t, request.Body)
			event, err := recipeexec.ParseEventV1(raw)
			if err != nil {
				http.Error(writer, "event", http.StatusBadRequest)
				return
			}
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
			writeWorkerJSON(t, writer, http.StatusOK, recipeexec.EventReceiptV1{Schema: recipeexec.EventReceiptV1Schema, TaskID: event.TaskID, Attempt: event.Attempt, LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: "accepted"})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()

	endpoint, err := url.Parse(server.URL + "/v2/worker-sessions")
	if err != nil {
		t.Fatalf("url.Parse() error = %v", err)
	}
	bootstrap := validTestManifest(endpoint.String())
	bootstrap.WorkerImageDigest = manifest.WorkerResourceManifestDigest
	session := &SessionClient{manifest: bootstrap, endpoint: endpoint, client: server.Client(), now: func() time.Time { return now }, state: SessionStateActive, access: accessToken, epoch: 7, leaseExpiresAt: now.Add(5 * time.Minute)}
	transport, err := session.NewRecipeTaskClient()
	if err != nil {
		t.Fatalf("NewRecipeTaskClient() error = %v", err)
	}
	resolver, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{{ArtifactDigest: manifest.ArtifactDigest, ActionIDs: []string{manifest.ActionID}}})
	if err != nil {
		t.Fatalf("NewFixedBundleResolver() error = %v", err)
	}
	store := &recipeMemoryCheckpointStore{state: recipeexec.InitialCheckpointState(recipeexec.Binding{ExecutionID: manifest.ExecutionID, ManifestDigest: manifestDigest})}
	driver := &recipeCheckpointDriver{run: func(ctx context.Context, request recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
		if request.ActionID != manifest.ActionID || request.Artifact.ArtifactDigest != manifest.ArtifactDigest || request.DeploymentID != manifest.DeploymentID {
			t.Fatalf("driver received unbound request: %#v", request)
		}
		for _, checkpoint := range manifest.CheckpointSequence {
			if err := reporter.Checkpoint(ctx, checkpoint); err != nil {
				return err
			}
		}
		return nil
	}}
	loop, err := NewRecipeTaskLoop(transport, resolver, store, driver)
	if err != nil {
		t.Fatalf("NewRecipeTaskLoop() error = %v", err)
	}
	if err := loop.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if claimCalls != 1 || driver.calls != 1 || len(events) != len(manifest.CheckpointSequence) {
		t.Fatalf("recipe flow = claims:%d driver:%d events:%#v", claimCalls, driver.calls, events)
	}
	for index, event := range events {
		if event.Sequence != uint64(index+1) || event.Checkpoint == nil || *event.Checkpoint != manifest.CheckpointSequence[index] || event.EvidenceDigest == nil || *event.EvidenceDigest != manifestDigest || event.ErrorCode != nil {
			t.Fatalf("event[%d] = %#v", index, event)
		}
	}
	if events[len(events)-1].Status != recipeexec.TaskStatusSucceeded {
		t.Fatalf("terminal event = %#v", events[len(events)-1])
	}
}

func TestRecipeTaskLoopPreservesRetryableCheckpointProgress(t *testing.T) {
	manifest := testRecipeExecutionManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-retry-0001", ExecutionID: manifest.ExecutionID,
		DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest,
		InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	transient := errors.New("probe is starting")
	transport := &loopRecipeTransport{claimed: ClaimedRecipeTask{Task: task, Manifest: manifest, Epoch: 1}}
	loop := &RecipeTaskLoop{transport: transport, executor: loopRecipeExecutor{result: recipeexec.Result{ExecutionID: manifest.ExecutionID,
		ManifestDigest: digest, LastCheckpoint: manifest.CheckpointSequence[1]}, err: transient}}
	if err := loop.ProcessOne(context.Background()); !errors.Is(err, transient) {
		t.Fatalf("ProcessOne() error=%v, want transient error", err)
	}
	if len(transport.reports) != 2 || transport.reports[0].status != recipeexec.TaskStatusRunning ||
		transport.reports[0].checkpoint != manifest.CheckpointSequence[0] || transport.reports[1].checkpoint != manifest.CheckpointSequence[1] {
		t.Fatalf("retryable reports=%#v", transport.reports)
	}
	for _, report := range transport.reports {
		if report.status == recipeexec.TaskStatusFailed {
			t.Fatalf("transient failure was made terminal: %#v", transport.reports)
		}
	}

	transport = &loopRecipeTransport{claimed: ClaimedRecipeTask{Task: task, Manifest: manifest, Epoch: 1}}
	loop = &RecipeTaskLoop{transport: transport, executor: loopRecipeExecutor{err: recipeexec.PermanentExecutionFailure(errors.New("scope rejected"))}}
	if err := loop.ProcessOne(context.Background()); err != nil {
		t.Fatalf("permanent ProcessOne() error=%v", err)
	}
	if len(transport.reports) != 1 || transport.reports[0].status != recipeexec.TaskStatusFailed || transport.reports[0].errorCode != "recipe_execution_failed" {
		t.Fatalf("permanent reports=%#v", transport.reports)
	}
}

func TestRecipeTaskLoopPreparesDynamicArtifactBeforeExecutorLookup(t *testing.T) {
	manifest := testRecipeExecutionManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-prepare-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	access := &recipeexec.ArtifactAccessV1{Method: http.MethodGet, URL: "https://artifacts.example.invalid/archive?versionId=version-0001&temporary=secret", ExpiresAt: "2026-07-16T10:10:00.000Z", VersionID: "version-0001", MediaType: recipeexec.RecipeArtifactMediaTypeV1, SizeBytes: 1024, ArchiveSHA256: strings.Repeat("a", 64)}
	prepared := false
	transport := &loopRecipeTransport{claimed: ClaimedRecipeTask{Task: task, Manifest: manifest, ArtifactAccess: access, Epoch: 1}}
	loop := &RecipeTaskLoop{transport: transport, preparer: loopArtifactPreparer{prepared: &prepared}, executor: orderedRecipeExecutor{t: t, prepared: &prepared, result: recipeexec.Result{ExecutionID: manifest.ExecutionID, ManifestDigest: digest, LastCheckpoint: task.CheckpointSequence[len(task.CheckpointSequence)-1], Completed: true}}}
	if err := loop.ProcessOne(context.Background()); err != nil {
		t.Fatalf("ProcessOne() error = %v", err)
	}
	if !prepared || len(transport.reports) != len(task.CheckpointSequence) {
		t.Fatalf("dynamic preparation=%v reports=%#v", prepared, transport.reports)
	}
}

type loopArtifactPreparer struct{ prepared *bool }

func (preparer loopArtifactPreparer) Prepare(_ context.Context, claimed ClaimedRecipeTask) error {
	if claimed.ArtifactAccess == nil {
		return ErrRecipeArtifactUnavailable
	}
	*preparer.prepared = true
	return nil
}

type orderedRecipeExecutor struct {
	t        *testing.T
	prepared *bool
	result   recipeexec.Result
}

func (executor orderedRecipeExecutor) ExecuteTask(context.Context, recipeexec.TaskV1, cloudorchestrator.RecipeExecutionManifestV1) (recipeexec.Result, error) {
	executor.t.Helper()
	if !*executor.prepared {
		executor.t.Fatal("executor resolved the task before artifact registration")
	}
	return executor.result, nil
}

type loopRecipeExecutor struct {
	result recipeexec.Result
	err    error
}

func (executor loopRecipeExecutor) ExecuteTask(context.Context, recipeexec.TaskV1, cloudorchestrator.RecipeExecutionManifestV1) (recipeexec.Result, error) {
	return executor.result, executor.err
}

type loopRecipeReport struct {
	status     recipeexec.TaskStatus
	checkpoint string
	errorCode  string
}

type loopRecipeTransport struct {
	claimed ClaimedRecipeTask
	reports []loopRecipeReport
}

func (transport *loopRecipeTransport) RetryPending(context.Context) error {
	return ErrNoPendingRecipeEvent
}
func (transport *loopRecipeTransport) Claim(context.Context) (ClaimedRecipeTask, bool, error) {
	return transport.claimed, true, nil
}
func (transport *loopRecipeTransport) Report(_ context.Context, _ ClaimedRecipeTask, status recipeexec.TaskStatus, checkpoint, errorCode, _ string) error {
	transport.reports = append(transport.reports, loopRecipeReport{status: status, checkpoint: checkpoint, errorCode: errorCode})
	return nil
}

func TestRecipeTaskTransportRejectsUnboundManifestBeforeExecution(t *testing.T) {
	manifest := testRecipeExecutionManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: manifestDigest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		wrong := manifest
		wrong.WorkerResourceManifestDigest = recipeDigest('f')
		writeWorkerJSON(t, writer, http.StatusOK, recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &wrong})
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL + "/v2/worker-sessions")
	bootstrap := validTestManifest(endpoint.String())
	session := &SessionClient{manifest: bootstrap, endpoint: endpoint, client: server.Client(), now: time.Now, state: SessionStateActive, access: "token", epoch: 7}
	client, err := session.NewRecipeTaskClient()
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.Claim(context.Background()); err == nil || found {
		t.Fatalf("unbound claim = found:%v error:%v", found, err)
	}
}

func TestRecipeTaskClientTreatsArtifactPendingAsNoWorkAndKeepsAccessTransient(t *testing.T) {
	manifest := testRecipeExecutionManifest()
	manifestDigest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: manifestDigest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	access := recipeexec.ArtifactAccessV1{Method: http.MethodGet, URL: "https://artifacts.example.invalid/archive?versionId=version-0001&temporary=secret", ExpiresAt: "2026-07-16T10:10:00.000Z", VersionID: "version-0001", MediaType: recipeexec.RecipeArtifactMediaTypeV1, SizeBytes: 1024, ArchiveSHA256: strings.Repeat("a", 64)}
	var claims int
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		claims++
		if claims == 1 {
			writeWorkerJSON(t, writer, http.StatusOK, recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "artifact_pending", LeaseEpoch: 7})
			return
		}
		writeWorkerJSON(t, writer, http.StatusOK, recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest, ArtifactAccess: &access})
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL + "/v2/worker-sessions")
	bootstrap := validTestManifest(endpoint.String())
	bootstrap.WorkerImageDigest = manifest.WorkerResourceManifestDigest
	session := &SessionClient{manifest: bootstrap, endpoint: endpoint, client: server.Client(), now: time.Now, state: SessionStateActive, access: "token", epoch: 7}
	client, err := session.NewRecipeTaskClient()
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := client.Claim(context.Background()); err != nil || found {
		t.Fatalf("artifact pending claim = found:%v error:%v", found, err)
	}
	claimed, found, err := client.Claim(context.Background())
	if err != nil || !found || claimed.ArtifactAccess == nil || *claimed.ArtifactAccess != access {
		t.Fatalf("artifact claim = %#v, found:%v error:%v", claimed, found, err)
	}
	if client.claimed == nil || client.claimed.ArtifactAccess != nil {
		t.Fatal("temporary artifact access was retained in Recipe task session state")
	}
}

func TestRecipeTaskClientRetriesTheExactPendingEvent(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	manifest := testRecipeExecutionManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	var eventBodies [][]byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v2/worker-sessions/worker-session-v2-01/recipe-tasks/claim":
			writeWorkerJSON(t, writer, http.StatusOK, recipeexec.TaskClaimResponseV1{Schema: recipeexec.TaskClaimResponseV1Schema, Status: "claimed", LeaseEpoch: 7, Task: &task, Manifest: &manifest})
		case "/v2/worker-sessions/worker-session-v2-01/recipe-tasks/recipe-task-0001/events":
			body := mustReadAll(t, request.Body)
			eventBodies = append(eventBodies, append([]byte(nil), body...))
			if len(eventBodies) == 1 {
				http.Error(writer, "lost response", http.StatusServiceUnavailable)
				return
			}
			event, err := recipeexec.ParseEventV1(body)
			if err != nil {
				http.Error(writer, "event", http.StatusBadRequest)
				return
			}
			writeWorkerJSON(t, writer, http.StatusOK, recipeexec.EventReceiptV1{Schema: recipeexec.EventReceiptV1Schema, TaskID: event.TaskID, Attempt: event.Attempt, LeaseEpoch: event.LeaseEpoch, Sequence: event.Sequence, Disposition: "idempotent"})
		default:
			http.Error(writer, "path", http.StatusNotFound)
		}
	}))
	defer server.Close()
	endpoint, _ := url.Parse(server.URL + "/v2/worker-sessions")
	bootstrap := validTestManifest(endpoint.String())
	bootstrap.WorkerImageDigest = manifest.WorkerResourceManifestDigest
	session := &SessionClient{manifest: bootstrap, endpoint: endpoint, client: server.Client(), now: func() time.Time { return now }, state: SessionStateActive, access: "token", epoch: 7}
	client, err := session.NewRecipeTaskClient()
	if err != nil {
		t.Fatal(err)
	}
	claimed, found, err := client.Claim(context.Background())
	if err != nil || !found {
		t.Fatalf("Claim() = (%#v, %v, %v)", claimed, found, err)
	}
	if err := client.Report(context.Background(), claimed, recipeexec.TaskStatusRunning, manifest.CheckpointSequence[0], "", digest); err == nil {
		t.Fatal("Report() unexpectedly accepted the lost response")
	}
	if err := client.RetryPending(context.Background()); err != nil {
		t.Fatalf("RetryPending() error = %v", err)
	}
	if len(eventBodies) != 2 || !bytes.Equal(eventBodies[0], eventBodies[1]) {
		t.Fatalf("pending event was not retried exactly: %q != %q", eventBodies[0], eventBodies[1])
	}
}

func TestRecipeTaskPendingEventYieldsToANewerSessionLease(t *testing.T) {
	manifest := testRecipeExecutionManifest()
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	task := recipeexec.TaskV1{Schema: recipeexec.TaskV1Schema, TaskID: "recipe-task-0001", ExecutionID: manifest.ExecutionID, DeploymentID: manifest.DeploymentID, TaskKind: recipeexec.TaskKindRecipeExecution, RecipeExecutionManifestDigest: digest, InputDigest: recipeDigest('e'), CheckpointSequence: append([]string(nil), manifest.CheckpointSequence...), Attempt: 1}
	progress, err := recipeexec.NewProgress(task)
	if err != nil {
		t.Fatal(err)
	}
	claimed := ClaimedRecipeTask{Task: task, Manifest: manifest, Epoch: 7}
	event := recipeexec.EventV1{Schema: recipeexec.EventV1Schema, TaskID: task.TaskID, Attempt: task.Attempt, LeaseEpoch: 7, Sequence: 1, Status: recipeexec.TaskStatusRunning, Checkpoint: optionalRecipeString(task.CheckpointSequence[0]), EvidenceDigest: optionalRecipeString(digest), OccurredAt: canonicalInstant(time.Now())}
	session := &SessionClient{state: SessionStateActive, access: "new-lease-token", epoch: 8}
	client := &RecipeTaskClient{session: session, claimed: &claimed, progress: progress, pending: &pendingRecipeTaskEvent{claimed: claimed, event: event}}
	if err := client.RetryPending(context.Background()); err != nil {
		t.Fatalf("RetryPending() after lease rotation error = %v", err)
	}
	if client.pending != nil || client.claimed != nil || client.progress.Task.TaskID != "" || client.progress.LastSequence != 0 || client.progress.Terminal {
		t.Fatal("stale Recipe event prevented reclaim under the new lease")
	}
}

func TestRecipeTaskLoopRequiresEveryTrustedDependencyBeforeClaim(t *testing.T) {
	resolver, err := recipeexec.NewFixedBundleResolver([]recipeexec.Bundle{{ArtifactDigest: recipeDigest('d'), ActionIDs: []string{"install-service"}}})
	if err != nil {
		t.Fatal(err)
	}
	store := &recipeMemoryCheckpointStore{}
	driver := &recipeCheckpointDriver{run: func(context.Context, recipeexec.ActionRequest, recipeexec.CheckpointReporter) error { return nil }}
	for _, test := range []struct {
		name      string
		transport *RecipeTaskClient
		resolver  *recipeexec.FixedBundleResolver
		store     recipeexec.CheckpointStore
		driver    recipeexec.ActionDriver
	}{
		{name: "transport", resolver: resolver, store: store, driver: driver},
		{name: "catalog", transport: &RecipeTaskClient{}, store: store, driver: driver},
		{name: "checkpoint store", transport: &RecipeTaskClient{}, resolver: resolver, driver: driver},
		{name: "action driver", transport: &RecipeTaskClient{}, resolver: resolver, store: store},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRecipeTaskLoop(test.transport, test.resolver, test.store, test.driver); !errors.Is(err, recipeexec.ErrExecutorConfiguration) {
				t.Fatalf("NewRecipeTaskLoop() error = %v", err)
			}
		})
	}
}

type recipeMemoryCheckpointStore struct {
	state recipeexec.CheckpointState
}

func (store *recipeMemoryCheckpointStore) Load(context.Context, recipeexec.Binding) (recipeexec.CheckpointState, error) {
	return store.state, nil
}

func (store *recipeMemoryCheckpointStore) Advance(_ context.Context, previous, next recipeexec.CheckpointState) error {
	if store.state != previous {
		return recipeexec.ErrCheckpointConflict
	}
	store.state = next
	return nil
}

type recipeCheckpointDriver struct {
	calls int
	run   func(context.Context, recipeexec.ActionRequest, recipeexec.CheckpointReporter) error
}

func (driver *recipeCheckpointDriver) Execute(ctx context.Context, request recipeexec.ActionRequest, reporter recipeexec.CheckpointReporter) error {
	driver.calls++
	return driver.run(ctx, request, reporter)
}

func testRecipeExecutionManifest() cloudorchestrator.RecipeExecutionManifestV1 {
	return cloudorchestrator.RecipeExecutionManifestV1{
		SchemaVersion: cloudorchestrator.RecipeExecutionManifestV1Schema, ExecutionID: "execution-recipe-0001", DeploymentID: "deployment-v2-0001",
		PlanID: "plan-recipe-0001", PlanHash: recipeDigest('a'), PlanRevision: 1, RecipeDigest: recipeDigest('b'), WorkerResourceManifestDigest: recipeDigest('c'),
		ArtifactDigest: recipeDigest('d'), ActionID: "install-service", RootRequired: true, TimeoutSeconds: 60,
		CheckpointSequence: []string{"artifact_verified", "install_complete", "health_verified"},
		SemanticReadiness:  cloudorchestrator.OCIServiceLoopbackProbeV1{Scheme: cloudorchestrator.OCIServiceProbeHTTP, Port: 18080, Path: "/ready", ExpectedStatus: 200, BodySHA256: FixedReadinessEvidenceDigest()},
	}
}

func recipeDigest(character rune) string {
	value := make([]byte, 64)
	for index := range value {
		value[index] = byte(character)
	}
	return "sha256:" + string(value)
}
