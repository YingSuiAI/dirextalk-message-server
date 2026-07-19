package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"
)

const (
	testKnowledgeBindingID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testKnowledgeSourceID  = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	testKnowledgeUploadID  = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

type knowledgeClientStub struct {
	state          KnowledgeState
	sources        KnowledgeSourcePage
	upload         KnowledgeUpload
	source         KnowledgeSource
	search         KnowledgeSearchResult
	status         KnowledgeStatus
	err            error
	calls          []string
	updates        []KnowledgeConfigUpdate
	starts         []KnowledgeUploadStart
	chunks         []KnowledgeUploadChunk
	finishes       []KnowledgeUploadFinish
	memories       []KnowledgeMemoryCreate
	deletes        []KnowledgeSourceDelete
	searchRequests []KnowledgeSearch
	startResponse  func(KnowledgeUploadStart) KnowledgeUpload
}

func (stub *knowledgeClientStub) GetKnowledgeState(_ context.Context, selector string) (KnowledgeState, error) {
	stub.calls = append(stub.calls, "config.get:"+selector)
	return stub.state, stub.err
}

func (stub *knowledgeClientStub) UpdateKnowledgeConfig(_ context.Context, request KnowledgeConfigUpdate) (KnowledgeState, error) {
	stub.calls = append(stub.calls, "config.update")
	stub.updates = append(stub.updates, request)
	return stub.state, stub.err
}

func (stub *knowledgeClientStub) ListKnowledgeSources(_ context.Context, request KnowledgeSourceList) (KnowledgeSourcePage, error) {
	stub.calls = append(stub.calls, "sources.list")
	return stub.sources, stub.err
}

func (stub *knowledgeClientStub) DeleteKnowledgeSource(_ context.Context, request KnowledgeSourceDelete) (KnowledgeSource, error) {
	stub.calls = append(stub.calls, "sources.delete")
	stub.deletes = append(stub.deletes, request)
	return stub.source, stub.err
}

func (stub *knowledgeClientStub) StartKnowledgeUpload(_ context.Context, request KnowledgeUploadStart) (KnowledgeUpload, error) {
	stub.calls = append(stub.calls, "upload.start")
	stub.starts = append(stub.starts, request)
	if stub.startResponse != nil {
		return stub.startResponse(request), stub.err
	}
	return stub.upload, stub.err
}

func (stub *knowledgeClientStub) AppendKnowledgeUploadChunk(_ context.Context, request KnowledgeUploadChunk) (KnowledgeUpload, error) {
	stub.calls = append(stub.calls, "upload.chunk")
	stub.chunks = append(stub.chunks, request)
	return stub.upload, stub.err
}

func (stub *knowledgeClientStub) FinishKnowledgeUpload(_ context.Context, request KnowledgeUploadFinish) (KnowledgeUploadResult, error) {
	stub.calls = append(stub.calls, "upload.finish")
	stub.finishes = append(stub.finishes, request)
	return KnowledgeUploadResult{Upload: stub.upload, Source: stub.source}, stub.err
}

func (stub *knowledgeClientStub) CreateKnowledgeMemory(_ context.Context, request KnowledgeMemoryCreate) (KnowledgeSource, error) {
	stub.calls = append(stub.calls, "memory.create")
	stub.memories = append(stub.memories, request)
	return stub.source, stub.err
}

func (stub *knowledgeClientStub) SearchKnowledge(_ context.Context, request KnowledgeSearch) (KnowledgeSearchResult, error) {
	stub.calls = append(stub.calls, "search")
	stub.searchRequests = append(stub.searchRequests, request)
	return stub.search, stub.err
}

func (stub *knowledgeClientStub) GetKnowledgeStatus(context.Context) (KnowledgeStatus, error) {
	stub.calls = append(stub.calls, "status")
	return stub.status, stub.err
}

func TestRemoteKnowledgeClientOwnsEveryKnowledgeActionWhileLocalFallbackIsPreserved(t *testing.T) {
	client := &knowledgeClientStub{
		state:   validKnowledgeState(),
		sources: KnowledgeSourcePage{Sources: []KnowledgeSource{validKnowledgeSource()}},
		status:  validKnowledgeStatus(),
	}
	local := &recordingRunner{result: map[string]any{"source": "local"}}
	module := New(Config{Runner: local, Knowledge: client})
	if handlers := module.knowledgeHandlers(); len(handlers) != 10 {
		t.Fatalf("remote Knowledge handlers=%d, want all 10", len(handlers))
	}
	for _, action := range []string{
		actionKnowledgeConfigGet, actionKnowledgeConfigUpdate, actionKnowledgeSourcesList, actionKnowledgeSourcesDelete,
		actionKnowledgeUploadStart, actionKnowledgeUploadChunk, actionKnowledgeUploadFinish, actionKnowledgeMemoryCreate,
		actionKnowledgeSearch, actionKnowledgeStatus,
	} {
		if module.knowledgeHandlers()[action] == nil {
			t.Fatalf("missing typed remote handler for %s", action)
		}
	}

	for _, test := range []struct {
		action string
		params map[string]any
	}{
		{actionKnowledgeConfigGet, map[string]any{}},
		{actionKnowledgeSourcesList, map[string]any{}},
		{actionKnowledgeStatus, map[string]any{}},
	} {
		if _, actionErr := module.Handlers()[test.action](context.Background(), test.params); actionErr != nil {
			t.Fatalf("%s: %#v", test.action, actionErr)
		}
	}
	if len(local.invokes) != 0 || !reflect.DeepEqual(client.calls, []string{"config.get:", "sources.list", "status"}) {
		t.Fatalf("routing local=%v remote=%v", local.invokes, client.calls)
	}

	localOnly := New(Config{Runner: local})
	got, actionErr := localOnly.Handlers()[actionKnowledgeStatus](context.Background(), map[string]any{"legacy": true})
	if actionErr != nil || got.(map[string]any)["source"] != "local" || local.invokes[len(local.invokes)-1] != actionKnowledgeStatus {
		t.Fatalf("local fallback result=%#v error=%#v invokes=%v", got, actionErr, local.invokes)
	}
}

func TestRemoteKnowledgeConfigUpdateChangesOnlyEnabledIntent(t *testing.T) {
	state := validKnowledgeState()
	state.Config.Enabled = false
	state.Config.Revision = 8
	state.Config.UpdatedAt = state.Config.UpdatedAt.Add(time.Second)
	client := &knowledgeClientStub{state: state}
	got, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeConfigUpdate](context.Background(), map[string]any{
		"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "expected_revision": int64(7), "enabled": false,
	})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	if len(client.updates) != 1 || client.updates[0] != (KnowledgeConfigUpdate{
		IdempotencyKey: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", ExpectedRevision: 7, Enabled: false,
	}) {
		t.Fatalf("update=%#v", client.updates)
	}
	response := got.(map[string]any)
	config := response["config"].(map[string]any)
	if response["revision"] != int64(8) || config["enabled"] != false || config["embedding_profile_id"] != "managed-default" {
		t.Fatalf("response=%#v", response)
	}
	for _, forbidden := range []string{"api_key", "secret_ref", "provider", "model", "base_url", "ocr_profile", "embedding_profile"} {
		if _, present := config[forbidden]; present {
			t.Fatalf("config response exposed %s: %#v", forbidden, config)
		}
	}
}

func TestRemoteKnowledgeBoundaryRejectsOwnerCredentialsModelsAndUnknownFields(t *testing.T) {
	validUpdate := map[string]any{
		"idempotency_key":   "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		"expected_revision": int64(7),
		"enabled":           true,
	}
	for name, mutate := range map[string]func(map[string]any){
		"owner":             func(value map[string]any) { value["owner_id"] = "attacker" },
		"binding":           func(value map[string]any) { value["binding_id"] = testKnowledgeBindingID },
		"api key":           func(value map[string]any) { value["api_key"] = "must-not-cross" },
		"secret ref":        func(value map[string]any) { value["secret_ref"] = "secret_ref:model" },
		"base url":          func(value map[string]any) { value["base_url"] = "https://attacker.example" },
		"provider":          func(value map[string]any) { value["provider"] = "deepseek" },
		"caller model":      func(value map[string]any) { value["model"] = "caller-model" },
		"embedding profile": func(value map[string]any) { value["embedding_profile"] = map[string]any{"api_key": "must-not-cross"} },
		"profile id":        func(value map[string]any) { value["embedding_profile_id"] = "caller-profile" },
		"ocr profile":       func(value map[string]any) { value["ocr_profile"] = map[string]any{"provider": "caller"} },
		"deployment":        func(value map[string]any) { value["deployment_id"] = testKnowledgeBindingID },
		"service":           func(value map[string]any) { value["managed_service_id"] = testKnowledgeBindingID },
		"recipe":            func(value map[string]any) { value["recipe_digest"] = knowledgeDigest([]byte("caller")) },
		"unknown":           func(value map[string]any) { value["extra"] = true },
	} {
		t.Run(name, func(t *testing.T) {
			params := cloneMap(validUpdate)
			mutate(params)
			client := &knowledgeClientStub{state: validKnowledgeState()}
			_, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeConfigUpdate](context.Background(), params)
			if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(client.calls) != 0 {
				t.Fatalf("error=%#v calls=%v", actionErr, client.calls)
			}
		})
	}

	for _, params := range []map[string]any{
		{"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "title": "safe", "mime_type": "text/plain", "size": 10, "expected_binding_revision": 7, "ocr_profile": map[string]any{}},
		{"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "title": "safe", "mime_type": "text/plain", "size": 10, "expected_binding_revision": 7, "embedding_profile": map[string]any{}},
		{"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "title": "safe", "mime_type": "text/plain", "size": 10, "expected_binding_revision": 7, "base_url": "https://attacker.example"},
	} {
		client := &knowledgeClientStub{upload: validKnowledgeUpload()}
		_, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeUploadStart](context.Background(), params)
		if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(client.calls) != 0 {
			t.Fatalf("unsafe upload params error=%#v calls=%v", actionErr, client.calls)
		}
	}
}

func TestRemoteKnowledgeEveryActionRejectsCallerOwnerBeforeClientInvocation(t *testing.T) {
	for _, action := range []string{
		actionKnowledgeConfigGet, actionKnowledgeConfigUpdate, actionKnowledgeSourcesList, actionKnowledgeSourcesDelete,
		actionKnowledgeUploadStart, actionKnowledgeUploadChunk, actionKnowledgeUploadFinish, actionKnowledgeMemoryCreate,
		actionKnowledgeSearch, actionKnowledgeStatus,
	} {
		t.Run(action, func(t *testing.T) {
			client := &knowledgeClientStub{}
			_, actionErr := New(Config{Knowledge: client}).Handlers()[action](context.Background(), map[string]any{"owner_id": "attacker"})
			if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(client.calls) != 0 {
				t.Fatalf("caller owner crossed %s: error=%#v calls=%v", action, actionErr, client.calls)
			}
		})
	}
}

func TestRemoteKnowledgeErrorDoesNotExposeUpstreamSecretOrBody(t *testing.T) {
	const canary = "sk-knowledge-upstream-canary-should-never-cross"
	client := &knowledgeClientStub{err: fmt.Errorf("%w: %s", ErrKnowledgeUnavailable, canary)}
	_, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeConfigGet](context.Background(), map[string]any{})
	if actionErr == nil || actionErr.Status != http.StatusServiceUnavailable || strings.Contains(actionErr.Error, canary) ||
		actionErr.Error != "Agent Knowledge is unavailable" {
		t.Fatalf("unsanitized Knowledge error=%#v", actionErr)
	}
}

func TestRemoteKnowledgeExplicitMissingBindingIsSanitizedNotFound(t *testing.T) {
	const canary = "foreign-binding-internal-body"
	client := &knowledgeClientStub{err: fmt.Errorf("%w: %s", ErrKnowledgeNotFound, canary)}
	_, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeConfigGet](context.Background(), map[string]any{
		"binding_id": testKnowledgeBindingID,
	})
	if actionErr == nil || actionErr.Status != http.StatusNotFound || actionErr.Error != "Knowledge entity was not found" ||
		strings.Contains(actionErr.Error, canary) {
		t.Fatalf("explicit missing binding error=%#v", actionErr)
	}
}

func TestRemoteKnowledgeChunkIsBoundedCanonicalAndClearedAfterOneCall(t *testing.T) {
	chunk := []byte("bounded transient knowledge bytes")
	encoded := base64.StdEncoding.EncodeToString(chunk)
	client := &knowledgeClientStub{upload: validKnowledgeUpload()}
	client.upload.ReceivedSize = int64(len(chunk))
	client.upload.NextOrdinal = 1
	client.upload.Revision = 4
	params := map[string]any{
		"idempotency_key":   "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		"upload_id":         testKnowledgeUploadID,
		"expected_revision": int64(3),
		"offset":            int64(0),
		"ordinal":           int64(0),
		"data":              encoded,
	}
	got, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeUploadChunk](context.Background(), params)
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	if got.(map[string]any)["upload_id"] != testKnowledgeUploadID || len(client.chunks) != 1 || client.chunks[0].ChunkSHA256 != knowledgeDigest(chunk) {
		t.Fatalf("result=%#v request=%#v", got, client.chunks)
	}
	for _, value := range client.chunks[0].Chunk {
		if value != 0 {
			t.Fatal("decoded chunk remained reachable after the synchronous client call")
		}
	}
	if strings.Contains(fmt.Sprint(got), encoded) || strings.Contains(fmt.Sprint(got), string(chunk)) {
		t.Fatalf("response retained blob material: %#v", got)
	}

	for name, data := range map[string]string{
		"non-base64":    "not base64!",
		"non-canonical": base64.RawStdEncoding.EncodeToString([]byte("x")),
		"empty":         "",
		"oversized":     base64.StdEncoding.EncodeToString(make([]byte, maxKnowledgeChunkBytes+1)),
	} {
		t.Run(name, func(t *testing.T) {
			invalid := cloneMap(params)
			invalid["data"] = data
			stub := &knowledgeClientStub{}
			_, actionErr := New(Config{Knowledge: stub}).Handlers()[actionKnowledgeUploadChunk](context.Background(), invalid)
			if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(stub.calls) != 0 {
				t.Fatalf("error=%#v calls=%v", actionErr, stub.calls)
			}
		})
	}
	invalidOrdinal := cloneMap(params)
	invalidOrdinal["ordinal"] = int64(maxKnowledgeAttachmentChunks)
	stub := &knowledgeClientStub{}
	_, actionErr = New(Config{Knowledge: stub}).Handlers()[actionKnowledgeUploadChunk](context.Background(), invalidOrdinal)
	if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(stub.calls) != 0 {
		t.Fatalf("out-of-range chunk ordinal error=%#v calls=%v", actionErr, stub.calls)
	}
	oversizedEnd := cloneMap(params)
	oversizedEnd["offset"] = maxKnowledgeAttachmentBytes
	oversizedEnd["data"] = base64.StdEncoding.EncodeToString([]byte("x"))
	stub = &knowledgeClientStub{}
	_, actionErr = New(Config{Knowledge: stub}).Handlers()[actionKnowledgeUploadChunk](context.Background(), oversizedEnd)
	if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(stub.calls) != 0 {
		t.Fatalf("chunk past attachment limit error=%#v calls=%v", actionErr, stub.calls)
	}
	exactLimit := cloneMap(params)
	exactLimit["offset"] = maxKnowledgeAttachmentBytes - 1
	exactLimit["data"] = base64.StdEncoding.EncodeToString([]byte("x"))
	boundaryClient := &knowledgeClientStub{upload: validKnowledgeUpload()}
	boundaryClient.upload.DeclaredSize = maxKnowledgeAttachmentBytes
	boundaryClient.upload.ReceivedSize = maxKnowledgeAttachmentBytes
	boundaryClient.upload.NextOrdinal = 1
	boundaryClient.upload.Revision = 4
	if _, actionErr = New(Config{Knowledge: boundaryClient}).Handlers()[actionKnowledgeUploadChunk](context.Background(), exactLimit); actionErr != nil ||
		len(boundaryClient.calls) != 1 {
		t.Fatalf("chunk ending at attachment limit error=%#v calls=%v", actionErr, boundaryClient.calls)
	}
}

func TestRemoteKnowledgeStartDerivesStableReplayIDsAndPreservesResponseAliases(t *testing.T) {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	client := &knowledgeClientStub{startResponse: func(request KnowledgeUploadStart) KnowledgeUpload {
		return KnowledgeUpload{
			BindingID: testKnowledgeBindingID, SourceID: request.SourceID, UploadID: request.UploadID, Status: "receiving",
			MediaType: request.MediaType, DeclaredSize: request.Size, ReceivedSize: 0,
			NextOrdinal: 0, Revision: 1, BindingRevision: request.ExpectedBindingRevision, CreatedAt: now, UpdatedAt: now,
		}
	}}
	params := map[string]any{
		"idempotency_key":           "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		"title":                     "Runbook",
		"mime_type":                 "text/markdown",
		"size":                      int64(42),
		"expected_binding_revision": int64(7),
	}
	module := New(Config{Knowledge: client})

	for iteration := 0; iteration < 2; iteration++ {
		got, actionErr := module.Handlers()[actionKnowledgeUploadStart](context.Background(), params)
		if actionErr != nil {
			t.Fatal(actionErr)
		}
		request := client.starts[len(client.starts)-1]
		if got.(map[string]any)["upload_id"] != request.UploadID || got.(map[string]any)["source_id"] != request.SourceID ||
			got.(map[string]any)["revision"] != int64(1) || got.(map[string]any)["received_size"] != int64(0) {
			t.Fatalf("upload response aliases=%#v", got)
		}
	}
	if len(client.starts) != 2 {
		t.Fatalf("start requests=%d", len(client.starts))
	}
	first, replay := client.starts[0], client.starts[1]
	if first.SourceID != replay.SourceID || first.UploadID != replay.UploadID || first.IdempotencyKey != replay.IdempotencyKey {
		t.Fatalf("replay identity drifted: first=%#v replay=%#v", first, replay)
	}
	if first.SourceID == first.UploadID || first.SourceID == "" || first.UploadID == "" || first.ExpectedBindingRevision != 7 {
		t.Fatalf("invalid derived identities: %#v", first)
	}
}

func TestRemoteKnowledgeContentMutationsRequireExactBindingRevisionFence(t *testing.T) {
	text := "revision-fenced memory"
	for _, test := range []struct {
		name   string
		action string
		params map[string]any
	}{
		{
			name: "upload start", action: actionKnowledgeUploadStart,
			params: map[string]any{
				"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "title": "Runbook",
				"mime_type": "text/plain", "size": int64(42),
			},
		},
		{
			name: "memory create", action: actionKnowledgeMemoryCreate,
			params: map[string]any{
				"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "source_id": testKnowledgeSourceID,
				"title": "Memory", "text": text, "content_sha256": knowledgeDigest([]byte(text)),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &knowledgeClientStub{}
			_, actionErr := New(Config{Knowledge: client}).Handlers()[test.action](context.Background(), test.params)
			if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(client.calls) != 0 {
				t.Fatalf("missing binding revision error=%#v calls=%v", actionErr, client.calls)
			}
		})
	}
}

func TestRemoteKnowledgeSourceResponsesKeepLegacyHarmlessFieldNames(t *testing.T) {
	source := validKnowledgeSource()
	client := &knowledgeClientStub{sources: KnowledgeSourcePage{Sources: []KnowledgeSource{source}}}
	got, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeSourcesList](context.Background(), map[string]any{})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	items := got.(map[string]any)["sources"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("sources=%#v", items)
	}
	for key, want := range map[string]any{
		"source_id": source.SourceID, "title": source.Title, "kind": source.Kind, "status": source.Status,
		"chunk_count": source.ChunkCount, "size": source.Size, "error": source.Error,
	} {
		if items[0][key] != want {
			t.Fatalf("source[%s]=%#v, want %#v: %#v", key, items[0][key], want, items[0])
		}
	}

	malformed := source
	malformed.Error = "backend_diagnostic"
	_, actionErr = New(Config{Knowledge: &knowledgeClientStub{
		sources: KnowledgeSourcePage{Sources: []KnowledgeSource{malformed}},
	}}).Handlers()[actionKnowledgeSourcesList](context.Background(), map[string]any{})
	if actionErr == nil || actionErr.Status != http.StatusBadGateway {
		t.Fatalf("unrecognized error classification was projected: %#v", actionErr)
	}
}

func TestRemoteKnowledgeMemoryContentIsDigestBoundAndCleared(t *testing.T) {
	text := "durable private memory"
	digest := knowledgeDigest([]byte(text))
	source := validKnowledgeSource()
	source.Kind = "memory"
	source.MediaType = "text/plain; charset=utf-8"
	source.ChunkCount = 1
	source.Title = "Private memory"
	source.Size = int64(len(text))
	source.ContentSHA256 = digest
	client := &knowledgeClientStub{source: source}
	got, actionErr := New(Config{Knowledge: client}).Handlers()[actionKnowledgeMemoryCreate](context.Background(), map[string]any{
		"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "source_id": testKnowledgeSourceID,
		"title": "Private memory", "text": text, "content_sha256": digest, "expected_binding_revision": int64(7),
	})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	if len(client.memories) != 1 || client.memories[0].ContentSHA256 != digest || client.memories[0].ExpectedBindingRevision != 7 || strings.Contains(fmt.Sprint(got), text) {
		t.Fatalf("request=%#v response=%#v", client.memories, got)
	}
	for _, value := range client.memories[0].Content {
		if value != 0 {
			t.Fatal("memory content remained reachable after the synchronous client call")
		}
	}
	invalid := map[string]any{
		"idempotency_key": "dddddddd-dddd-4ddd-8ddd-dddddddddddd", "source_id": testKnowledgeSourceID,
		"title": "Private memory", "text": text, "content_sha256": knowledgeDigest([]byte("different")), "expected_binding_revision": int64(7),
	}
	stub := &knowledgeClientStub{}
	_, actionErr = New(Config{Knowledge: stub}).Handlers()[actionKnowledgeMemoryCreate](context.Background(), invalid)
	if actionErr == nil || actionErr.Status != http.StatusBadRequest || len(stub.calls) != 0 {
		t.Fatalf("mismatched digest error=%#v calls=%v", actionErr, stub.calls)
	}
}

func validKnowledgeState() KnowledgeState {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return KnowledgeState{
		Available: true,
		Capabilities: KnowledgeCapabilities{
			Config: true, AttachmentUpload: true, Memory: true, Search: true,
			EmbeddingProfileIDs: []string{"managed-default"}, MaxAttachmentSize: maxKnowledgeAttachmentBytes,
			MaxAttachmentChunkSize: maxKnowledgeChunkBytes, MaxSearchResults: maxKnowledgeSearchResults,
		},
		Config: &KnowledgeConfig{
			BindingID: testKnowledgeBindingID, DeploymentID: "11111111-1111-4111-8111-111111111111",
			ManagedServiceID: "22222222-2222-4222-8222-222222222222", RecipeDigest: knowledgeDigest([]byte("recipe")),
			EmbeddingProfileID: "managed-default", Enabled: true, Revision: 7, CreatedAt: now, UpdatedAt: now,
		},
	}
}

func validKnowledgeSource() KnowledgeSource {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return KnowledgeSource{
		BindingID: testKnowledgeBindingID, SourceID: testKnowledgeSourceID, Title: "Runbook", Kind: "attachment", Status: "ready", MediaType: "text/markdown",
		Size: 42, ContentSHA256: knowledgeDigest([]byte("content")), ChunkCount: 2, Revision: 3,
		CreatedAt: now, UpdatedAt: now, Error: "",
	}
}

func validKnowledgeUpload() KnowledgeUpload {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return KnowledgeUpload{
		BindingID: testKnowledgeBindingID, SourceID: testKnowledgeSourceID, UploadID: testKnowledgeUploadID, Status: "receiving", MediaType: "text/plain",
		DeclaredSize: 42, ReceivedSize: 0, NextOrdinal: 0, Revision: 3, BindingRevision: 7,
		CreatedAt: now, UpdatedAt: now,
	}
}

func validKnowledgeStatus() KnowledgeStatus {
	return KnowledgeStatus{
		BindingID: testKnowledgeBindingID, Enabled: true, BackendStatus: "ready", ReadySourceCount: 1, UploadingSourceCount: 0,
		FailedSourceCount: 0, BindingRevision: 7, CheckedAt: time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC),
	}
}

func knowledgeDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest[:])
}
