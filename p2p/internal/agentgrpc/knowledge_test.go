package agentgrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	agentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agent"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	grpcKnowledgeBindingID = "33333333-3333-4333-8333-333333333333"
	grpcKnowledgeSourceID  = "44444444-4444-4444-8444-444444444444"
	grpcKnowledgeUploadID  = "55555555-5555-4555-8555-555555555555"
	grpcKnowledgeRecipe    = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	grpcKnowledgeContent   = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestRunnerKnowledgeGetAndUpdateBindTrustedOwnerAndPreserveImmutableSpec(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{UnaryTimeout: time.Second})

	state, err := runner.GetKnowledgeState(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !state.Available || state.Config == nil || state.Config.BindingID != grpcKnowledgeBindingID || state.Config.Revision != 7 ||
		!reflect.DeepEqual(state.Capabilities.EmbeddingProfileIDs, []string{"managed-default"}) {
		t.Fatalf("state=%#v", state)
	}
	server.knowledge.mu.Lock()
	getRequest := server.knowledge.getRequests[len(server.knowledge.getRequests)-1]
	authorization := server.knowledge.authorization
	deadlineSet := server.knowledge.deadlineSet
	methods := append([]string(nil), server.knowledge.methods...)
	server.knowledge.mu.Unlock()
	if getRequest.GetOwnerId() != "owner-from-config" || getRequest.GetBindingId() != "" {
		t.Fatalf("trusted-owner request=%#v", getRequest)
	}
	if authorization != "DTX-Service-Key "+testServiceKey || !deadlineSet {
		t.Fatalf("authorization/deadline=%q/%v", authorization, deadlineSet)
	}
	for _, method := range methods {
		if !strings.HasPrefix(method, "/dirextalk.agent.v1.KnowledgeService/") {
			t.Fatalf("Knowledge client escaped its least-privilege service: %v", methods)
		}
	}

	updated, err := runner.UpdateKnowledgeConfig(context.Background(), agentmodule.KnowledgeConfigUpdate{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", ExpectedRevision: 7, Enabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	server.knowledge.mu.Lock()
	putRequest := server.knowledge.putRequests[len(server.knowledge.putRequests)-1]
	server.knowledge.mu.Unlock()
	if putRequest.GetOwnerId() != "owner-from-config" || putRequest.GetBindingId() != grpcKnowledgeBindingID ||
		putRequest.GetExpectedRevision() != 7 || putRequest.GetIdempotencyKey() != "66666666-6666-4666-8666-666666666666" || putRequest.GetSpec().GetEnabled() {
		t.Fatalf("put request=%#v", putRequest)
	}
	original := validKnowledgeConfigProto().GetSpec()
	if !sameKnowledgeConfigIdentity(original, putRequest.GetSpec()) || updated.Config == nil || updated.Config.Revision != 8 || updated.Config.Enabled {
		t.Fatalf("immutable spec/update drift: request=%#v state=%#v", putRequest.GetSpec(), updated)
	}

	// A response-lost replay must reach Agent's idempotency ledger even after
	// the config read has advanced past the caller's original revision fence.
	server.knowledge.getConfig = func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
		config := validKnowledgeConfigProto()
		config.Spec.Enabled = false
		config.Revision = 8
		config.UpdatedAt = timestamppb.New(config.GetUpdatedAt().AsTime().Add(time.Second))
		return &agentv1.GetKnowledgeConfigResponse{Config: config}, nil
	}
	replayed, err := runner.UpdateKnowledgeConfig(context.Background(), agentmodule.KnowledgeConfigUpdate{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", ExpectedRevision: 7, Enabled: false,
	})
	if err != nil || replayed.Config == nil || replayed.Config.Revision != 8 || replayed.Config.Enabled {
		t.Fatalf("config replay state=%#v err=%v", replayed, err)
	}
	server.knowledge.mu.Lock()
	putRequests := append([]*agentv1.PutKnowledgeConfigRequest(nil), server.knowledge.putRequests...)
	server.knowledge.mu.Unlock()
	if len(putRequests) != 2 || !proto.Equal(putRequests[0], putRequests[1]) {
		t.Fatalf("config replay changed request: %#v", putRequests)
	}
}

func TestRunnerKnowledgeNotConfiguredAndAmbiguousStatesAreHonest(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	server.knowledge.getConfig = func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
		return nil, status.Error(codes.NotFound, "none")
	}
	state, err := runner.GetKnowledgeState(context.Background(), "")
	if err != nil || !state.Available || state.Config != nil {
		t.Fatalf("not-configured state=%#v err=%v", state, err)
	}
	if _, err = runner.GetKnowledgeState(context.Background(), grpcKnowledgeBindingID); !errors.Is(err, agentmodule.ErrKnowledgeNotFound) ||
		strings.Contains(err.Error(), "none") {
		t.Fatalf("explicit missing binding error=%v", err)
	}
	if _, err = runner.GetKnowledgeStatus(context.Background()); !errors.Is(err, agentmodule.ErrKnowledgeNotConfigured) {
		t.Fatalf("not-configured operation error=%v", err)
	}
	if _, err = runner.UpdateKnowledgeConfig(context.Background(), agentmodule.KnowledgeConfigUpdate{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", ExpectedRevision: 1, Enabled: true,
	}); !errors.Is(err, agentmodule.ErrKnowledgeNotConfigured) {
		t.Fatalf("not-configured update error=%v", err)
	}
	server.knowledge.mu.Lock()
	putCalls := len(server.knowledge.putRequests)
	server.knowledge.mu.Unlock()
	if putCalls != 0 {
		t.Fatalf("not-configured update reached mutation %d times", putCalls)
	}

	server.knowledge.getConfig = func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "multiple internal bindings")
	}
	_, err = runner.StartKnowledgeUpload(context.Background(), agentmodule.KnowledgeUploadStart{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", SourceID: grpcKnowledgeSourceID,
		UploadID: grpcKnowledgeUploadID, Title: "Runbook", MediaType: "text/plain", Size: 4, ExpectedBindingRevision: 7,
	})
	if !errors.Is(err, agentmodule.ErrKnowledgeState) || strings.Contains(err.Error(), "multiple internal") {
		t.Fatalf("ambiguous error=%v", err)
	}
	server.knowledge.mu.Lock()
	startCalls := len(server.knowledge.startRequests)
	server.knowledge.mu.Unlock()
	if startCalls != 0 {
		t.Fatalf("ambiguous binding reached mutation %d times", startCalls)
	}
}

func TestRunnerKnowledgeChunkDoesNotRetryAndReplayMappingIsStable(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	chunk := []byte("one bounded chunk")
	request := agentmodule.KnowledgeUploadChunk{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", UploadID: grpcKnowledgeUploadID,
		ExpectedRevision: 3, Offset: 0, Ordinal: 0, Chunk: chunk, ChunkSHA256: grpcKnowledgeContent,
	}
	server.knowledge.appendChunk = func(*agentv1.AppendKnowledgeAttachmentChunkRequest) (*agentv1.AppendKnowledgeAttachmentChunkResponse, error) {
		return nil, status.Error(codes.Unavailable, "backend credential canary")
	}
	_, err := runner.AppendKnowledgeUploadChunk(context.Background(), request)
	if !errors.Is(err, agentmodule.ErrKnowledgeUnavailable) || strings.Contains(err.Error(), "credential canary") {
		t.Fatalf("chunk error=%v", err)
	}
	server.knowledge.mu.Lock()
	appendCalls := len(server.knowledge.appendRequests)
	server.knowledge.mu.Unlock()
	if appendCalls != 1 {
		t.Fatalf("blob mutation was retried %d times", appendCalls)
	}

	server.knowledge.appendChunk = nil
	first, err := runner.AppendKnowledgeUploadChunk(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := runner.AppendKnowledgeUploadChunk(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replay) || first.UploadID != grpcKnowledgeUploadID || first.Revision != 4 {
		t.Fatalf("replay mapping first=%#v replay=%#v", first, replay)
	}
	server.knowledge.mu.Lock()
	last := server.knowledge.appendRequests[len(server.knowledge.appendRequests)-1]
	server.knowledge.mu.Unlock()
	if last.GetOwnerId() != "owner-from-config" || last.GetBindingId() != grpcKnowledgeBindingID || last.GetUploadId() != request.UploadID ||
		last.GetExpectedUploadRevision() != request.ExpectedRevision || last.GetChunkSha256() != request.ChunkSHA256 || !reflect.DeepEqual(last.GetChunk(), chunk) {
		t.Fatalf("chunk request=%#v", last)
	}
}

func TestRunnerKnowledgeMutationsPreserveCallerBindingRevisionAcrossReplay(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	server.knowledge.getConfig = func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
		config := validKnowledgeConfigProto()
		config.Revision = 9
		config.UpdatedAt = timestamppb.New(config.GetUpdatedAt().AsTime().Add(2 * time.Second))
		return &agentv1.GetKnowledgeConfigResponse{Config: config}, nil
	}

	upload, err := runner.StartKnowledgeUpload(context.Background(), agentmodule.KnowledgeUploadStart{
		IdempotencyKey: "66666666-6666-4666-8666-666666666666", SourceID: grpcKnowledgeSourceID,
		UploadID: grpcKnowledgeUploadID, Title: "Runbook", MediaType: "text/plain", Size: 42, ExpectedBindingRevision: 7,
	})
	if err != nil || upload.BindingRevision != 7 {
		t.Fatalf("start replay upload=%#v err=%v", upload, err)
	}
	chunk := []byte("replayed chunk")
	appended, err := runner.AppendKnowledgeUploadChunk(context.Background(), agentmodule.KnowledgeUploadChunk{
		IdempotencyKey: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", UploadID: grpcKnowledgeUploadID,
		ExpectedRevision: 3, Offset: 0, Ordinal: 0, Chunk: chunk, ChunkSHA256: agentKnowledgeDigest(chunk),
	})
	if err != nil || appended.BindingRevision != 7 || appended.Revision != 4 {
		t.Fatalf("chunk replay upload=%#v err=%v", appended, err)
	}
	committed, err := runner.FinishKnowledgeUpload(context.Background(), agentmodule.KnowledgeUploadFinish{
		IdempotencyKey: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", UploadID: grpcKnowledgeUploadID,
		ExpectedRevision: 3, ContentSHA256: grpcKnowledgeContent,
	})
	if err != nil || committed.Upload.BindingRevision != 7 || committed.Upload.Status != "committed" {
		t.Fatalf("finish replay result=%#v err=%v", committed, err)
	}

	content := []byte("durable replay memory")
	digest := agentKnowledgeDigest(content)
	source, err := runner.CreateKnowledgeMemory(context.Background(), agentmodule.KnowledgeMemoryCreate{
		IdempotencyKey: "77777777-7777-4777-8777-777777777777", SourceID: grpcKnowledgeSourceID,
		Title: "Memory", Content: content, ContentSHA256: digest, ExpectedBindingRevision: 7,
	})
	if err != nil || source.SourceID != grpcKnowledgeSourceID {
		t.Fatalf("memory replay source=%#v err=%v", source, err)
	}

	deleted, err := runner.DeleteKnowledgeSource(context.Background(), agentmodule.KnowledgeSourceDelete{
		IdempotencyKey: "88888888-8888-4888-8888-888888888888", SourceID: grpcKnowledgeSourceID,
		ExpectedBindingRevision: 7, ExpectedSourceRevision: 3,
	})
	if err != nil || deleted.Status != "deleted" || deleted.Revision != 4 {
		t.Fatalf("delete replay source=%#v err=%v", deleted, err)
	}

	server.knowledge.mu.Lock()
	startRequest := server.knowledge.startRequests[len(server.knowledge.startRequests)-1]
	memoryRequest := server.knowledge.memoryRequests[len(server.knowledge.memoryRequests)-1]
	deleteRequest := server.knowledge.deleteRequests[len(server.knowledge.deleteRequests)-1]
	server.knowledge.mu.Unlock()
	if startRequest.GetExpectedBindingRevision() != 7 || memoryRequest.GetExpectedBindingRevision() != 7 || deleteRequest.GetExpectedBindingRevision() != 7 {
		t.Fatalf("binding revision drift: start=%d memory=%d delete=%d", startRequest.GetExpectedBindingRevision(), memoryRequest.GetExpectedBindingRevision(), deleteRequest.GetExpectedBindingRevision())
	}
	clear(memoryRequest.Content)
}

func TestRunnerKnowledgeReadAndCommitMappingsAreComplete(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})

	page, err := runner.ListKnowledgeSources(context.Background(), agentmodule.KnowledgeSourceList{PageSize: 10})
	if err != nil || len(page.Sources) != 1 || page.Sources[0].SourceID != grpcKnowledgeSourceID || page.NextPageToken != "next-page" {
		t.Fatalf("source page=%#v err=%v", page, err)
	}
	result, err := runner.FinishKnowledgeUpload(context.Background(), agentmodule.KnowledgeUploadFinish{
		IdempotencyKey: "99999999-9999-4999-8999-999999999999", UploadID: grpcKnowledgeUploadID,
		ExpectedRevision: 3, ContentSHA256: grpcKnowledgeContent,
	})
	if err != nil || result.Upload.Status != "committed" || result.Upload.ReceivedSize != result.Upload.DeclaredSize ||
		result.Source.SourceID != grpcKnowledgeSourceID || result.Source.ContentSHA256 != grpcKnowledgeContent {
		t.Fatalf("commit result=%#v err=%v", result, err)
	}
	search, err := runner.SearchKnowledge(context.Background(), agentmodule.KnowledgeSearch{Query: "runbook", Limit: 5, SourceIDs: []string{grpcKnowledgeSourceID}})
	if err != nil || search.BindingRevision != 7 || len(search.Matches) != 1 || search.Matches[0].SourceID != grpcKnowledgeSourceID {
		t.Fatalf("search=%#v err=%v", search, err)
	}
	knowledgeStatus, err := runner.GetKnowledgeStatus(context.Background())
	if err != nil || knowledgeStatus.BindingID != grpcKnowledgeBindingID || knowledgeStatus.BackendStatus != "ready" || knowledgeStatus.BindingRevision != 7 {
		t.Fatalf("status=%#v err=%v", knowledgeStatus, err)
	}

	server.knowledge.mu.Lock()
	commitRequest := server.knowledge.commitRequests[len(server.knowledge.commitRequests)-1]
	searchRequest := server.knowledge.searchRequests[len(server.knowledge.searchRequests)-1]
	server.knowledge.mu.Unlock()
	if commitRequest.GetOwnerId() != "owner-from-config" || commitRequest.GetBindingId() != grpcKnowledgeBindingID ||
		commitRequest.GetExpectedUploadRevision() != 3 || searchRequest.GetExpectedBindingRevision() != 7 ||
		!reflect.DeepEqual(searchRequest.GetSourceIds(), []string{grpcKnowledgeSourceID}) {
		t.Fatalf("read/commit request drift: commit=%#v search=%#v", commitRequest, searchRequest)
	}
}

func TestRunnerKnowledgeRejectsForeignOrMalformedResponseBeforeProjection(t *testing.T) {
	server := startRuntimeServer(t)
	runner := newTestRunner(t, server, Config{})
	server.knowledge.getConfig = func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
		config := validKnowledgeConfigProto()
		config.OwnerId = "foreign-owner"
		return &agentv1.GetKnowledgeConfigResponse{Config: config}, nil
	}
	_, err := runner.GetKnowledgeState(context.Background(), "")
	if !errors.Is(err, agentmodule.ErrInvalidKnowledgeResponse) || strings.Contains(err.Error(), "foreign-owner") {
		t.Fatalf("foreign response error=%v", err)
	}
}

type knowledgeTestService struct {
	agentv1.UnimplementedKnowledgeServiceServer
	mu             sync.Mutex
	getRequests    []*agentv1.GetKnowledgeConfigRequest
	putRequests    []*agentv1.PutKnowledgeConfigRequest
	startRequests  []*agentv1.StartKnowledgeAttachmentUploadRequest
	appendRequests []*agentv1.AppendKnowledgeAttachmentChunkRequest
	memoryRequests []*agentv1.CreateKnowledgeMemoryRequest
	deleteRequests []*agentv1.DeleteKnowledgeSourceRequest
	commitRequests []*agentv1.CommitKnowledgeAttachmentUploadRequest
	searchRequests []*agentv1.SearchKnowledgeRequest
	methods        []string
	authorization  string
	deadlineSet    bool
	getConfig      func(*agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error)
	appendChunk    func(*agentv1.AppendKnowledgeAttachmentChunkRequest) (*agentv1.AppendKnowledgeAttachmentChunkResponse, error)
}

func newKnowledgeTestService() *knowledgeTestService { return &knowledgeTestService{} }

func (service *knowledgeTestService) capture(ctx context.Context, method string) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	authorization := ""
	if len(values) == 1 {
		authorization = values[0]
	}
	_, deadlineSet := ctx.Deadline()
	service.methods = append(service.methods, method)
	service.authorization = authorization
	service.deadlineSet = deadlineSet
}

func (service *knowledgeTestService) GetKnowledgeCapabilities(ctx context.Context, request *agentv1.GetKnowledgeCapabilitiesRequest) (*agentv1.GetKnowledgeCapabilitiesResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_GetKnowledgeCapabilities_FullMethodName)
	return &agentv1.GetKnowledgeCapabilitiesResponse{Capabilities: &agentv1.KnowledgeCapabilities{
		Config: true, AttachmentUpload: true, Memory: true, Search: true, EmbeddingProfileIds: []string{"managed-default"},
		MaxAttachmentSizeBytes: 64 << 20, MaxAttachmentChunkBytes: 256 << 10, MaxSearchResults: 50,
	}}, nil
}

func (service *knowledgeTestService) GetKnowledgeConfig(ctx context.Context, request *agentv1.GetKnowledgeConfigRequest) (*agentv1.GetKnowledgeConfigResponse, error) {
	service.mu.Lock()
	service.capture(ctx, agentv1.KnowledgeService_GetKnowledgeConfig_FullMethodName)
	service.getRequests = append(service.getRequests, proto.Clone(request).(*agentv1.GetKnowledgeConfigRequest))
	callback := service.getConfig
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	return &agentv1.GetKnowledgeConfigResponse{Config: validKnowledgeConfigProto()}, nil
}

func (service *knowledgeTestService) PutKnowledgeConfig(ctx context.Context, request *agentv1.PutKnowledgeConfigRequest) (*agentv1.PutKnowledgeConfigResponse, error) {
	service.mu.Lock()
	service.capture(ctx, agentv1.KnowledgeService_PutKnowledgeConfig_FullMethodName)
	service.putRequests = append(service.putRequests, proto.Clone(request).(*agentv1.PutKnowledgeConfigRequest))
	service.mu.Unlock()
	config := validKnowledgeConfigProto()
	config.Spec = proto.Clone(request.GetSpec()).(*agentv1.KnowledgeConfigSpec)
	config.Revision = request.GetExpectedRevision() + 1
	config.UpdatedAt = timestamppb.New(config.GetUpdatedAt().AsTime().Add(time.Second))
	return &agentv1.PutKnowledgeConfigResponse{Config: config}, nil
}

func (service *knowledgeTestService) ListKnowledgeSources(ctx context.Context, request *agentv1.ListKnowledgeSourcesRequest) (*agentv1.ListKnowledgeSourcesResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_ListKnowledgeSources_FullMethodName)
	return &agentv1.ListKnowledgeSourcesResponse{Sources: []*agentv1.KnowledgeSource{validKnowledgeSourceProto()}, NextPageToken: "next-page"}, nil
}

func (service *knowledgeTestService) StartKnowledgeAttachmentUpload(ctx context.Context, request *agentv1.StartKnowledgeAttachmentUploadRequest) (*agentv1.StartKnowledgeAttachmentUploadResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_StartKnowledgeAttachmentUpload_FullMethodName)
	service.startRequests = append(service.startRequests, proto.Clone(request).(*agentv1.StartKnowledgeAttachmentUploadRequest))
	upload := validKnowledgeUploadProto()
	upload.SourceId = request.GetSourceId()
	upload.UploadId = request.GetUploadId()
	upload.MediaType = request.GetMediaType()
	upload.DeclaredSizeBytes = request.GetDeclaredSizeBytes()
	upload.BindingRevision = request.GetExpectedBindingRevision()
	upload.Revision = 1
	return &agentv1.StartKnowledgeAttachmentUploadResponse{Upload: upload}, nil
}

func (service *knowledgeTestService) AppendKnowledgeAttachmentChunk(ctx context.Context, request *agentv1.AppendKnowledgeAttachmentChunkRequest) (*agentv1.AppendKnowledgeAttachmentChunkResponse, error) {
	service.mu.Lock()
	service.capture(ctx, agentv1.KnowledgeService_AppendKnowledgeAttachmentChunk_FullMethodName)
	service.appendRequests = append(service.appendRequests, proto.Clone(request).(*agentv1.AppendKnowledgeAttachmentChunkRequest))
	callback := service.appendChunk
	service.mu.Unlock()
	if callback != nil {
		return callback(request)
	}
	upload := validKnowledgeUploadProto()
	upload.UploadId = request.GetUploadId()
	upload.ReceivedSizeBytes = int64(len(request.GetChunk()))
	upload.NextChunkOrdinal = request.GetChunkOrdinal() + 1
	upload.Revision = request.GetExpectedUploadRevision() + 1
	return &agentv1.AppendKnowledgeAttachmentChunkResponse{Upload: upload}, nil
}

func (service *knowledgeTestService) CommitKnowledgeAttachmentUpload(ctx context.Context, request *agentv1.CommitKnowledgeAttachmentUploadRequest) (*agentv1.CommitKnowledgeAttachmentUploadResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_CommitKnowledgeAttachmentUpload_FullMethodName)
	service.commitRequests = append(service.commitRequests, proto.Clone(request).(*agentv1.CommitKnowledgeAttachmentUploadRequest))
	upload := validKnowledgeUploadProto()
	upload.UploadId = request.GetUploadId()
	upload.Status = agentv1.KnowledgeUploadStatus_KNOWLEDGE_UPLOAD_STATUS_COMMITTED
	upload.ReceivedSizeBytes = upload.DeclaredSizeBytes
	upload.NextChunkOrdinal = 1
	upload.Revision = request.GetExpectedUploadRevision() + 1
	source := validKnowledgeSourceProto()
	source.SourceId = upload.GetSourceId()
	source.SizeBytes = upload.GetDeclaredSizeBytes()
	source.ChunkCount = upload.GetNextChunkOrdinal()
	source.ContentSha256 = request.GetContentSha256()
	return &agentv1.CommitKnowledgeAttachmentUploadResponse{Upload: upload, Source: source}, nil
}

func (service *knowledgeTestService) CreateKnowledgeMemory(ctx context.Context, request *agentv1.CreateKnowledgeMemoryRequest) (*agentv1.CreateKnowledgeMemoryResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_CreateKnowledgeMemory_FullMethodName)
	service.memoryRequests = append(service.memoryRequests, proto.Clone(request).(*agentv1.CreateKnowledgeMemoryRequest))
	source := validKnowledgeSourceProto()
	source.Kind = agentv1.KnowledgeSourceKind_KNOWLEDGE_SOURCE_KIND_MEMORY
	source.MediaType = "text/plain; charset=utf-8"
	source.SourceId = request.GetSourceId()
	source.Title = request.GetTitle()
	source.SizeBytes = int64(len(request.GetContent()))
	source.ContentSha256 = request.GetContentSha256()
	source.ChunkCount = 1
	return &agentv1.CreateKnowledgeMemoryResponse{Source: source}, nil
}

func (service *knowledgeTestService) DeleteKnowledgeSource(ctx context.Context, request *agentv1.DeleteKnowledgeSourceRequest) (*agentv1.DeleteKnowledgeSourceResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_DeleteKnowledgeSource_FullMethodName)
	service.deleteRequests = append(service.deleteRequests, proto.Clone(request).(*agentv1.DeleteKnowledgeSourceRequest))
	source := validKnowledgeSourceProto()
	source.SourceId = request.GetSourceId()
	source.Status = agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_DELETED
	source.Revision = request.GetExpectedSourceRevision() + 1
	return &agentv1.DeleteKnowledgeSourceResponse{Source: source}, nil
}

func (service *knowledgeTestService) SearchKnowledge(ctx context.Context, request *agentv1.SearchKnowledgeRequest) (*agentv1.SearchKnowledgeResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_SearchKnowledge_FullMethodName)
	service.searchRequests = append(service.searchRequests, proto.Clone(request).(*agentv1.SearchKnowledgeRequest))
	return &agentv1.SearchKnowledgeResponse{
		Matches:         []*agentv1.KnowledgeSearchMatch{{SourceId: grpcKnowledgeSourceID, ChunkRef: "chunk:0", Score: 0.75}},
		BindingRevision: request.GetExpectedBindingRevision(),
	}, nil
}

func (service *knowledgeTestService) GetKnowledgeStatus(ctx context.Context, request *agentv1.GetKnowledgeStatusRequest) (*agentv1.GetKnowledgeStatusResponse, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.capture(ctx, agentv1.KnowledgeService_GetKnowledgeStatus_FullMethodName)
	now := time.Date(2026, 7, 19, 1, 2, 4, 0, time.UTC)
	return &agentv1.GetKnowledgeStatusResponse{Status: &agentv1.KnowledgeStatus{
		OwnerId: "owner-from-config", BindingId: request.GetBindingId(), Enabled: true,
		BackendStatus:    agentv1.KnowledgeBackendStatus_KNOWLEDGE_BACKEND_STATUS_READY,
		ReadySourceCount: 1, BindingRevision: 7, CheckedAt: timestamppb.New(now),
	}}, nil
}

func validKnowledgeConfigProto() *agentv1.KnowledgeConfig {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return &agentv1.KnowledgeConfig{
		OwnerId: "owner-from-config", BindingId: grpcKnowledgeBindingID, Revision: 7,
		Spec: &agentv1.KnowledgeConfigSpec{
			DeploymentId: "11111111-1111-4111-8111-111111111111", ManagedServiceId: "22222222-2222-4222-8222-222222222222",
			RecipeDigest: grpcKnowledgeRecipe, EmbeddingProfileId: "managed-default", Enabled: true,
		},
		CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now),
	}
}

func validKnowledgeUploadProto() *agentv1.KnowledgeAttachmentUpload {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return &agentv1.KnowledgeAttachmentUpload{
		OwnerId: "owner-from-config", BindingId: grpcKnowledgeBindingID, SourceId: grpcKnowledgeSourceID, UploadId: grpcKnowledgeUploadID,
		Status: agentv1.KnowledgeUploadStatus_KNOWLEDGE_UPLOAD_STATUS_RECEIVING, MediaType: "text/plain", DeclaredSizeBytes: 42,
		ReceivedSizeBytes: 0, NextChunkOrdinal: 0, Revision: 3, BindingRevision: 7,
		CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now),
	}
}

func validKnowledgeSourceProto() *agentv1.KnowledgeSource {
	now := time.Date(2026, 7, 19, 1, 2, 3, 0, time.UTC)
	return &agentv1.KnowledgeSource{
		OwnerId: "owner-from-config", BindingId: grpcKnowledgeBindingID, SourceId: grpcKnowledgeSourceID,
		Kind: agentv1.KnowledgeSourceKind_KNOWLEDGE_SOURCE_KIND_ATTACHMENT, Status: agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_READY,
		MediaType: "text/plain", SizeBytes: 42, ContentSha256: grpcKnowledgeContent, ChunkCount: 1, Revision: 3,
		CreatedAt: timestamppb.New(now), UpdatedAt: timestamppb.New(now), Title: "Runbook",
	}
}

func agentKnowledgeDigest(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest[:])
}
