package agentgrpc

import (
	"context"
	"errors"
	"time"

	agentv1 "github.com/YingSuiAI/dirextalk-agent/api/gen/dirextalk/agent/v1"
	agentmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (runner *Runner) GetKnowledgeState(ctx context.Context, selector string) (agentmodule.KnowledgeState, error) {
	if runner == nil || runner.knowledge == nil {
		return agentmodule.KnowledgeState{}, agentmodule.ErrKnowledgeUnavailable
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	state, _, err := runner.loadKnowledgeState(callContext, selector)
	return state, err
}

func (runner *Runner) UpdateKnowledgeConfig(ctx context.Context, request agentmodule.KnowledgeConfigUpdate) (agentmodule.KnowledgeState, error) {
	if runner == nil || runner.knowledge == nil {
		return agentmodule.KnowledgeState{}, agentmodule.ErrKnowledgeUnavailable
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	defer cancel()
	state, current, err := runner.loadKnowledgeState(callContext, "")
	if err != nil {
		return agentmodule.KnowledgeState{}, err
	}
	if current == nil || state.Config == nil {
		return agentmodule.KnowledgeState{}, agentmodule.ErrKnowledgeNotConfigured
	}
	spec := proto.Clone(current.GetSpec()).(*agentv1.KnowledgeConfigSpec)
	spec.Enabled = request.Enabled
	response, err := runner.knowledge.PutKnowledgeConfig(callContext, &agentv1.PutKnowledgeConfigRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: current.GetBindingId(),
		Spec: spec, ExpectedRevision: request.ExpectedRevision,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeState{}, mapKnowledgeRPCError(callContext, err)
	}
	updated, err := mapKnowledgeConfig(response.GetConfig(), runner.ownerID, current.GetBindingId())
	if err != nil || updated.Revision != request.ExpectedRevision+1 || updated.Enabled != request.Enabled || !sameKnowledgeConfigIdentity(current.GetSpec(), response.GetConfig().GetSpec()) {
		return agentmodule.KnowledgeState{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	if !updated.CreatedAt.Equal(state.Config.CreatedAt) {
		return agentmodule.KnowledgeState{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	state.Config = &updated
	if err := agentmodule.ValidateKnowledgeState(state); err != nil {
		return agentmodule.KnowledgeState{}, err
	}
	return state, nil
}

func (runner *Runner) ListKnowledgeSources(ctx context.Context, request agentmodule.KnowledgeSourceList) (agentmodule.KnowledgeSourcePage, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeSourcePage{}, err
	}
	defer cancel()
	response, err := runner.knowledge.ListKnowledgeSources(callContext, &agentv1.ListKnowledgeSourcesRequest{
		OwnerId: runner.ownerID, BindingId: config.GetBindingId(), PageSize: int32(request.PageSize), PageToken: request.PageToken,
	})
	if err != nil {
		return agentmodule.KnowledgeSourcePage{}, mapKnowledgeRPCError(callContext, err)
	}
	page := agentmodule.KnowledgeSourcePage{Sources: make([]agentmodule.KnowledgeSource, 0, len(response.GetSources())), NextPageToken: response.GetNextPageToken()}
	for _, value := range response.GetSources() {
		source, mapErr := mapKnowledgeSource(value, runner.ownerID, config.GetBindingId())
		if mapErr != nil {
			return agentmodule.KnowledgeSourcePage{}, mapErr
		}
		page.Sources = append(page.Sources, source)
	}
	return page, nil
}

func (runner *Runner) StartKnowledgeUpload(ctx context.Context, request agentmodule.KnowledgeUploadStart) (agentmodule.KnowledgeUpload, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeUpload{}, err
	}
	defer cancel()
	response, err := runner.knowledge.StartKnowledgeAttachmentUpload(callContext, &agentv1.StartKnowledgeAttachmentUploadRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: config.GetBindingId(), SourceId: request.SourceID,
		UploadId: request.UploadID, MediaType: request.MediaType, DeclaredSizeBytes: request.Size,
		ExpectedBindingRevision: request.ExpectedBindingRevision, Title: request.Title,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeUpload{}, mapKnowledgeRPCError(callContext, err)
	}
	upload, err := mapKnowledgeUpload(response.GetUpload(), runner.ownerID, config.GetBindingId())
	if err != nil || upload.SourceID != request.SourceID || upload.UploadID != request.UploadID || upload.Status != "receiving" ||
		upload.MediaType != request.MediaType || upload.DeclaredSize != request.Size || upload.ReceivedSize != 0 || upload.NextOrdinal != 0 ||
		upload.Revision != 1 || upload.BindingRevision != request.ExpectedBindingRevision {
		return agentmodule.KnowledgeUpload{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return upload, nil
}

func (runner *Runner) AppendKnowledgeUploadChunk(ctx context.Context, request agentmodule.KnowledgeUploadChunk) (agentmodule.KnowledgeUpload, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeUpload{}, err
	}
	defer cancel()
	chunk := append([]byte(nil), request.Chunk...)
	defer clear(chunk)
	response, err := runner.knowledge.AppendKnowledgeAttachmentChunk(callContext, &agentv1.AppendKnowledgeAttachmentChunkRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: config.GetBindingId(), UploadId: request.UploadID,
		ExpectedUploadRevision: request.ExpectedRevision, OffsetBytes: request.Offset, ChunkOrdinal: request.Ordinal,
		Chunk: chunk, ChunkSha256: request.ChunkSHA256,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeUpload{}, mapKnowledgeRPCError(callContext, err)
	}
	upload, err := mapKnowledgeUpload(response.GetUpload(), runner.ownerID, config.GetBindingId())
	if err != nil || upload.UploadID != request.UploadID || upload.Status != "receiving" || upload.Revision != request.ExpectedRevision+1 ||
		upload.ReceivedSize != request.Offset+int64(len(request.Chunk)) || upload.NextOrdinal != request.Ordinal+1 {
		return agentmodule.KnowledgeUpload{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return upload, nil
}

func (runner *Runner) FinishKnowledgeUpload(ctx context.Context, request agentmodule.KnowledgeUploadFinish) (agentmodule.KnowledgeUploadResult, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeUploadResult{}, err
	}
	defer cancel()
	response, err := runner.knowledge.CommitKnowledgeAttachmentUpload(callContext, &agentv1.CommitKnowledgeAttachmentUploadRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: config.GetBindingId(), UploadId: request.UploadID,
		ExpectedUploadRevision: request.ExpectedRevision, ContentSha256: request.ContentSHA256,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeUploadResult{}, mapKnowledgeRPCError(callContext, err)
	}
	upload, err := mapKnowledgeUpload(response.GetUpload(), runner.ownerID, config.GetBindingId())
	if err != nil {
		return agentmodule.KnowledgeUploadResult{}, err
	}
	source, err := mapKnowledgeSource(response.GetSource(), runner.ownerID, config.GetBindingId())
	if err != nil {
		return agentmodule.KnowledgeUploadResult{}, err
	}
	if upload.UploadID != request.UploadID || upload.Status != "committed" || upload.Revision != request.ExpectedRevision+1 ||
		source.SourceID != upload.SourceID || source.Status != "ready" ||
		source.ContentSHA256 != request.ContentSHA256 || source.Size != upload.DeclaredSize || source.ChunkCount != upload.NextOrdinal {
		return agentmodule.KnowledgeUploadResult{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return agentmodule.KnowledgeUploadResult{Upload: upload, Source: source}, nil
}

func (runner *Runner) CreateKnowledgeMemory(ctx context.Context, request agentmodule.KnowledgeMemoryCreate) (agentmodule.KnowledgeSource, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeSource{}, err
	}
	defer cancel()
	content := append([]byte(nil), request.Content...)
	defer clear(content)
	response, err := runner.knowledge.CreateKnowledgeMemory(callContext, &agentv1.CreateKnowledgeMemoryRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: config.GetBindingId(), SourceId: request.SourceID,
		ExpectedBindingRevision: request.ExpectedBindingRevision, Content: content, ContentSha256: request.ContentSHA256, Title: request.Title,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeSource{}, mapKnowledgeRPCError(callContext, err)
	}
	source, err := mapKnowledgeSource(response.GetSource(), runner.ownerID, config.GetBindingId())
	if err != nil || source.SourceID != request.SourceID || source.Kind != "memory" || source.Status != "ready" ||
		source.Title != request.Title || source.Size != int64(len(request.Content)) || source.ContentSHA256 != request.ContentSHA256 {
		return agentmodule.KnowledgeSource{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return source, nil
}

func (runner *Runner) DeleteKnowledgeSource(ctx context.Context, request agentmodule.KnowledgeSourceDelete) (agentmodule.KnowledgeSource, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeSource{}, err
	}
	defer cancel()
	response, err := runner.knowledge.DeleteKnowledgeSource(callContext, &agentv1.DeleteKnowledgeSourceRequest{
		IdempotencyKey: request.IdempotencyKey, OwnerId: runner.ownerID, BindingId: config.GetBindingId(), SourceId: request.SourceID,
		ExpectedBindingRevision: request.ExpectedBindingRevision, ExpectedSourceRevision: request.ExpectedSourceRevision,
	}, grpc.MaxRetryRPCBufferSize(0))
	if err != nil {
		return agentmodule.KnowledgeSource{}, mapKnowledgeRPCError(callContext, err)
	}
	source, err := mapKnowledgeSource(response.GetSource(), runner.ownerID, config.GetBindingId())
	if err != nil || source.SourceID != request.SourceID || source.Status != "deleted" || source.Revision != request.ExpectedSourceRevision+1 {
		return agentmodule.KnowledgeSource{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return source, nil
}

func (runner *Runner) SearchKnowledge(ctx context.Context, request agentmodule.KnowledgeSearch) (agentmodule.KnowledgeSearchResult, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeSearchResult{}, err
	}
	defer cancel()
	response, err := runner.knowledge.SearchKnowledge(callContext, &agentv1.SearchKnowledgeRequest{
		OwnerId: runner.ownerID, BindingId: config.GetBindingId(), ExpectedBindingRevision: config.GetRevision(),
		Query: request.Query, Limit: int32(request.Limit), SourceIds: append([]string(nil), request.SourceIDs...),
	})
	if err != nil {
		return agentmodule.KnowledgeSearchResult{}, mapKnowledgeRPCError(callContext, err)
	}
	result := agentmodule.KnowledgeSearchResult{BindingRevision: response.GetBindingRevision(), Matches: make([]agentmodule.KnowledgeSearchMatch, 0, len(response.GetMatches()))}
	for _, match := range response.GetMatches() {
		if match == nil {
			return agentmodule.KnowledgeSearchResult{}, agentmodule.ErrInvalidKnowledgeResponse
		}
		result.Matches = append(result.Matches, agentmodule.KnowledgeSearchMatch{SourceID: match.GetSourceId(), ChunkRef: match.GetChunkRef(), Score: match.GetScore()})
	}
	if err := agentmodule.ValidateKnowledgeSearchResult(result, request.Limit, request.SourceIDs); err != nil || result.BindingRevision != config.GetRevision() {
		return agentmodule.KnowledgeSearchResult{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return result, nil
}

func (runner *Runner) GetKnowledgeStatus(ctx context.Context) (agentmodule.KnowledgeStatus, error) {
	callContext, cancel, config, err := runner.knowledgeOperationContext(ctx)
	if err != nil {
		return agentmodule.KnowledgeStatus{}, err
	}
	defer cancel()
	response, err := runner.knowledge.GetKnowledgeStatus(callContext, &agentv1.GetKnowledgeStatusRequest{OwnerId: runner.ownerID, BindingId: config.GetBindingId()})
	if err != nil {
		return agentmodule.KnowledgeStatus{}, mapKnowledgeRPCError(callContext, err)
	}
	value := response.GetStatus()
	if value == nil || value.GetOwnerId() != runner.ownerID || value.GetBindingId() != config.GetBindingId() {
		return agentmodule.KnowledgeStatus{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	checkedAt, err := mapKnowledgeTime(value.GetCheckedAt())
	if err != nil {
		return agentmodule.KnowledgeStatus{}, err
	}
	mapped := agentmodule.KnowledgeStatus{
		BindingID: value.GetBindingId(), Enabled: value.GetEnabled(), BackendStatus: knowledgeBackendStatus(value.GetBackendStatus()),
		ReadySourceCount: int(value.GetReadySourceCount()), UploadingSourceCount: int(value.GetUploadingSourceCount()),
		FailedSourceCount: int(value.GetFailedSourceCount()), BindingRevision: value.GetBindingRevision(), CheckedAt: checkedAt,
	}
	if err := agentmodule.ValidateKnowledgeStatus(mapped); err != nil || mapped.BindingRevision != config.GetRevision() || mapped.Enabled != config.GetSpec().GetEnabled() {
		return agentmodule.KnowledgeStatus{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return mapped, nil
}

func (runner *Runner) knowledgeOperationContext(ctx context.Context) (context.Context, context.CancelFunc, *agentv1.KnowledgeConfig, error) {
	if runner == nil || runner.knowledge == nil {
		return nil, func() {}, nil, agentmodule.ErrKnowledgeUnavailable
	}
	callContext, cancel := context.WithTimeout(ctx, runner.chainTimeout)
	config, err := runner.loadKnowledgeConfig(callContext, "")
	if err != nil {
		cancel()
		return nil, func() {}, nil, err
	}
	return callContext, cancel, config, nil
}

func (runner *Runner) loadKnowledgeState(ctx context.Context, selector string) (agentmodule.KnowledgeState, *agentv1.KnowledgeConfig, error) {
	capabilitiesResponse, err := runner.knowledge.GetKnowledgeCapabilities(ctx, &agentv1.GetKnowledgeCapabilitiesRequest{OwnerId: runner.ownerID})
	if err != nil {
		return agentmodule.KnowledgeState{}, nil, mapKnowledgeRPCError(ctx, err)
	}
	capabilities := capabilitiesResponse.GetCapabilities()
	if capabilities == nil {
		return agentmodule.KnowledgeState{}, nil, agentmodule.ErrInvalidKnowledgeResponse
	}
	state := agentmodule.KnowledgeState{Available: true, Capabilities: agentmodule.KnowledgeCapabilities{
		Config: capabilities.GetConfig(), AttachmentUpload: capabilities.GetAttachmentUpload(), Memory: capabilities.GetMemory(), Search: capabilities.GetSearch(),
		EmbeddingProfileIDs: append([]string(nil), capabilities.GetEmbeddingProfileIds()...), MaxAttachmentSize: capabilities.GetMaxAttachmentSizeBytes(),
		MaxAttachmentChunkSize: int(capabilities.GetMaxAttachmentChunkBytes()), MaxSearchResults: int(capabilities.GetMaxSearchResults()),
	}}
	config, err := runner.loadKnowledgeConfig(ctx, selector)
	if errors.Is(err, agentmodule.ErrKnowledgeNotConfigured) {
		if validateErr := agentmodule.ValidateKnowledgeState(state); validateErr != nil {
			return agentmodule.KnowledgeState{}, nil, validateErr
		}
		return state, nil, nil
	}
	if err != nil {
		return agentmodule.KnowledgeState{}, nil, err
	}
	mapped, err := mapKnowledgeConfig(config, runner.ownerID, selector)
	if err != nil {
		return agentmodule.KnowledgeState{}, nil, err
	}
	state.Config = &mapped
	if err := agentmodule.ValidateKnowledgeState(state); err != nil {
		return agentmodule.KnowledgeState{}, nil, err
	}
	return state, config, nil
}

func (runner *Runner) loadKnowledgeConfig(ctx context.Context, selector string) (*agentv1.KnowledgeConfig, error) {
	response, err := runner.knowledge.GetKnowledgeConfig(ctx, &agentv1.GetKnowledgeConfigRequest{OwnerId: runner.ownerID, BindingId: selector})
	if err != nil {
		mapped := mapKnowledgeRPCError(ctx, err)
		if selector == "" && errors.Is(mapped, agentmodule.ErrKnowledgeNotFound) {
			return nil, agentmodule.ErrKnowledgeNotConfigured
		}
		return nil, mapped
	}
	config := response.GetConfig()
	mapped, err := mapKnowledgeConfig(config, runner.ownerID, selector)
	if err != nil {
		return nil, err
	}
	if agentmodule.ValidateKnowledgeConfig(mapped) != nil {
		return nil, agentmodule.ErrInvalidKnowledgeResponse
	}
	return config, nil
}

func mapKnowledgeConfig(config *agentv1.KnowledgeConfig, ownerID, expectedBindingID string) (agentmodule.KnowledgeConfig, error) {
	if config == nil || config.GetSpec() == nil || config.GetOwnerId() != ownerID || (expectedBindingID != "" && config.GetBindingId() != expectedBindingID) {
		return agentmodule.KnowledgeConfig{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	createdAt, err := mapKnowledgeTime(config.GetCreatedAt())
	if err != nil {
		return agentmodule.KnowledgeConfig{}, err
	}
	updatedAt, err := mapKnowledgeTime(config.GetUpdatedAt())
	if err != nil {
		return agentmodule.KnowledgeConfig{}, err
	}
	return agentmodule.KnowledgeConfig{
		BindingID: config.GetBindingId(), DeploymentID: config.GetSpec().GetDeploymentId(), ManagedServiceID: config.GetSpec().GetManagedServiceId(),
		RecipeDigest: config.GetSpec().GetRecipeDigest(), EmbeddingProfileID: config.GetSpec().GetEmbeddingProfileId(), Enabled: config.GetSpec().GetEnabled(),
		Revision: config.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt,
	}, nil
}

func mapKnowledgeSource(value *agentv1.KnowledgeSource, ownerID, bindingID string) (agentmodule.KnowledgeSource, error) {
	if value == nil || value.GetOwnerId() != ownerID || value.GetBindingId() != bindingID {
		return agentmodule.KnowledgeSource{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	createdAt, err := mapKnowledgeTime(value.GetCreatedAt())
	if err != nil {
		return agentmodule.KnowledgeSource{}, err
	}
	updatedAt, err := mapKnowledgeTime(value.GetUpdatedAt())
	if err != nil {
		return agentmodule.KnowledgeSource{}, err
	}
	mapped := agentmodule.KnowledgeSource{
		BindingID: value.GetBindingId(), SourceID: value.GetSourceId(), Title: value.GetTitle(), Kind: knowledgeSourceKind(value.GetKind()),
		Status: knowledgeSourceStatus(value.GetStatus()), MediaType: value.GetMediaType(), Size: value.GetSizeBytes(), ContentSHA256: value.GetContentSha256(),
		ChunkCount: value.GetChunkCount(), Revision: value.GetRevision(), CreatedAt: createdAt, UpdatedAt: updatedAt, Error: value.GetErrorCode(),
	}
	if err := agentmodule.ValidateKnowledgeSource(mapped); err != nil {
		return agentmodule.KnowledgeSource{}, err
	}
	return mapped, nil
}

func mapKnowledgeUpload(value *agentv1.KnowledgeAttachmentUpload, ownerID, bindingID string) (agentmodule.KnowledgeUpload, error) {
	if value == nil || value.GetOwnerId() != ownerID || value.GetBindingId() != bindingID {
		return agentmodule.KnowledgeUpload{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	createdAt, err := mapKnowledgeTime(value.GetCreatedAt())
	if err != nil {
		return agentmodule.KnowledgeUpload{}, err
	}
	updatedAt, err := mapKnowledgeTime(value.GetUpdatedAt())
	if err != nil {
		return agentmodule.KnowledgeUpload{}, err
	}
	mapped := agentmodule.KnowledgeUpload{
		BindingID: value.GetBindingId(), SourceID: value.GetSourceId(), UploadID: value.GetUploadId(), Status: knowledgeUploadStatus(value.GetStatus()),
		MediaType: value.GetMediaType(), DeclaredSize: value.GetDeclaredSizeBytes(), ReceivedSize: value.GetReceivedSizeBytes(),
		NextOrdinal: value.GetNextChunkOrdinal(), Revision: value.GetRevision(), BindingRevision: value.GetBindingRevision(),
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if err := agentmodule.ValidateKnowledgeUpload(mapped); err != nil {
		return agentmodule.KnowledgeUpload{}, err
	}
	return mapped, nil
}

func mapKnowledgeTime(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || value.CheckValid() != nil {
		return time.Time{}, agentmodule.ErrInvalidKnowledgeResponse
	}
	return value.AsTime().UTC(), nil
}

func sameKnowledgeConfigIdentity(left, right *agentv1.KnowledgeConfigSpec) bool {
	if left == nil || right == nil {
		return false
	}
	leftCopy := proto.Clone(left).(*agentv1.KnowledgeConfigSpec)
	rightCopy := proto.Clone(right).(*agentv1.KnowledgeConfigSpec)
	leftCopy.Enabled = false
	rightCopy.Enabled = false
	return proto.Equal(leftCopy, rightCopy)
}

func knowledgeSourceKind(value agentv1.KnowledgeSourceKind) string {
	switch value {
	case agentv1.KnowledgeSourceKind_KNOWLEDGE_SOURCE_KIND_ATTACHMENT:
		return "attachment"
	case agentv1.KnowledgeSourceKind_KNOWLEDGE_SOURCE_KIND_MEMORY:
		return "memory"
	default:
		return ""
	}
}

func knowledgeSourceStatus(value agentv1.KnowledgeSourceStatus) string {
	switch value {
	case agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_UPLOADING:
		return "uploading"
	case agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_READY:
		return "ready"
	case agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_DELETING:
		return "deleting"
	case agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_DELETED:
		return "deleted"
	case agentv1.KnowledgeSourceStatus_KNOWLEDGE_SOURCE_STATUS_FAILED:
		return "failed"
	default:
		return ""
	}
}

func knowledgeUploadStatus(value agentv1.KnowledgeUploadStatus) string {
	switch value {
	case agentv1.KnowledgeUploadStatus_KNOWLEDGE_UPLOAD_STATUS_RECEIVING:
		return "receiving"
	case agentv1.KnowledgeUploadStatus_KNOWLEDGE_UPLOAD_STATUS_COMMITTED:
		return "committed"
	case agentv1.KnowledgeUploadStatus_KNOWLEDGE_UPLOAD_STATUS_FAILED:
		return "failed"
	default:
		return ""
	}
}

func knowledgeBackendStatus(value agentv1.KnowledgeBackendStatus) string {
	switch value {
	case agentv1.KnowledgeBackendStatus_KNOWLEDGE_BACKEND_STATUS_UNAVAILABLE:
		return "unavailable"
	case agentv1.KnowledgeBackendStatus_KNOWLEDGE_BACKEND_STATUS_READY:
		return "ready"
	case agentv1.KnowledgeBackendStatus_KNOWLEDGE_BACKEND_STATUS_DEGRADED:
		return "degraded"
	default:
		return ""
	}
}

func mapKnowledgeRPCError(ctx context.Context, err error) error {
	if ctx == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return agentmodule.ErrKnowledgeUnavailable
	}
	switch status.Code(err) {
	case codes.NotFound:
		return agentmodule.ErrKnowledgeNotFound
	case codes.Aborted, codes.AlreadyExists:
		return agentmodule.ErrKnowledgeConflict
	case codes.FailedPrecondition:
		return agentmodule.ErrKnowledgeState
	case codes.Unavailable, codes.Unimplemented, codes.DeadlineExceeded, codes.Canceled, codes.Unauthenticated, codes.PermissionDenied:
		return agentmodule.ErrKnowledgeUnavailable
	default:
		return agentmodule.ErrInvalidKnowledgeResponse
	}
}

var _ agentmodule.KnowledgeClient = (*Runner)(nil)
