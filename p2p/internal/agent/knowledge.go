package agent

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	cloudmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloud"
	"github.com/google/uuid"
)

const (
	actionKnowledgeConfigGet     = "agent.knowledge.config.get"
	actionKnowledgeConfigUpdate  = "agent.knowledge.config.update"
	actionKnowledgeSourcesList   = "agent.knowledge.sources.list"
	actionKnowledgeSourcesDelete = "agent.knowledge.sources.delete"
	actionKnowledgeUploadStart   = "agent.knowledge.upload.start"
	actionKnowledgeUploadChunk   = "agent.knowledge.upload.chunk"
	actionKnowledgeUploadFinish  = "agent.knowledge.upload.finish"
	actionKnowledgeMemoryCreate  = "agent.knowledge.memory.create"
	actionKnowledgeSearch        = "agent.knowledge.search"
	actionKnowledgeStatus        = "agent.knowledge.status"

	maxKnowledgeAttachmentBytes  int64 = 64 << 20
	maxKnowledgeChunkBytes             = 256 << 10
	maxKnowledgeAttachmentChunks       = 256
	maxKnowledgeMemoryBytes            = 1 << 20
	maxKnowledgeSearchQueryBytes       = 16 << 10
	maxKnowledgeSearchResults          = 50
	maxKnowledgeSourceIDs              = 32
	maxKnowledgeSourceTitleBytes       = 255
	maxKnowledgeListPageSize           = 100
)

var (
	ErrKnowledgeUnavailable     = errors.New("Agent Knowledge is unavailable")
	ErrKnowledgeNotConfigured   = errors.New("Knowledge is not configured")
	ErrKnowledgeNotFound        = errors.New("Knowledge entity was not found")
	ErrKnowledgeConflict        = errors.New("Knowledge revision conflicts")
	ErrKnowledgeState           = errors.New("Knowledge state does not allow the operation")
	ErrInvalidKnowledgeResponse = errors.New("Agent Knowledge response is invalid")
	knowledgeDigestPattern      = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	knowledgeChunkRefPattern    = regexp.MustCompile(`^chunk:[A-Za-z0-9][A-Za-z0-9._-]{0,479}$`)
)

// KnowledgeCapabilities is the bounded, de-secreted Agent capability
// projection. Provider endpoints, model names, dimensions and credentials are
// deliberately not representable.
type KnowledgeCapabilities struct {
	Config                 bool
	AttachmentUpload       bool
	Memory                 bool
	Search                 bool
	EmbeddingProfileIDs    []string
	MaxAttachmentSize      int64
	MaxAttachmentChunkSize int
	MaxSearchResults       int
}

type KnowledgeConfig struct {
	BindingID          string
	DeploymentID       string
	ManagedServiceID   string
	RecipeDigest       string
	EmbeddingProfileID string
	Enabled            bool
	Revision           int64
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

type KnowledgeState struct {
	Available    bool
	Capabilities KnowledgeCapabilities
	Config       *KnowledgeConfig
}

type KnowledgeConfigUpdate struct {
	IdempotencyKey   string
	ExpectedRevision int64
	Enabled          bool
}

type KnowledgeSource struct {
	BindingID     string
	SourceID      string
	Title         string
	Kind          string
	Status        string
	MediaType     string
	Size          int64
	ContentSHA256 string
	ChunkCount    int32
	Revision      int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Error         string
}

type KnowledgeSourceList struct {
	PageSize  int
	PageToken string
}

type KnowledgeSourcePage struct {
	Sources       []KnowledgeSource
	NextPageToken string
}

type KnowledgeSourceDelete struct {
	IdempotencyKey          string
	SourceID                string
	ExpectedBindingRevision int64
	ExpectedSourceRevision  int64
}

type KnowledgeUploadStart struct {
	IdempotencyKey          string
	SourceID                string
	UploadID                string
	Title                   string
	MediaType               string
	Size                    int64
	ExpectedBindingRevision int64
}

type KnowledgeUploadChunk struct {
	IdempotencyKey   string
	UploadID         string
	ExpectedRevision int64
	Offset           int64
	Ordinal          int32
	Chunk            []byte
	ChunkSHA256      string
}

type KnowledgeUploadFinish struct {
	IdempotencyKey   string
	UploadID         string
	ExpectedRevision int64
	ContentSHA256    string
}

type KnowledgeUpload struct {
	BindingID       string
	SourceID        string
	UploadID        string
	Status          string
	MediaType       string
	DeclaredSize    int64
	ReceivedSize    int64
	NextOrdinal     int32
	Revision        int64
	BindingRevision int64
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type KnowledgeUploadResult struct {
	Upload KnowledgeUpload
	Source KnowledgeSource
}

type KnowledgeMemoryCreate struct {
	IdempotencyKey          string
	SourceID                string
	Title                   string
	Content                 []byte
	ContentSHA256           string
	ExpectedBindingRevision int64
}

type KnowledgeSearch struct {
	Query     string
	Limit     int
	SourceIDs []string
}

type KnowledgeSearchMatch struct {
	SourceID string
	ChunkRef string
	Score    float64
}

type KnowledgeSearchResult struct {
	Matches         []KnowledgeSearchMatch
	BindingRevision int64
}

type KnowledgeStatus struct {
	BindingID            string
	Enabled              bool
	BackendStatus        string
	ReadySourceCount     int
	UploadingSourceCount int
	FailedSourceCount    int
	BindingRevision      int64
	CheckedAt            time.Time
}

// KnowledgeClient is the complete typed independent-Agent capability consumed
// by ProductCore. Implementations bind the trusted owner and resolve the
// owner's exactly-one Agent configuration before every operation.
type KnowledgeClient interface {
	GetKnowledgeState(context.Context, string) (KnowledgeState, error)
	UpdateKnowledgeConfig(context.Context, KnowledgeConfigUpdate) (KnowledgeState, error)
	ListKnowledgeSources(context.Context, KnowledgeSourceList) (KnowledgeSourcePage, error)
	DeleteKnowledgeSource(context.Context, KnowledgeSourceDelete) (KnowledgeSource, error)
	StartKnowledgeUpload(context.Context, KnowledgeUploadStart) (KnowledgeUpload, error)
	AppendKnowledgeUploadChunk(context.Context, KnowledgeUploadChunk) (KnowledgeUpload, error)
	FinishKnowledgeUpload(context.Context, KnowledgeUploadFinish) (KnowledgeUploadResult, error)
	CreateKnowledgeMemory(context.Context, KnowledgeMemoryCreate) (KnowledgeSource, error)
	SearchKnowledge(context.Context, KnowledgeSearch) (KnowledgeSearchResult, error)
	GetKnowledgeStatus(context.Context) (KnowledgeStatus, error)
}

func (m *Module) knowledgeHandlers() map[string]actionbase.Handler {
	return map[string]actionbase.Handler{
		actionKnowledgeConfigGet:     m.getKnowledgeConfig,
		actionKnowledgeConfigUpdate:  m.updateKnowledgeConfig,
		actionKnowledgeSourcesList:   m.listKnowledgeSources,
		actionKnowledgeSourcesDelete: m.deleteKnowledgeSource,
		actionKnowledgeUploadStart:   m.startKnowledgeUpload,
		actionKnowledgeUploadChunk:   m.appendKnowledgeUploadChunk,
		actionKnowledgeUploadFinish:  m.finishKnowledgeUpload,
		actionKnowledgeMemoryCreate:  m.createKnowledgeMemory,
		actionKnowledgeSearch:        m.searchKnowledge,
		actionKnowledgeStatus:        m.getKnowledgeStatus,
	}
}

func (m *Module) getKnowledgeConfig(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "binding_id"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge configuration request")
	}
	selector := ""
	if value, present := params["binding_id"]; present {
		parsed, err := requiredKnowledgeUUID(value)
		if err != nil {
			return nil, actionbase.BadRequest("invalid Knowledge configuration request")
		}
		selector = parsed
	}
	state, err := m.knowledge.GetKnowledgeState(ctx, selector)
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeState(state); err != nil {
		return nil, knowledgeActionError(err)
	}
	return state.Response(), nil
}

func (m *Module) updateKnowledgeConfig(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "expected_revision", "enabled"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge configuration update")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	revision, revisionErr := requiredPositiveKnowledgeInt(params["expected_revision"])
	enabled, enabledOK := params["enabled"].(bool)
	if keyErr != nil || revisionErr != nil || !enabledOK {
		return nil, actionbase.BadRequest("invalid Knowledge configuration update")
	}
	state, err := m.knowledge.UpdateKnowledgeConfig(ctx, KnowledgeConfigUpdate{IdempotencyKey: key, ExpectedRevision: revision, Enabled: enabled})
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeState(state); err != nil || state.Config == nil || state.Config.Revision != revision+1 || state.Config.Enabled != enabled {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return state.Response(), nil
}

func (m *Module) listKnowledgeSources(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "page_size", "page_token"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge source list request")
	}
	request := KnowledgeSourceList{}
	if value, present := params["page_size"]; present {
		parsed, err := exactNonnegativeInt64(value)
		if err != nil || parsed > maxKnowledgeListPageSize {
			return nil, actionbase.BadRequest("invalid Knowledge source list request")
		}
		request.PageSize = int(parsed)
	}
	if value, present := params["page_token"]; present {
		parsed, ok := value.(string)
		if !ok || strings.TrimSpace(parsed) != parsed || len(parsed) > 128 || strings.ContainsAny(parsed, "\r\n\t ") {
			return nil, actionbase.BadRequest("invalid Knowledge source list request")
		}
		request.PageToken = parsed
	}
	page, err := m.knowledge.ListKnowledgeSources(ctx, request)
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeSourcePage(page); err != nil {
		return nil, knowledgeActionError(err)
	}
	sources := make([]map[string]any, 0, len(page.Sources))
	for _, source := range page.Sources {
		sources = append(sources, source.Response())
	}
	return map[string]any{"sources": sources, "next_page_token": page.NextPageToken}, nil
}

func (m *Module) deleteKnowledgeSource(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "source_id", "expected_binding_revision", "expected_source_revision"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge source delete request")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	sourceID, sourceErr := requiredKnowledgeUUID(params["source_id"])
	bindingRevision, bindingErr := requiredPositiveKnowledgeInt(params["expected_binding_revision"])
	sourceRevision, revisionErr := requiredPositiveKnowledgeInt(params["expected_source_revision"])
	if keyErr != nil || sourceErr != nil || bindingErr != nil || revisionErr != nil {
		return nil, actionbase.BadRequest("invalid Knowledge source delete request")
	}
	source, err := m.knowledge.DeleteKnowledgeSource(ctx, KnowledgeSourceDelete{
		IdempotencyKey: key, SourceID: sourceID, ExpectedBindingRevision: bindingRevision, ExpectedSourceRevision: sourceRevision,
	})
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeSource(source); err != nil || source.SourceID != sourceID || source.Status != "deleted" || source.Revision != sourceRevision+1 {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return source.Response(), nil
}

func (m *Module) startKnowledgeUpload(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "source_id", "title", "mime_type", "size", "expected_binding_revision"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge upload start request")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	title, titleErr := requiredKnowledgeText(params["title"], maxKnowledgeSourceTitleBytes)
	mediaType, mediaErr := requiredKnowledgeMediaType(params["mime_type"])
	size, sizeErr := requiredPositiveKnowledgeInt(params["size"])
	bindingRevision, bindingErr := requiredPositiveKnowledgeInt(params["expected_binding_revision"])
	if keyErr != nil || titleErr != nil || mediaErr != nil || sizeErr != nil || size > maxKnowledgeAttachmentBytes || bindingErr != nil {
		return nil, actionbase.BadRequest("invalid Knowledge upload start request")
	}
	parsedKey, _ := uuid.Parse(key)
	sourceID := uuid.NewSHA1(parsedKey, []byte("dirextalk-knowledge-source/v1")).String()
	if value, present := params["source_id"]; present {
		var err error
		sourceID, err = requiredKnowledgeUUID(value)
		if err != nil {
			return nil, actionbase.BadRequest("invalid Knowledge upload start request")
		}
	}
	uploadID := uuid.NewSHA1(parsedKey, []byte("dirextalk-knowledge-upload/v1")).String()
	request := KnowledgeUploadStart{
		IdempotencyKey: key, SourceID: sourceID, UploadID: uploadID, Title: title, MediaType: mediaType, Size: size,
		ExpectedBindingRevision: bindingRevision,
	}
	upload, err := m.knowledge.StartKnowledgeUpload(ctx, request)
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeUpload(upload); err != nil || upload.SourceID != sourceID || upload.UploadID != uploadID || upload.Status != "receiving" ||
		upload.MediaType != mediaType || upload.DeclaredSize != size || upload.ReceivedSize != 0 || upload.NextOrdinal != 0 || upload.Revision != 1 ||
		upload.BindingRevision != bindingRevision {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return upload.Response(), nil
}

func (m *Module) appendKnowledgeUploadChunk(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "upload_id", "expected_revision", "offset", "ordinal", "data"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge upload chunk request")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	uploadID, uploadErr := requiredKnowledgeUUID(params["upload_id"])
	revision, revisionErr := requiredPositiveKnowledgeInt(params["expected_revision"])
	offset, offsetErr := exactNonnegativeInt64(params["offset"])
	ordinal, ordinalErr := exactNonnegativeInt64(params["ordinal"])
	encoded, encodedOK := params["data"].(string)
	if keyErr != nil || uploadErr != nil || revisionErr != nil || offsetErr != nil || offset > maxKnowledgeAttachmentBytes || ordinalErr != nil || ordinal >= maxKnowledgeAttachmentChunks ||
		!encodedOK || encoded == "" || len(encoded) > base64.StdEncoding.EncodedLen(maxKnowledgeChunkBytes) {
		return nil, actionbase.BadRequest("invalid Knowledge upload chunk request")
	}
	chunk, decodeErr := base64.StdEncoding.Strict().DecodeString(encoded)
	if decodeErr != nil || len(chunk) == 0 || len(chunk) > maxKnowledgeChunkBytes ||
		int64(len(chunk)) > maxKnowledgeAttachmentBytes-offset || base64.StdEncoding.EncodeToString(chunk) != encoded {
		clear(chunk)
		return nil, actionbase.BadRequest("invalid Knowledge upload chunk request")
	}
	defer clear(chunk)
	request := KnowledgeUploadChunk{
		IdempotencyKey: key, UploadID: uploadID, ExpectedRevision: revision, Offset: offset, Ordinal: int32(ordinal),
		Chunk: chunk, ChunkSHA256: digestKnowledgeBytes(chunk),
	}
	upload, err := m.knowledge.AppendKnowledgeUploadChunk(ctx, request)
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeUpload(upload); err != nil || upload.UploadID != uploadID || upload.Status != "receiving" || upload.Revision != revision+1 ||
		upload.ReceivedSize != offset+int64(len(chunk)) || upload.NextOrdinal != int32(ordinal)+1 {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return upload.Response(), nil
}

func (m *Module) finishKnowledgeUpload(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "upload_id", "expected_revision", "content_sha256"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge upload finish request")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	uploadID, uploadErr := requiredKnowledgeUUID(params["upload_id"])
	revision, revisionErr := requiredPositiveKnowledgeInt(params["expected_revision"])
	digest, digestErr := requiredKnowledgeDigest(params["content_sha256"])
	if keyErr != nil || uploadErr != nil || revisionErr != nil || digestErr != nil {
		return nil, actionbase.BadRequest("invalid Knowledge upload finish request")
	}
	result, err := m.knowledge.FinishKnowledgeUpload(ctx, KnowledgeUploadFinish{IdempotencyKey: key, UploadID: uploadID, ExpectedRevision: revision, ContentSHA256: digest})
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeUpload(result.Upload); err != nil || result.Upload.UploadID != uploadID || result.Upload.Status != "committed" || result.Upload.Revision != revision+1 ||
		result.Upload.ReceivedSize != result.Upload.DeclaredSize ||
		validateKnowledgeSource(result.Source) != nil || result.Source.SourceID != result.Upload.SourceID || result.Source.Status != "ready" ||
		result.Source.ContentSHA256 != digest || result.Source.Size != result.Upload.DeclaredSize || result.Source.ChunkCount != result.Upload.NextOrdinal {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return map[string]any{"upload": result.Upload.Response(), "source": result.Source.Response()}, nil
}

func (m *Module) createKnowledgeMemory(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "idempotency_key", "source_id", "title", "text", "content_sha256", "expected_binding_revision"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge memory request")
	}
	key, keyErr := requiredKnowledgeUUID(params["idempotency_key"])
	sourceID, sourceErr := requiredKnowledgeUUID(params["source_id"])
	title, titleErr := requiredKnowledgeText(params["title"], maxKnowledgeSourceTitleBytes)
	text, textOK := params["text"].(string)
	digest, digestErr := requiredKnowledgeDigest(params["content_sha256"])
	bindingRevision, bindingErr := requiredPositiveKnowledgeInt(params["expected_binding_revision"])
	if keyErr != nil || sourceErr != nil || titleErr != nil || !textOK || len(text) == 0 || len(text) > maxKnowledgeMemoryBytes ||
		strings.ContainsRune(text, '\x00') || digestErr != nil || bindingErr != nil {
		return nil, actionbase.BadRequest("invalid Knowledge memory request")
	}
	content := []byte(text)
	defer clear(content)
	if !utf8.Valid(content) || digestKnowledgeBytes(content) != digest {
		return nil, actionbase.BadRequest("invalid Knowledge memory request")
	}
	source, err := m.knowledge.CreateKnowledgeMemory(ctx, KnowledgeMemoryCreate{
		IdempotencyKey: key, SourceID: sourceID, Title: title, Content: content, ContentSHA256: digest,
		ExpectedBindingRevision: bindingRevision,
	})
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeSource(source); err != nil || source.SourceID != sourceID || source.Kind != "memory" || source.Status != "ready" ||
		source.Title != title || source.Size != int64(len(content)) || source.ContentSHA256 != digest {
		return nil, knowledgeActionError(ErrInvalidKnowledgeResponse)
	}
	return source.Response(), nil
}

func (m *Module) searchKnowledge(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if err := rejectKnowledgeFields(params, "query", "limit", "source_ids"); err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge search request")
	}
	query, queryOK := params["query"].(string)
	query = strings.TrimSpace(query)
	if !queryOK || query == "" || len(query) > maxKnowledgeSearchQueryBytes || strings.ContainsRune(query, '\x00') {
		return nil, actionbase.BadRequest("invalid Knowledge search request")
	}
	limit := int64(10)
	if value, present := params["limit"]; present {
		var err error
		limit, err = requiredPositiveKnowledgeInt(value)
		if err != nil || limit > maxKnowledgeSearchResults {
			return nil, actionbase.BadRequest("invalid Knowledge search request")
		}
	}
	sourceIDs, err := optionalKnowledgeSourceIDs(params["source_ids"])
	if err != nil {
		return nil, actionbase.BadRequest("invalid Knowledge search request")
	}
	result, clientErr := m.knowledge.SearchKnowledge(ctx, KnowledgeSearch{Query: query, Limit: int(limit), SourceIDs: sourceIDs})
	if clientErr != nil {
		return nil, knowledgeActionError(clientErr)
	}
	if err := validateKnowledgeSearchResult(result, int(limit), sourceIDs); err != nil {
		return nil, knowledgeActionError(err)
	}
	matches := make([]map[string]any, 0, len(result.Matches))
	for _, match := range result.Matches {
		matches = append(matches, map[string]any{"source_id": match.SourceID, "chunk_ref": match.ChunkRef, "score": match.Score})
	}
	return map[string]any{"matches": matches, "binding_revision": result.BindingRevision}, nil
}

func (m *Module) getKnowledgeStatus(ctx context.Context, params map[string]any) (any, *actionbase.Error) {
	if len(params) != 0 {
		return nil, actionbase.BadRequest("Knowledge status does not accept parameters")
	}
	status, err := m.knowledge.GetKnowledgeStatus(ctx)
	if err != nil {
		return nil, knowledgeActionError(err)
	}
	if err := validateKnowledgeStatus(status); err != nil {
		return nil, knowledgeActionError(err)
	}
	return status.Response(), nil
}

func rejectKnowledgeFields(params map[string]any, allowed ...string) error {
	accepted := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		accepted[name] = struct{}{}
	}
	for name := range params {
		if _, ok := accepted[name]; !ok {
			return errors.New("unknown field")
		}
	}
	return nil
}

func requiredKnowledgeUUID(value any) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) != text {
		return "", errors.New("invalid UUID")
	}
	parsed, err := uuid.Parse(text)
	if err != nil || parsed == uuid.Nil || parsed.String() != text {
		return "", errors.New("invalid UUID")
	}
	return text, nil
}

func requiredPositiveKnowledgeInt(value any) (int64, error) {
	parsed, err := exactNonnegativeInt64(value)
	if err != nil || parsed < 1 || parsed == math.MaxInt64 {
		return 0, errors.New("invalid positive integer")
	}
	return parsed, nil
}

func requiredKnowledgeText(value any, maximum int) (string, error) {
	raw, ok := value.(string)
	text := strings.TrimSpace(raw)
	if !ok || raw != text || text == "" || len(text) > maximum || strings.ContainsAny(text, "\r\n\t\x00") || cloudmodule.ContainsSensitiveGoalMaterial(text) {
		return "", errors.New("invalid text")
	}
	return text, nil
}

func requiredKnowledgeMediaType(value any) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) != text {
		return "", errors.New("invalid media type")
	}
	switch text {
	case "text/plain", "text/markdown", "application/json":
		return text, nil
	default:
		return "", errors.New("invalid media type")
	}
}

func requiredKnowledgeDigest(value any) (string, error) {
	text, ok := value.(string)
	if !ok || !knowledgeDigestPattern.MatchString(text) {
		return "", errors.New("invalid digest")
	}
	return text, nil
}

func optionalKnowledgeSourceIDs(value any) ([]string, error) {
	if value == nil {
		return []string{}, nil
	}
	var raw []string
	switch values := value.(type) {
	case []string:
		raw = append([]string(nil), values...)
	case []any:
		raw = make([]string, 0, len(values))
		for _, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil, errors.New("invalid source ID")
			}
			raw = append(raw, text)
		}
	default:
		return nil, errors.New("invalid source IDs")
	}
	if len(raw) > maxKnowledgeSourceIDs {
		return nil, errors.New("too many source IDs")
	}
	seen := make(map[string]struct{}, len(raw))
	result := make([]string, 0, len(raw))
	for _, value := range raw {
		id, err := requiredKnowledgeUUID(value)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

func digestKnowledgeBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return fmt.Sprintf("sha256:%x", digest[:])
}

func validateKnowledgeState(state KnowledgeState) error {
	if !state.Available || !state.Capabilities.Config || len(state.Capabilities.EmbeddingProfileIDs) == 0 || len(state.Capabilities.EmbeddingProfileIDs) > 128 ||
		!sort.StringsAreSorted(state.Capabilities.EmbeddingProfileIDs) || state.Capabilities.MaxAttachmentSize < 1 || state.Capabilities.MaxAttachmentSize > maxKnowledgeAttachmentBytes ||
		state.Capabilities.MaxAttachmentChunkSize < 1 || state.Capabilities.MaxAttachmentChunkSize > maxKnowledgeChunkBytes ||
		state.Capabilities.MaxSearchResults < 1 || state.Capabilities.MaxSearchResults > maxKnowledgeSearchResults {
		return ErrInvalidKnowledgeResponse
	}
	for index, id := range state.Capabilities.EmbeddingProfileIDs {
		if !runtimeProfileIDPattern.MatchString(id) || cloudmodule.ContainsSensitiveGoalMaterial(id) || (index > 0 && state.Capabilities.EmbeddingProfileIDs[index-1] == id) {
			return ErrInvalidKnowledgeResponse
		}
	}
	if state.Config == nil {
		return nil
	}
	config := state.Config
	if validateKnowledgeConfig(*config) != nil || !containsSortedProfileID(state.Capabilities.EmbeddingProfileIDs, config.EmbeddingProfileID) {
		return ErrInvalidKnowledgeResponse
	}
	return nil
}

// ValidateKnowledgeState is shared with the gRPC adapter so a forged or
// malformed upstream response is rejected before it can influence a follow-up
// Agent mutation.
func ValidateKnowledgeState(state KnowledgeState) error { return validateKnowledgeState(state) }

func validateKnowledgeConfig(config KnowledgeConfig) error {
	if !canonicalKnowledgeUUID(config.BindingID) || !canonicalKnowledgeUUID(config.DeploymentID) || !canonicalKnowledgeUUID(config.ManagedServiceID) ||
		!knowledgeDigestPattern.MatchString(config.RecipeDigest) || !runtimeProfileIDPattern.MatchString(config.EmbeddingProfileID) ||
		cloudmodule.ContainsSensitiveGoalMaterial(config.EmbeddingProfileID) || config.Revision < 1 || !validKnowledgeTime(config.CreatedAt) ||
		!validKnowledgeTime(config.UpdatedAt) || config.UpdatedAt.Before(config.CreatedAt) {
		return ErrInvalidKnowledgeResponse
	}
	return nil
}

func ValidateKnowledgeConfig(config KnowledgeConfig) error { return validateKnowledgeConfig(config) }

func validateKnowledgeSourcePage(page KnowledgeSourcePage) error {
	if len(page.Sources) > maxKnowledgeListPageSize || len(page.NextPageToken) > 128 || strings.ContainsAny(page.NextPageToken, "\r\n\t ") {
		return ErrInvalidKnowledgeResponse
	}
	seen := make(map[string]struct{}, len(page.Sources))
	for _, source := range page.Sources {
		if validateKnowledgeSource(source) != nil {
			return ErrInvalidKnowledgeResponse
		}
		if _, exists := seen[source.SourceID]; exists {
			return ErrInvalidKnowledgeResponse
		}
		seen[source.SourceID] = struct{}{}
	}
	return nil
}

func validateKnowledgeSource(source KnowledgeSource) error {
	if !canonicalKnowledgeUUID(source.BindingID) || !canonicalKnowledgeUUID(source.SourceID) || source.Title == "" || len(source.Title) > maxKnowledgeSourceTitleBytes ||
		strings.ContainsAny(source.Title, "\r\n\t\x00") || cloudmodule.ContainsSensitiveGoalMaterial(source.Title) || source.Size < 1 || source.Size > maxKnowledgeAttachmentBytes ||
		source.ChunkCount < 0 || source.ChunkCount > maxKnowledgeAttachmentChunks || source.Revision < 1 || !validKnowledgeTime(source.CreatedAt) ||
		!validKnowledgeTime(source.UpdatedAt) || source.UpdatedAt.Before(source.CreatedAt) {
		return ErrInvalidKnowledgeResponse
	}
	if source.Kind != "attachment" && source.Kind != "memory" {
		return ErrInvalidKnowledgeResponse
	}
	switch source.Status {
	case "uploading", "ready", "deleting", "deleted", "failed":
	default:
		return ErrInvalidKnowledgeResponse
	}
	if source.MediaType != "" && !validKnowledgeSourceMediaType(source.Kind, source.MediaType) {
		return ErrInvalidKnowledgeResponse
	}
	if source.ContentSHA256 != "" && !knowledgeDigestPattern.MatchString(source.ContentSHA256) {
		return ErrInvalidKnowledgeResponse
	}
	if source.Status == "ready" && source.ContentSHA256 == "" {
		return ErrInvalidKnowledgeResponse
	}
	if (source.Status == "ready" && source.ChunkCount < 1) || (source.Kind == "memory" && source.ChunkCount != 1) {
		return ErrInvalidKnowledgeResponse
	}
	if !validKnowledgeErrorCode(source.Error) {
		return ErrInvalidKnowledgeResponse
	}
	return nil
}

func validKnowledgeErrorCode(value string) bool {
	switch value {
	case "", "ingest_failed", "backend_unavailable", "invalid_content":
		return true
	default:
		return false
	}
}

func validKnowledgeSourceMediaType(kind, value string) bool {
	if kind == "memory" {
		return value == "text/plain; charset=utf-8"
	}
	_, err := requiredKnowledgeMediaType(value)
	return err == nil
}

func ValidateKnowledgeSource(source KnowledgeSource) error { return validateKnowledgeSource(source) }

func validateKnowledgeUpload(upload KnowledgeUpload) error {
	if !canonicalKnowledgeUUID(upload.BindingID) || !canonicalKnowledgeUUID(upload.SourceID) || !canonicalKnowledgeUUID(upload.UploadID) ||
		upload.DeclaredSize < 1 || upload.DeclaredSize > maxKnowledgeAttachmentBytes || upload.ReceivedSize < 0 || upload.ReceivedSize > upload.DeclaredSize ||
		upload.NextOrdinal < 0 || upload.NextOrdinal > maxKnowledgeAttachmentChunks || upload.Revision < 1 || upload.BindingRevision < 1 ||
		!validKnowledgeTime(upload.CreatedAt) || !validKnowledgeTime(upload.UpdatedAt) || upload.UpdatedAt.Before(upload.CreatedAt) {
		return ErrInvalidKnowledgeResponse
	}
	if upload.Status != "receiving" && upload.Status != "committed" && upload.Status != "failed" {
		return ErrInvalidKnowledgeResponse
	}
	if upload.Status == "committed" && upload.ReceivedSize != upload.DeclaredSize {
		return ErrInvalidKnowledgeResponse
	}
	if _, err := requiredKnowledgeMediaType(upload.MediaType); err != nil {
		return ErrInvalidKnowledgeResponse
	}
	return nil
}

func ValidateKnowledgeUpload(upload KnowledgeUpload) error { return validateKnowledgeUpload(upload) }

func validateKnowledgeSearchResult(result KnowledgeSearchResult, limit int, requestedSourceIDs []string) error {
	if result.BindingRevision < 1 || len(result.Matches) > limit {
		return ErrInvalidKnowledgeResponse
	}
	requested := make(map[string]struct{}, len(requestedSourceIDs))
	for _, sourceID := range requestedSourceIDs {
		requested[sourceID] = struct{}{}
	}
	for _, match := range result.Matches {
		if !canonicalKnowledgeUUID(match.SourceID) || !knowledgeChunkRefPattern.MatchString(match.ChunkRef) || cloudmodule.ContainsSensitiveGoalMaterial(match.ChunkRef) ||
			math.IsNaN(match.Score) || math.IsInf(match.Score, 0) || match.Score < 0 || match.Score > 1 {
			return ErrInvalidKnowledgeResponse
		}
		if len(requested) > 0 {
			if _, ok := requested[match.SourceID]; !ok {
				return ErrInvalidKnowledgeResponse
			}
		}
	}
	return nil
}

func ValidateKnowledgeSearchResult(result KnowledgeSearchResult, limit int, requestedSourceIDs []string) error {
	return validateKnowledgeSearchResult(result, limit, requestedSourceIDs)
}

func validateKnowledgeStatus(status KnowledgeStatus) error {
	if !canonicalKnowledgeUUID(status.BindingID) || status.BindingRevision < 1 || status.ReadySourceCount < 0 || status.UploadingSourceCount < 0 || status.FailedSourceCount < 0 ||
		!validKnowledgeTime(status.CheckedAt) {
		return ErrInvalidKnowledgeResponse
	}
	if status.BackendStatus != "unavailable" && status.BackendStatus != "ready" && status.BackendStatus != "degraded" {
		return ErrInvalidKnowledgeResponse
	}
	return nil
}

func ValidateKnowledgeStatus(status KnowledgeStatus) error { return validateKnowledgeStatus(status) }

func canonicalKnowledgeUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed != uuid.Nil && parsed.String() == value
}

func validKnowledgeTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC
}

func (state KnowledgeState) Response() map[string]any {
	var config any
	revision := int64(0)
	if state.Config != nil {
		config = state.Config.Response()
		revision = state.Config.Revision
	}
	return map[string]any{
		"available": state.Available, "configured": state.Config != nil, "revision": revision,
		"capabilities": state.Capabilities.Response(), "config": config,
	}
}

func (capabilities KnowledgeCapabilities) Response() map[string]any {
	return map[string]any{
		"config": capabilities.Config, "attachment_upload": capabilities.AttachmentUpload, "memory": capabilities.Memory, "search": capabilities.Search,
		"embedding_profile_ids": append([]string(nil), capabilities.EmbeddingProfileIDs...), "max_attachment_size": capabilities.MaxAttachmentSize,
		"max_attachment_chunk_size": capabilities.MaxAttachmentChunkSize, "max_search_results": capabilities.MaxSearchResults,
	}
}

func (config KnowledgeConfig) Response() map[string]any {
	return map[string]any{
		"binding_id": config.BindingID, "deployment_id": config.DeploymentID, "managed_service_id": config.ManagedServiceID,
		"recipe_digest": config.RecipeDigest, "embedding_profile_id": config.EmbeddingProfileID, "enabled": config.Enabled,
		"revision": config.Revision, "created_at": config.CreatedAt.Format(time.RFC3339Nano), "updated_at": config.UpdatedAt.Format(time.RFC3339Nano),
	}
}

func (source KnowledgeSource) Response() map[string]any {
	return map[string]any{
		"source_id": source.SourceID, "title": source.Title, "kind": source.Kind, "status": source.Status,
		"mime_type": source.MediaType, "size": source.Size, "content_sha256": source.ContentSHA256, "chunk_count": source.ChunkCount,
		"revision": source.Revision, "created_at": source.CreatedAt.Format(time.RFC3339Nano), "updated_at": source.UpdatedAt.Format(time.RFC3339Nano),
		"error": source.Error,
	}
}

func (upload KnowledgeUpload) Response() map[string]any {
	return map[string]any{
		"source_id": upload.SourceID, "upload_id": upload.UploadID, "status": upload.Status, "mime_type": upload.MediaType,
		"size": upload.DeclaredSize, "received_size": upload.ReceivedSize, "next_ordinal": upload.NextOrdinal,
		"revision": upload.Revision, "binding_revision": upload.BindingRevision,
	}
}

func (status KnowledgeStatus) Response() map[string]any {
	return map[string]any{
		"enabled": status.Enabled, "status": status.BackendStatus, "ready_source_count": status.ReadySourceCount,
		"uploading_source_count": status.UploadingSourceCount, "failed_source_count": status.FailedSourceCount,
		"binding_revision": status.BindingRevision, "checked_at": status.CheckedAt.Format(time.RFC3339Nano),
	}
}

func knowledgeActionError(err error) *actionbase.Error {
	switch {
	case errors.Is(err, ErrKnowledgeUnavailable):
		return actionbase.StatusError(http.StatusServiceUnavailable, "Agent Knowledge is unavailable")
	case errors.Is(err, ErrKnowledgeNotConfigured):
		return actionbase.StatusError(http.StatusNotFound, "Knowledge is not configured")
	case errors.Is(err, ErrKnowledgeNotFound):
		return actionbase.StatusError(http.StatusNotFound, "Knowledge entity was not found")
	case errors.Is(err, ErrKnowledgeConflict):
		return actionbase.StatusError(http.StatusConflict, "Knowledge revision conflicts")
	case errors.Is(err, ErrKnowledgeState):
		return actionbase.StatusError(http.StatusConflict, "Knowledge state does not allow the operation")
	case errors.Is(err, ErrInvalidKnowledgeResponse):
		return actionbase.StatusError(http.StatusBadGateway, "Agent Knowledge response is invalid")
	default:
		return actionbase.StatusError(http.StatusBadGateway, "Agent Knowledge request failed")
	}
}
