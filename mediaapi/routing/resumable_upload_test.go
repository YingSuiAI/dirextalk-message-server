package routing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/mediaapi/types"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

func TestResumableUploadCompletesAndStoresMedia(t *testing.T) {
	cfg := testResumableMediaConfig(t, 100)
	db := newTestResumableMediaDB()
	dev := &userapi.Device{UserID: "@alice:example.com"}
	payload := []byte("hello resumable upload")
	sum := sha256.Sum256(payload)

	startReq := testJSONRequest(t, resumableUploadStartRequest{
		Filename:    "clip.mp4",
		ContentType: "video/mp4",
		Size:        int64(len(payload)),
		SHA256:      hex.EncodeToString(sum[:]),
	})
	startRes := ResumableUploadStart(startReq, cfg, dev)
	if startRes.Code != http.StatusOK {
		t.Fatalf("start code = %d, want 200: %#v", startRes.Code, startRes.JSON)
	}
	var status resumableUploadStatusResponse
	mustRoundTripJSON(t, startRes.JSON, &status)
	if status.ChunkSize != resumableUploadChunkSize || status.UploadID == "" {
		t.Fatalf("unexpected start response: %#v", status)
	}

	chunkReq := testChunkRequest(payload, 0, len(payload)-1, len(payload))
	chunkRes := ResumableUploadChunk(chunkReq, cfg, dev, status.UploadID)
	if chunkRes.Code != http.StatusOK {
		t.Fatalf("chunk code = %d, want 200: %#v", chunkRes.Code, chunkRes.JSON)
	}
	mustRoundTripJSON(t, chunkRes.JSON, &status)
	if status.ReceivedBytes != int64(len(payload)) {
		t.Fatalf("received bytes = %d, want %d", status.ReceivedBytes, len(payload))
	}

	completeReq := testJSONRequest(t, nil)
	completeRes := ResumableUploadComplete(
		completeReq, cfg, dev, status.UploadID, db,
		&types.ActiveThumbnailGeneration{PathToResult: map[string]*types.ThumbnailGenerationResult{}},
	)
	if completeRes.Code != http.StatusOK {
		t.Fatalf("complete code = %d, want 200: %#v", completeRes.Code, completeRes.JSON)
	}
	var upload uploadResponse
	mustRoundTripJSON(t, completeRes.JSON, &upload)
	if !strings.HasPrefix(upload.ContentURI, "mxc://example.com/") {
		t.Fatalf("content uri = %q", upload.ContentURI)
	}
	if len(db.metadataByID) != 1 {
		t.Fatalf("stored metadata count = %d, want 1", len(db.metadataByID))
	}
	for _, metadata := range db.metadataByID {
		finalPath, err := filepath.Abs(filepath.Join(
			string(cfg.AbsBasePath),
			string(metadata.Base64Hash[0:1]),
			string(metadata.Base64Hash[1:2]),
			string(metadata.Base64Hash[2:]),
			"file",
		))
		if err != nil {
			t.Fatal(err)
		}
		stored, err := os.ReadFile(finalPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(stored, payload) {
			t.Fatalf("stored payload = %q, want %q", stored, payload)
		}
	}
}

func TestResumableUploadRejectsWrongUserAndOutOfOrderChunk(t *testing.T) {
	cfg := testResumableMediaConfig(t, 100)
	alice := &userapi.Device{UserID: "@alice:example.com"}
	bob := &userapi.Device{UserID: "@bob:example.com"}
	payload := []byte("abcdef")

	startRes := ResumableUploadStart(testJSONRequest(t, resumableUploadStartRequest{
		Filename: "clip.mp4",
		Size:     int64(len(payload)),
	}), cfg, alice)
	if startRes.Code != http.StatusOK {
		t.Fatalf("start code = %d, want 200", startRes.Code)
	}
	var status resumableUploadStatusResponse
	mustRoundTripJSON(t, startRes.JSON, &status)

	wrongUserRes := ResumableUploadStatus(testJSONRequest(t, nil), cfg, bob, status.UploadID)
	if wrongUserRes.Code != http.StatusForbidden {
		t.Fatalf("wrong user status code = %d, want 403", wrongUserRes.Code)
	}

	outOfOrderReq := testChunkRequest(payload[2:4], 2, 3, len(payload))
	outOfOrderRes := ResumableUploadChunk(outOfOrderReq, cfg, alice, status.UploadID)
	if outOfOrderRes.Code != http.StatusConflict {
		t.Fatalf("out of order code = %d, want 409", outOfOrderRes.Code)
	}
	mustRoundTripJSON(t, outOfOrderRes.JSON, &status)
	if status.ReceivedBytes != 0 {
		t.Fatalf("received bytes after out-of-order chunk = %d, want 0", status.ReceivedBytes)
	}
}

func TestResumableUploadStartEnforcesMaxSize(t *testing.T) {
	cfg := testResumableMediaConfig(t, 5)
	dev := &userapi.Device{UserID: "@alice:example.com"}
	res := ResumableUploadStart(testJSONRequest(t, resumableUploadStartRequest{
		Filename: "too-large.mp4",
		Size:     6,
	}), cfg, dev)
	if res.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d, want 413", res.Code)
	}
}

func TestResumableUploadStartRejectsShortSHA256(t *testing.T) {
	cfg := testResumableMediaConfig(t, 100)
	dev := &userapi.Device{UserID: "@alice:example.com"}
	res := ResumableUploadStart(testJSONRequest(t, resumableUploadStartRequest{
		Filename: "clip.mp4",
		Size:     4,
		SHA256:   "00",
	}), cfg, dev)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400: %#v", res.Code, res.JSON)
	}
}

func TestResumableUploadConcurrentSameOffsetChunkDoesNotDoubleWrite(t *testing.T) {
	cfg := testResumableMediaConfig(t, 100)
	dev := &userapi.Device{UserID: "@alice:example.com"}
	payload := []byte("abcd")

	startRes := ResumableUploadStart(testJSONRequest(t, resumableUploadStartRequest{
		Filename: "clip.mp4",
		Size:     int64(len(payload) * 2),
	}), cfg, dev)
	if startRes.Code != http.StatusOK {
		t.Fatalf("start code = %d, want 200: %#v", startRes.Code, startRes.JSON)
	}
	var status resumableUploadStatusResponse
	mustRoundTripJSON(t, startRes.JSON, &status)

	var wg sync.WaitGroup
	results := make(chan util.JSONResponse, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- ResumableUploadChunk(
				testChunkRequest(payload, 0, len(payload)-1, len(payload)*2),
				cfg,
				dev,
				status.UploadID,
			)
		}()
	}
	wg.Wait()
	close(results)

	codes := map[int]int{}
	for res := range results {
		codes[res.Code]++
	}
	if codes[http.StatusOK] != 1 || codes[http.StatusConflict] != 1 {
		t.Fatalf("response codes = %#v, want one 200 and one 409", codes)
	}

	stored, err := os.ReadFile(resumableUploadContentPath(cfg, status.UploadID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored payload = %q, want %q", stored, payload)
	}
	session, err := loadResumableUploadSession(cfg, status.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if session.ReceivedBytes != int64(len(payload)) {
		t.Fatalf("received bytes = %d, want %d", session.ReceivedBytes, len(payload))
	}
}

func TestResumableUploadCompleteWaitsForInFlightChunk(t *testing.T) {
	cfg := testResumableMediaConfig(t, 100)
	db := newTestResumableMediaDB()
	dev := &userapi.Device{UserID: "@alice:example.com"}
	payload := []byte("complete after chunk")

	startRes := ResumableUploadStart(testJSONRequest(t, resumableUploadStartRequest{
		Filename: "clip.mp4",
		Size:     int64(len(payload)),
	}), cfg, dev)
	if startRes.Code != http.StatusOK {
		t.Fatalf("start code = %d, want 200: %#v", startRes.Code, startRes.JSON)
	}
	var status resumableUploadStatusResponse
	mustRoundTripJSON(t, startRes.JSON, &status)

	body := &blockingChunkBody{
		data:      payload,
		firstRead: make(chan struct{}),
		release:   make(chan struct{}),
	}
	chunkReq := testChunkRequest(nil, 0, len(payload)-1, len(payload))
	chunkReq.Body = body
	chunkDone := make(chan util.JSONResponse, 1)
	go func() {
		chunkDone <- ResumableUploadChunk(chunkReq, cfg, dev, status.UploadID)
	}()
	<-body.firstRead

	completeDone := make(chan util.JSONResponse, 1)
	go func() {
		completeDone <- ResumableUploadComplete(
			testJSONRequest(t, nil),
			cfg,
			dev,
			status.UploadID,
			db,
			&types.ActiveThumbnailGeneration{PathToResult: map[string]*types.ThumbnailGenerationResult{}},
		)
	}()
	select {
	case res := <-completeDone:
		t.Fatalf("complete returned before in-flight chunk finished: %#v", res)
	case <-time.After(50 * time.Millisecond):
	}

	close(body.release)
	chunkRes := <-chunkDone
	if chunkRes.Code != http.StatusOK {
		t.Fatalf("chunk code = %d, want 200: %#v", chunkRes.Code, chunkRes.JSON)
	}
	completeRes := <-completeDone
	if completeRes.Code != http.StatusOK {
		t.Fatalf("complete code = %d, want 200: %#v", completeRes.Code, completeRes.JSON)
	}
}

func testResumableMediaConfig(t *testing.T, maxBytes config.FileSizeBytes) *config.MediaAPI {
	t.Helper()
	base := t.TempDir()
	return &config.MediaAPI{
		Matrix: &config.Global{
			SigningIdentity: fclient.SigningIdentity{
				ServerName: "example.com",
			},
		},
		MaxFileSizeBytes:       maxBytes,
		BasePath:               config.Path(base),
		AbsBasePath:            config.Path(base),
		MaxThumbnailGenerators: 1,
	}
}

type blockingChunkBody struct {
	data      []byte
	firstRead chan struct{}
	release   chan struct{}
	sent      bool
}

func (b *blockingChunkBody) Read(p []byte) (int, error) {
	if b.sent {
		return 0, io.EOF
	}
	b.sent = true
	close(b.firstRead)
	<-b.release
	return copy(p, b.data), nil
}

func (b *blockingChunkBody) Close() error {
	return nil
}

func testJSONRequest(t *testing.T, body any) *http.Request {
	t.Helper()
	var reader io.Reader = http.NoBody
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(http.MethodPost, "https://example.com/_matrix/media/v3/upload/resumable", reader)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func testChunkRequest(body []byte, start, end, total int) *http.Request {
	req, _ := http.NewRequest(http.MethodPut, "https://example.com/_matrix/media/v3/upload/resumable/id/chunk", bytes.NewReader(body))
	req.Header.Set("Content-Range", "bytes "+strconv.Itoa(start)+"-"+strconv.Itoa(end)+"/"+strconv.Itoa(total))
	return req
}

func mustRoundTripJSON(t *testing.T, in any, out any) {
	t.Helper()
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
}

type testResumableMediaDB struct {
	mu             sync.Mutex
	metadataByID   map[string]*types.MediaMetadata
	metadataByHash map[string]*types.MediaMetadata
}

func newTestResumableMediaDB() *testResumableMediaDB {
	return &testResumableMediaDB{
		metadataByID:   map[string]*types.MediaMetadata{},
		metadataByHash: map[string]*types.MediaMetadata{},
	}
}

func (db *testResumableMediaDB) StoreMediaMetadata(ctx context.Context, mediaMetadata *types.MediaMetadata) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	clone := *mediaMetadata
	db.metadataByID[string(mediaMetadata.MediaID)] = &clone
	db.metadataByHash[string(mediaMetadata.Base64Hash)] = &clone
	return nil
}

func (db *testResumableMediaDB) GetMediaMetadata(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.metadataByID[string(mediaID)], nil
}

func (db *testResumableMediaDB) GetMediaMetadataByHash(ctx context.Context, mediaHash types.Base64Hash, mediaOrigin spec.ServerName) (*types.MediaMetadata, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.metadataByHash[string(mediaHash)], nil
}

func (db *testResumableMediaDB) StoreThumbnail(ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata) error {
	return nil
}

func (db *testResumableMediaDB) GetThumbnail(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName, width, height int, resizeMethod string) (*types.ThumbnailMetadata, error) {
	return nil, nil
}

func (db *testResumableMediaDB) GetThumbnails(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) ([]*types.ThumbnailMetadata, error) {
	return nil, nil
}
