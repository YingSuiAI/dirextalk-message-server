// Copyright 2026 YingSui AI
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/mediaapi/fileutils"
	"github.com/YingSuiAI/dirextalk-message-server/mediaapi/storage"
	"github.com/YingSuiAI/dirextalk-message-server/mediaapi/types"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	userapi "github.com/YingSuiAI/dirextalk-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	log "github.com/sirupsen/logrus"
)

const (
	resumableUploadChunkSize = int64(4 * 1024 * 1024)
	resumableUploadTTL       = 24 * time.Hour
	resumableUploadIDBytes   = 32
)

var contentRangePattern = regexp.MustCompile(`^bytes ([0-9]+)-([0-9]+)/([0-9]+)$`)

type resumableUploadStartRequest struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256,omitempty"`
}

type resumableUploadStatusResponse struct {
	UploadID      string `json:"upload_id"`
	ChunkSize     int64  `json:"chunk_size"`
	ReceivedBytes int64  `json:"received_bytes"`
	ExpiresAt     string `json:"expires_at"`
}

type resumableUploadSession struct {
	UploadID      string    `json:"upload_id"`
	UserID        string    `json:"user_id"`
	Filename      string    `json:"filename"`
	ContentType   string    `json:"content_type"`
	Size          int64     `json:"size"`
	SHA256        string    `json:"sha256,omitempty"`
	ReceivedBytes int64     `json:"received_bytes"`
	CreatedAt     time.Time `json:"created_at"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// ResumableUploadStart implements POST /_matrix/media/v3/upload/resumable.
func ResumableUploadStart(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device) util.JSONResponse {
	var body resumableUploadStartRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("invalid JSON body")}
	}
	body.Filename = strings.TrimSpace(body.Filename)
	body.ContentType = strings.TrimSpace(body.ContentType)
	body.SHA256 = strings.TrimSpace(body.SHA256)
	if body.Filename == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("filename is required")}
	}
	if body.Size <= 0 {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("size must be positive")}
	}
	if cfg.MaxFileSizeBytes > 0 && body.Size > int64(cfg.MaxFileSizeBytes) {
		return *requestEntityTooLargeJSONResponse(cfg.MaxFileSizeBytes)
	}

	if body.SHA256 != "" && !isSupportedSHA256(body.SHA256) {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("sha256 must be hex or base64url encoded")}
	}

	if err := cleanupExpiredResumableUploads(cfg); err != nil {
		util.GetLogger(req.Context()).WithError(err).Warn("failed to cleanup expired resumable uploads")
	}

	uploadID, err := generateResumableUploadID()
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to generate resumable upload id")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	now := time.Now().UTC()
	session := &resumableUploadSession{
		UploadID:    uploadID,
		UserID:      dev.UserID,
		Filename:    body.Filename,
		ContentType: body.ContentType,
		Size:        body.Size,
		SHA256:      body.SHA256,
		CreatedAt:   now,
		ExpiresAt:   now.Add(resumableUploadTTL),
	}
	if err = saveResumableUploadSession(cfg, session); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to save resumable upload session")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: session.statusResponse()}
}

// ResumableUploadStatus implements GET /_matrix/media/v3/upload/resumable/{uploadID}.
func ResumableUploadStatus(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device, uploadID string) util.JSONResponse {
	session, resErr := loadOwnedResumableUpload(req.Context(), cfg, dev, uploadID)
	if resErr != nil {
		return *resErr
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: session.statusResponse()}
}

// ResumableUploadChunk implements PUT /_matrix/media/v3/upload/resumable/{uploadID}/chunk.
func ResumableUploadChunk(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device, uploadID string) util.JSONResponse {
	session, resErr := loadOwnedResumableUpload(req.Context(), cfg, dev, uploadID)
	if resErr != nil {
		return *resErr
	}
	start, end, total, err := parseContentRange(req.Header.Get("Content-Range"))
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(err.Error())}
	}
	if total != session.Size || end < start || end >= session.Size {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("Content-Range does not match upload size")}
	}
	if start != session.ReceivedBytes {
		return util.JSONResponse{Code: http.StatusConflict, JSON: session.statusResponse()}
	}

	expected := end - start + 1
	if expected > resumableUploadChunkSize {
		return util.JSONResponse{Code: http.StatusRequestEntityTooLarge, JSON: spec.Unknown("chunk exceeds maximum chunk size")}
	}
	reader := io.LimitReader(req.Body, expected+1)
	chunk, err := io.ReadAll(reader)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Warn("failed to read resumable upload chunk")
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.Unknown("failed to read chunk")}
	}
	if int64(len(chunk)) != expected {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("chunk length does not match Content-Range")}
	}

	contentPath := resumableUploadContentPath(cfg, uploadID)
	file, err := os.OpenFile(contentPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to open resumable upload content")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	defer file.Close() // nolint: errcheck
	current, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if current != session.ReceivedBytes {
		session.ReceivedBytes = current
		_ = saveResumableUploadSession(cfg, session)
		return util.JSONResponse{Code: http.StatusConflict, JSON: session.statusResponse()}
	}
	if _, err = file.Write(chunk); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to write resumable upload chunk")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}

	session.ReceivedBytes += expected
	if err = saveResumableUploadSession(cfg, session); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to update resumable upload session")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: session.statusResponse()}
}

// ResumableUploadComplete implements POST /_matrix/media/v3/upload/resumable/{uploadID}/complete.
func ResumableUploadComplete(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device, uploadID string, db storage.Database, activeThumbnailGeneration *types.ActiveThumbnailGeneration) util.JSONResponse {
	session, resErr := loadOwnedResumableUpload(req.Context(), cfg, dev, uploadID)
	if resErr != nil {
		return *resErr
	}
	if session.ReceivedBytes != session.Size {
		return util.JSONResponse{Code: http.StatusConflict, JSON: session.statusResponse()}
	}
	contentPath := resumableUploadContentPath(cfg, uploadID)
	hash, size, err := hashFile(contentPath)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Warn("failed to hash resumable upload")
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.Unknown("failed to read upload")}
	}
	if size != session.Size {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("upload size does not match session")}
	}
	if session.SHA256 != "" && !sha256Matches(session.SHA256, hash) {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("sha256 mismatch")}
	}

	base64Hash := types.Base64Hash(base64.RawURLEncoding.EncodeToString(hash))
	uploadReq := &uploadRequest{
		MediaMetadata: &types.MediaMetadata{
			Origin:        cfg.Matrix.ServerName,
			FileSizeBytes: types.FileSizeBytes(size),
			ContentType:   types.ContentType(session.ContentType),
			UploadName:    types.Filename(url.PathEscape(session.Filename)),
			Base64Hash:    base64Hash,
			UserID:        types.MatrixUserID(dev.UserID),
		},
		Logger: util.GetLogger(req.Context()).WithField("Origin", cfg.Matrix.ServerName),
	}
	if resErr = uploadReq.Validate(cfg.MaxFileSizeBytes); resErr != nil {
		return *resErr
	}

	existingMetadata, err := db.GetMediaMetadataByHash(req.Context(), base64Hash, uploadReq.MediaMetadata.Origin)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to query resumable upload hash")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	uploadReq.MediaMetadata.MediaID, err = uploadReq.generateMediaID(req.Context(), db)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to generate resumable media id")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if existingMetadata != nil {
		uploadReq.MediaMetadata.Base64Hash = existingMetadata.Base64Hash
	}

	if resErr = uploadReq.storeFileAndMetadata(
		req.Context(),
		types.Path(resumableUploadDir(cfg, uploadID)),
		cfg.AbsBasePath,
		db,
		cfg.ThumbnailSizes,
		activeThumbnailGeneration,
		cfg.MaxThumbnailGenerators,
	); resErr != nil {
		return *resErr
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: uploadResponse{ContentURI: fmt.Sprintf("mxc://%s/%s", cfg.Matrix.ServerName, uploadReq.MediaMetadata.MediaID)},
	}
}

// ResumableUploadCancel implements DELETE /_matrix/media/v3/upload/resumable/{uploadID}.
func ResumableUploadCancel(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device, uploadID string) util.JSONResponse {
	session, resErr := loadOwnedResumableUpload(req.Context(), cfg, dev, uploadID)
	if resErr != nil {
		return *resErr
	}
	fileutils.RemoveDir(types.Path(resumableUploadDir(cfg, session.UploadID)), util.GetLogger(req.Context()))
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}

func (s *resumableUploadSession) statusResponse() resumableUploadStatusResponse {
	return resumableUploadStatusResponse{
		UploadID:      s.UploadID,
		ChunkSize:     resumableUploadChunkSize,
		ReceivedBytes: s.ReceivedBytes,
		ExpiresAt:     s.ExpiresAt.Format(time.RFC3339),
	}
}

func loadOwnedResumableUpload(ctx context.Context, cfg *config.MediaAPI, dev *userapi.Device, uploadID string) (*resumableUploadSession, *util.JSONResponse) {
	session, err := loadResumableUploadSession(cfg, uploadID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("upload session not found")}
		}
		util.GetLogger(ctx).WithError(err).Error("failed to load resumable upload session")
		return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if session.UserID != dev.UserID {
		return nil, &util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("upload session belongs to another user")}
	}
	if time.Now().UTC().After(session.ExpiresAt) {
		fileutils.RemoveDir(types.Path(resumableUploadDir(cfg, uploadID)), util.GetLogger(ctx))
		return nil, &util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("upload session expired")}
	}
	return session, nil
}

func parseContentRange(value string) (start, end, total int64, err error) {
	matches := contentRangePattern.FindStringSubmatch(strings.TrimSpace(value))
	if matches == nil {
		return 0, 0, 0, fmt.Errorf("Content-Range must be in the form bytes start-end/total")
	}
	start, err = strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return
	}
	end, err = strconv.ParseInt(matches[2], 10, 64)
	if err != nil {
		return
	}
	total, err = strconv.ParseInt(matches[3], 10, 64)
	return
}

func generateResumableUploadID() (string, error) {
	idBytes := make([]byte, resumableUploadIDBytes)
	if _, err := rand.Read(idBytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(idBytes), nil
}

func resumableUploadBaseDir(cfg *config.MediaAPI) string {
	return filepath.Join(string(cfg.AbsBasePath), "tmp", "resumable")
}

func resumableUploadDir(cfg *config.MediaAPI, uploadID string) string {
	return filepath.Join(resumableUploadBaseDir(cfg), uploadID)
}

func resumableUploadSessionPath(cfg *config.MediaAPI, uploadID string) string {
	return filepath.Join(resumableUploadDir(cfg, uploadID), "session.json")
}

func resumableUploadContentPath(cfg *config.MediaAPI, uploadID string) string {
	return filepath.Join(resumableUploadDir(cfg, uploadID), "content")
}

func saveResumableUploadSession(cfg *config.MediaAPI, session *resumableUploadSession) error {
	dir := resumableUploadDir(cfg, session.UploadID)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := resumableUploadSessionPath(cfg, session.UploadID)
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encErr := json.NewEncoder(file).Encode(session)
	closeErr := file.Close()
	if encErr != nil {
		return encErr
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmpPath, path)
}

func loadResumableUploadSession(cfg *config.MediaAPI, uploadID string) (*resumableUploadSession, error) {
	if uploadID == "" || strings.Contains(uploadID, ".") || strings.ContainsAny(uploadID, `/\`) {
		return nil, os.ErrNotExist
	}
	file, err := os.Open(resumableUploadSessionPath(cfg, uploadID))
	if err != nil {
		return nil, err
	}
	defer file.Close() // nolint: errcheck
	var session resumableUploadSession
	if err = json.NewDecoder(file).Decode(&session); err != nil {
		return nil, err
	}
	if session.UploadID != uploadID {
		return nil, fmt.Errorf("resumable upload session id mismatch")
	}
	return &session, nil
}

func cleanupExpiredResumableUploads(cfg *config.MediaAPI) error {
	entries, err := os.ReadDir(resumableUploadBaseDir(cfg))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	now := time.Now().UTC()
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		session, err := loadResumableUploadSession(cfg, entry.Name())
		if err != nil || now.After(session.ExpiresAt) {
			_ = os.RemoveAll(resumableUploadDir(cfg, entry.Name()))
		}
	}
	return nil
}

func hashFile(path string) ([]byte, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close() // nolint: errcheck
	hasher := sha256.New()
	size, err := io.Copy(hasher, file)
	if err != nil {
		return nil, 0, err
	}
	return hasher.Sum(nil), size, nil
}

func isSupportedSHA256(value string) bool {
	_, err := hex.DecodeString(value)
	if err == nil {
		return true
	}
	_, err = base64.RawURLEncoding.DecodeString(value)
	return err == nil
}

func sha256Matches(value string, hash []byte) bool {
	if decoded, err := hex.DecodeString(value); err == nil {
		return string(decoded) == string(hash)
	}
	if decoded, err := base64.RawURLEncoding.DecodeString(value); err == nil {
		return string(decoded) == string(hash)
	}
	return false
}

func resumableLog(req *http.Request, uploadID string) *log.Entry {
	return util.GetLogger(req.Context()).WithField("upload_id", uploadID)
}
