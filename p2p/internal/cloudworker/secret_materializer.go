package cloudworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudworker/recipeexec"
)

const maxMaterializedSecretSize = 32 * 1024

var (
	materializeTaskID    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$`)
	materializeBindingID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	materializeSecretRef = regexp.MustCompile(`^secret_ref:[A-Za-z0-9._/-]{1,120}$`)
)

type sessionSecretMaterializer struct{ session *SessionClient }

type materializeWireRequest struct {
	TaskID         string `json:"task_id"`
	ExecutionID    string `json:"execution_id"`
	ManifestDigest string `json:"manifest_digest"`
	ArtifactDigest string `json:"artifact_digest"`
	SlotID         string `json:"slot_id"`
	SecretRef      string `json:"secret_ref"`
}

// NewSecretMaterializer derives a closed secret client from an active Worker
// session. The bearer token remains owned by SessionClient.
func (client *SessionClient) NewSecretMaterializer() (recipeexec.SecretMaterializer, error) {
	if client == nil {
		return nil, ErrSessionNotClaimed
	}
	if _, _, _, err := client.recipeTaskAuthorization(); err != nil {
		return nil, err
	}
	return &sessionSecretMaterializer{session: client}, nil
}

func (client *sessionSecretMaterializer) Materialize(ctx context.Context, input recipeexec.SecretMaterializeRequest) ([]byte, error) {
	if client == nil || client.session == nil || !validMaterializeRequest(input) {
		return nil, errors.New("worker secret materialization request is invalid")
	}
	_, token, epoch, err := client.session.recipeTaskAuthorization()
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(materializeWireRequest(input))
	if err != nil {
		return nil, errors.New("worker secret materialization request is invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.session.operationURL("service-secrets/materialize"), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("worker secret materialization request is invalid")
	}
	request.Header.Set("Accept", "application/octet-stream")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Dirextalk-Worker-Lease-Epoch", strconv.FormatUint(epoch, 10))
	response, err := client.session.client.Do(request)
	if err != nil {
		if ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("worker secret materialization is unavailable")
	}
	defer response.Body.Close()
	contentType := strings.ToLower(strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0]))
	if response.StatusCode == http.StatusTooEarly {
		return nil, recipeexec.ErrSecretMaterializePending
	}
	if response.StatusCode != http.StatusOK || contentType != "application/octet-stream" {
		return nil, errors.New("worker secret materialization was rejected")
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, maxMaterializedSecretSize+1))
	if err != nil || len(value) == 0 || len(value) > maxMaterializedSecretSize {
		clear(value)
		return nil, errors.New("worker secret materialization response is invalid")
	}
	result := append([]byte(nil), value...)
	clear(value)
	return result, nil
}

func validMaterializeRequest(input recipeexec.SecretMaterializeRequest) bool {
	return materializeTaskID.MatchString(input.TaskID) && materializeBindingID.MatchString(input.ExecutionID) &&
		validSHA256Digest(input.ManifestDigest) && validSHA256Digest(input.ArtifactDigest) &&
		materializeBindingID.MatchString(input.SlotID) && materializeSecretRef.MatchString(input.SecretRef)
}

func validSHA256Digest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[7:] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}
