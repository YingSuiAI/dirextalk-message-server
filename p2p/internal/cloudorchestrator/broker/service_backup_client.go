package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"strings"
)

func (client *Client) SubmitServiceBackup(ctx context.Context, command ServiceBackupCommand) (ServiceBackupResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil || ctx == nil {
		return ServiceBackupResult{}, newError("broker_client_unavailable", nil)
	}
	if e := command.Validate(); e != nil {
		return ServiceBackupResult{}, e
	}
	body, e := json.Marshal(command)
	if e != nil || len(body) > maxRequestBytes {
		return ServiceBackupResult{}, newError("invalid_command", e)
	}
	request, e := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if e != nil {
		return ServiceBackupResult{}, newError("broker_request_unavailable", e)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, e := client.httpClient.Do(request)
	if e != nil {
		if errors.Is(e, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ServiceBackupResult{}, newError("broker_timeout", e)
		}
		return ServiceBackupResult{}, newError("broker_unavailable", e)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return ServiceBackupResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return ServiceBackupResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, e := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if e != nil || !strings.EqualFold(media, "application/json") {
		return ServiceBackupResult{}, newError("invalid_broker_content_type", e)
	}
	raw, e := readBounded(response.Body, client.maxResponseBytes)
	if e != nil {
		return ServiceBackupResult{}, e
	}
	result, e := decodeServiceBackupResult(raw)
	if e != nil || ValidateServiceBackupResult(command, result) != nil {
		return ServiceBackupResult{}, newError("invalid_broker_response", e)
	}
	return result, nil
}
