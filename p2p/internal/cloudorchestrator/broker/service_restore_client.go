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

func (client *Client) SubmitServiceRestore(ctx context.Context, command ServiceRestoreCommand) (ServiceRestoreResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil || ctx == nil {
		return ServiceRestoreResult{}, newError("broker_client_unavailable", nil)
	}
	if err := command.Validate(); err != nil {
		return ServiceRestoreResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return ServiceRestoreResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return ServiceRestoreResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ServiceRestoreResult{}, newError("broker_timeout", err)
		}
		return ServiceRestoreResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return ServiceRestoreResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return ServiceRestoreResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		return ServiceRestoreResult{}, newError("invalid_broker_content_type", err)
	}
	raw, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return ServiceRestoreResult{}, err
	}
	result, err := decodeServiceRestoreResult(raw)
	if err != nil || ValidateServiceRestoreResult(command, result) != nil {
		return ServiceRestoreResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}
