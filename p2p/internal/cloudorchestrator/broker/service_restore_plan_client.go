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

func (client *Client) SubmitServiceRestorePlan(ctx context.Context, command ServiceRestorePlanCommand) (ServiceRestorePlanResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil || ctx == nil {
		return ServiceRestorePlanResult{}, newError("broker_client_unavailable", nil)
	}
	if err := command.Validate(); err != nil {
		return ServiceRestorePlanResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return ServiceRestorePlanResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return ServiceRestorePlanResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return ServiceRestorePlanResult{}, newError("broker_timeout", err)
		}
		return ServiceRestorePlanResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return ServiceRestorePlanResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return ServiceRestorePlanResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	media, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(media, "application/json") {
		return ServiceRestorePlanResult{}, newError("invalid_broker_content_type", err)
	}
	raw, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return ServiceRestorePlanResult{}, err
	}
	result, err := decodeServiceRestorePlanResult(raw)
	if err != nil || ValidateServiceRestorePlanResult(command, result) != nil {
		return ServiceRestorePlanResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}
