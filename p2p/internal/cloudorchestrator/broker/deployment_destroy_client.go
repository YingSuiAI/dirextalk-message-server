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

func (client *Client) SubmitDeploymentDestroy(ctx context.Context, command DeploymentDestroyCommand) (DeploymentDestroyResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil || ctx == nil {
		return DeploymentDestroyResult{}, newError("broker_client_unavailable", nil)
	}
	if err := command.Validate(); err != nil {
		return DeploymentDestroyResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return DeploymentDestroyResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return DeploymentDestroyResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return DeploymentDestroyResult{}, newError("broker_timeout", err)
		}
		return DeploymentDestroyResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return DeploymentDestroyResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return DeploymentDestroyResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return DeploymentDestroyResult{}, newError("invalid_broker_content_type", err)
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return DeploymentDestroyResult{}, err
	}
	result, err := decodeDestroyResult(responseBody)
	if err != nil || ValidateDeploymentDestroyResult(command, result) != nil {
		return DeploymentDestroyResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}
