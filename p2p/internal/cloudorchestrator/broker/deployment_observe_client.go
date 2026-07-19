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

// SubmitDeploymentObserve sends one persisted deployment.observe envelope.
// It validates the exact response before any Stack-derived Worker evidence can
// be written to the Orchestrator database.
func (client *Client) SubmitDeploymentObserve(ctx context.Context, command DeploymentObserveCommand) (DeploymentObserveResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return DeploymentObserveResult{}, newError("broker_client_unavailable", nil)
	}
	if ctx == nil {
		return DeploymentObserveResult{}, newError("invalid_broker_context", nil)
	}
	if err := command.Validate(); err != nil {
		return DeploymentObserveResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return DeploymentObserveResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return DeploymentObserveResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return DeploymentObserveResult{}, newError("broker_timeout", err)
		}
		return DeploymentObserveResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return DeploymentObserveResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return DeploymentObserveResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return DeploymentObserveResult{}, newError("invalid_broker_content_type", err)
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return DeploymentObserveResult{}, err
	}
	result, err := decodeDeploymentObserveResultJSON(responseBody)
	if err != nil {
		return DeploymentObserveResult{}, newError("invalid_broker_response", err)
	}
	if err := ValidateDeploymentObserveResult(command, result); err != nil {
		return DeploymentObserveResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}
