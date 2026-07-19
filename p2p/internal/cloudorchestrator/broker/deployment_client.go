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

// SubmitDeployment sends one already-persisted deployment.create envelope to
// the configured user-owned Connection Stack. A successful result is strictly
// bound to the exact signed payload, proof and node counter before private
// resource identifiers can enter the Orchestrator store.
func (client *Client) SubmitDeployment(ctx context.Context, command DeploymentCommand) (DeploymentResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return DeploymentResult{}, newError("broker_client_unavailable", nil)
	}
	if ctx == nil {
		return DeploymentResult{}, newError("invalid_broker_context", nil)
	}
	if err := command.Validate(); err != nil {
		return DeploymentResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return DeploymentResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return DeploymentResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return DeploymentResult{}, newError("broker_timeout", err)
		}
		return DeploymentResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return DeploymentResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return DeploymentResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return DeploymentResult{}, newError("invalid_broker_content_type", err)
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return DeploymentResult{}, err
	}
	result, err := decodeDeploymentResultJSON(responseBody)
	if err != nil {
		return DeploymentResult{}, newError("invalid_broker_response", err)
	}
	if err := ValidateDeploymentResult(command, result); err != nil {
		return DeploymentResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}
