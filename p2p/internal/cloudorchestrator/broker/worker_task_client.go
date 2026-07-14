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

// SubmitWorkerTaskIssue sends one persisted worker.task.issue envelope. The
// returned task is accepted only after strict receipt/request binding and
// de-secreted summary validation.
func (client *Client) SubmitWorkerTaskIssue(ctx context.Context, command WorkerTaskIssueCommand) (WorkerTaskIssueResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return WorkerTaskIssueResult{}, newError("broker_client_unavailable", nil)
	}
	if ctx == nil {
		return WorkerTaskIssueResult{}, newError("invalid_broker_context", nil)
	}
	if err := command.Validate(); err != nil {
		return WorkerTaskIssueResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return WorkerTaskIssueResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return WorkerTaskIssueResult{}, newError("broker_request_unavailable", err)
	}
	setWorkerTaskRequestHeaders(request)
	response, err := client.httpClient.Do(request)
	if err != nil {
		return WorkerTaskIssueResult{}, workerTaskHTTPError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return WorkerTaskIssueResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return WorkerTaskIssueResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	if err := requireWorkerTaskJSON(response); err != nil {
		return WorkerTaskIssueResult{}, err
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return WorkerTaskIssueResult{}, err
	}
	result, err := decodeWorkerTaskIssueResultJSON(responseBody)
	if err != nil {
		return WorkerTaskIssueResult{}, newError("invalid_broker_response", err)
	}
	if err := ValidateWorkerTaskIssueResult(command, result); err != nil {
		return WorkerTaskIssueResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}

// SubmitWorkerTaskObserve sends one persisted worker.task.observe envelope.
// The returned task is accepted only after strict receipt/request binding and
// de-secreted summary validation.
func (client *Client) SubmitWorkerTaskObserve(ctx context.Context, command WorkerTaskObserveCommand) (WorkerTaskObserveResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return WorkerTaskObserveResult{}, newError("broker_client_unavailable", nil)
	}
	if ctx == nil {
		return WorkerTaskObserveResult{}, newError("invalid_broker_context", nil)
	}
	if err := command.Validate(); err != nil {
		return WorkerTaskObserveResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return WorkerTaskObserveResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return WorkerTaskObserveResult{}, newError("broker_request_unavailable", err)
	}
	setWorkerTaskRequestHeaders(request)
	response, err := client.httpClient.Do(request)
	if err != nil {
		return WorkerTaskObserveResult{}, workerTaskHTTPError(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return WorkerTaskObserveResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return WorkerTaskObserveResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	if err := requireWorkerTaskJSON(response); err != nil {
		return WorkerTaskObserveResult{}, err
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return WorkerTaskObserveResult{}, err
	}
	result, err := decodeWorkerTaskObserveResultJSON(responseBody)
	if err != nil {
		return WorkerTaskObserveResult{}, newError("invalid_broker_response", err)
	}
	if err := ValidateWorkerTaskObserveResult(command, result); err != nil {
		return WorkerTaskObserveResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}

func setWorkerTaskRequestHeaders(request *http.Request) {
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
}

func workerTaskHTTPError(ctx context.Context, err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return newError("broker_timeout", err)
	}
	return newError("broker_unavailable", err)
}

func requireWorkerTaskJSON(response *http.Response) error {
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return newError("invalid_broker_content_type", err)
	}
	return nil
}
