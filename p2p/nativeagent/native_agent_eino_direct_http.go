package nativeagent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/cloudwego/eino/schema"
)

const directModelResponseLimit = 4 << 20

type directModelRequestBuilder func(context.Context, map[string]any) (*http.Request, error)
type directModelStreamDecoder func([]byte) *schema.Message

func postDirectModel(
	ctx context.Context,
	client *http.Client,
	buildRequest directModelRequestBuilder,
	payload map[string]any,
) (map[string]any, error) {
	resp, err := doDirectModelRequest(ctx, client, buildRequest, payload)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, directModelResponseLimit))
	if err != nil {
		return nil, err
	}
	if err := directModelStatusError(resp.StatusCode, body); err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func streamDirectModel(
	ctx context.Context,
	client *http.Client,
	buildRequest directModelRequestBuilder,
	payload map[string]any,
	decode directModelStreamDecoder,
) (*schema.StreamReader[*schema.Message], error) {
	resp, err := doDirectModelRequest(ctx, client, buildRequest, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, directModelResponseLimit))
		return nil, directModelStatusError(resp.StatusCode, body)
	}

	reader, writer := schema.Pipe[*schema.Message](8)
	go func() {
		defer writer.Close()
		defer resp.Body.Close()
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 1024), directModelResponseLimit)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "" || data == "[DONE]" {
				continue
			}
			if msg := decode([]byte(data)); msg != nil {
				writer.Send(msg, nil)
			}
		}
		if err := scanner.Err(); err != nil {
			writer.Send(nil, err)
		}
	}()
	return reader, nil
}

func doDirectModelRequest(
	ctx context.Context,
	client *http.Client,
	buildRequest directModelRequestBuilder,
	payload map[string]any,
) (*http.Response, error) {
	req, err := buildRequest(ctx, payload)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func directModelStatusError(statusCode int, body []byte) error {
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		return nil
	}
	return fmt.Errorf("model provider returned %d: %s", statusCode, strings.TrimSpace(string(body)))
}
