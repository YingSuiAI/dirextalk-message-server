package nativeagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTavilySearchEndpoint = "https://api.tavily.com/search"
	maxWebSearchResponseBytes   = 2 << 20
)

func (r *Runtime) requestScopedWebSearchTool(params map[string]any) []Tool {
	credentials := toolCredentialsFromParams(params).WebSearch
	if credentials.validate() != nil {
		return nil
	}
	return []Tool{{
		Name:        "web_search",
		Description: "Search the public web for current information. Use this for recent facts, news, weather, schedules, prices, or sources that may have changed.",
		Parameters: map[string]any{
			"type":     "object",
			"required": []any{"query"},
			"properties": map[string]any{
				"query":       stringSchema(),
				"max_results": map[string]any{"type": "integer", "minimum": 1, "maximum": 10},
			},
		},
		Handler: func(ctx context.Context, args map[string]any) (any, error) {
			return r.searchTavily(ctx, credentials, args)
		},
	}}
}

func (r *Runtime) testWebSearch(ctx context.Context, params map[string]any) (map[string]any, error) {
	credentials := toolCredentialsFromParams(params).WebSearch
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	result, err := r.searchTavily(ctx, credentials, map[string]any{
		"query":       "Dirextalk connection test",
		"max_results": 1,
	})
	if err != nil {
		return nil, err
	}
	results, _ := result["results"].([]map[string]any)
	return map[string]any{
		"ok":           true,
		"provider":     "tavily",
		"result_count": len(results),
	}, nil
}

// searchTavily performs one bounded Tavily request with request-scoped Bearer
// credentials. It returns sanitized snippets and never exposes provider
// response bodies or credentials through its result or errors.
func (r *Runtime) searchTavily(ctx context.Context, credentials webSearchCredentials, args map[string]any) (map[string]any, error) {
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	query := strings.TrimSpace(trimString(args["query"]))
	if query == "" {
		return nil, fmt.Errorf("web search query is required")
	}
	if len([]rune(query)) > 1000 {
		return nil, fmt.Errorf("web search query must be at most 1000 characters")
	}
	maxResults := int(int64Param(args["max_results"]))
	if maxResults <= 0 {
		maxResults = 5
	}
	if maxResults > 10 {
		maxResults = 10
	}
	payload, err := json.Marshal(map[string]any{
		"query":        query,
		"search_depth": "basic",
		"max_results":  maxResults,
	})
	if err != nil {
		return nil, fmt.Errorf("build web search request: %w", err)
	}
	endpoint := strings.TrimSpace(r.webSearchEndpoint)
	if endpoint == "" {
		endpoint = defaultTavilySearchEndpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, fmt.Errorf("web search endpoint is invalid")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("build web search request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(credentials.APIKey))
	response, err := r.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("web search request failed: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxWebSearchResponseBytes))
	if err != nil {
		return nil, fmt.Errorf("read web search response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return nil, fmt.Errorf("web search API key was rejected")
		case http.StatusTooManyRequests:
			return nil, fmt.Errorf("web search provider rate limit was exceeded")
		default:
			return nil, fmt.Errorf("web search provider returned HTTP %d", response.StatusCode)
		}
	}
	var decoded struct {
		Answer  string `json:"answer"`
		Results []struct {
			Title   string  `json:"title"`
			URL     string  `json:"url"`
			Content string  `json:"content"`
			Score   float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("decode web search response: %w", err)
	}
	results := make([]map[string]any, 0, len(decoded.Results))
	for _, item := range decoded.Results {
		if strings.TrimSpace(item.URL) == "" {
			continue
		}
		results = append(results, map[string]any{
			"title":   previewText(item.Title, 300),
			"url":     strings.TrimSpace(item.URL),
			"content": previewText(item.Content, 2000),
			"score":   item.Score,
		})
	}
	return map[string]any{
		"provider": "tavily",
		"query":    query,
		"answer":   previewText(decoded.Answer, 3000),
		"results":  results,
	}, nil
}
