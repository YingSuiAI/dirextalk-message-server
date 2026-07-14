package broker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const (
	DefaultTimeout          = 10 * time.Second
	DefaultMaxResponseBytes = int64(256 * 1024)
	maxTimeout              = 30 * time.Second
	maxResponseBytes        = int64(1024 * 1024)
)

var (
	brokerPathPattern      = regexp.MustCompile(`^/([A-Za-z0-9_-]{1,32}/)?v2/commands$`)
	brokerErrorCodePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{2,63}$`)
)

// ClientOptions configures a fixed Connection Stack command endpoint. The
// endpoint is not accepted per request, preventing a plan or Worker from
// redirecting the orchestrator to an arbitrary URL.
type ClientOptions struct {
	Endpoint         string
	RootCAs          *x509.CertPool
	Timeout          time.Duration
	MaxResponseBytes int64
}

// Client is a narrow HTTPS-only Connection Stack V2 quote client.
type Client struct {
	endpoint         *url.URL
	httpClient       *http.Client
	maxResponseBytes int64
}

// NewClient validates an exact V2 endpoint and creates a transport which never
// inherits environment proxy settings, follows redirects, or negotiates TLS
// below 1.2. RootCAs is optional and normally nil; tests and private PKI can
// supply an explicit trusted pool without disabling certificate verification.
func NewClient(options ClientOptions) (*Client, error) {
	endpoint, err := parseBrokerEndpoint(options.Endpoint)
	if err != nil {
		return nil, err
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	if timeout <= 0 || timeout > maxTimeout {
		return nil, newError("invalid_broker_timeout", nil)
	}
	responseLimit := options.MaxResponseBytes
	if responseLimit == 0 {
		responseLimit = DefaultMaxResponseBytes
	}
	if responseLimit < 1024 || responseLimit > maxResponseBytes {
		return nil, newError("invalid_broker_response_limit", nil)
	}
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   2,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    options.RootCAs,
		},
	}
	return &Client{
		endpoint: endpoint,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		maxResponseBytes: responseLimit,
	}, nil
}

// SubmitQuote sends one already-signed envelope to the configured endpoint.
// A successful return means both the top-level quote and its durable receipt
// have been strictly verified against the exact signed command.
func (client *Client) SubmitQuote(ctx context.Context, command QuoteCommand) (QuoteResult, error) {
	if client == nil || client.endpoint == nil || client.httpClient == nil {
		return QuoteResult{}, newError("broker_client_unavailable", nil)
	}
	if ctx == nil {
		return QuoteResult{}, newError("invalid_broker_context", nil)
	}
	if err := command.Validate(); err != nil {
		return QuoteResult{}, err
	}
	body, err := json.Marshal(command)
	if err != nil || len(body) > maxRequestBytes {
		return QuoteResult{}, newError("invalid_command", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return QuoteResult{}, newError("broker_request_unavailable", err)
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Cache-Control", "no-store")
	request.Header.Set("Content-Type", "application/json")
	response, err := client.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return QuoteResult{}, newError("broker_timeout", err)
		}
		return QuoteResult{}, newError("broker_unavailable", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		if code := v2ErrorCode(response, client.maxResponseBytes); code != "" {
			return QuoteResult{}, newHTTPError(code, response.StatusCode, nil)
		}
		return QuoteResult{}, newHTTPError("broker_http_status", response.StatusCode, nil)
	}
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return QuoteResult{}, newError("invalid_broker_content_type", err)
	}
	responseBody, err := readBounded(response.Body, client.maxResponseBytes)
	if err != nil {
		return QuoteResult{}, err
	}
	result, err := decodeQuoteResultJSON(responseBody)
	if err != nil {
		return QuoteResult{}, newError("invalid_broker_response", err)
	}
	if err := ValidateQuoteResult(command, result); err != nil {
		return QuoteResult{}, newError("invalid_broker_response", err)
	}
	return result, nil
}

func parseBrokerEndpoint(raw string) (*url.URL, error) {
	if raw == "" || len(raw) > 2048 {
		return nil, newError("invalid_broker_endpoint", nil)
	}
	endpoint, err := url.ParseRequestURI(raw)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.RawPath != "" || !brokerPathPattern.MatchString(endpoint.Path) {
		return nil, newError("invalid_broker_endpoint", err)
	}
	return endpoint, nil
}

func readBounded(reader io.Reader, maximum int64) ([]byte, error) {
	limited := io.LimitReader(reader, maximum+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, newError("broker_response_unavailable", err)
	}
	if int64(len(body)) > maximum {
		return nil, newError("broker_response_too_large", nil)
	}
	return body, nil
}

func v2ErrorCode(response *http.Response, maximum int64) string {
	contentType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(contentType, "application/json") {
		return ""
	}
	body, err := readBounded(response.Body, maximum)
	if err != nil {
		return ""
	}
	object, err := exactJSONObject(body, []string{"error"})
	if err != nil {
		return ""
	}
	if _, err := exactJSONObject(object["error"], []string{"code"}); err != nil {
		return ""
	}
	var payload struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := decodeStrictJSON(body, &payload); err != nil || !brokerErrorCodePattern.MatchString(payload.Error.Code) {
		return ""
	}
	return payload.Error.Code
}
