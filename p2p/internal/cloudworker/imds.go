package cloudworker

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
)

const (
	productionIMDSEndpoint    = "http://169.254.169.254"
	imdsTokenPath             = "/latest/api/token"
	imdsIdentityDocumentPath  = "/latest/dynamic/instance-identity/document"
	imdsIdentitySignaturePath = "/latest/dynamic/instance-identity/signature"
	imdsTokenTTLHeader        = "X-aws-ec2-metadata-token-ttl-seconds"
	imdsTokenHeader           = "X-aws-ec2-metadata-token"
	imdsTokenTTLSeconds       = "60"
	maxIMDSTokenBytes         = 4 * 1024
	maxIMDSSignatureBodyBytes = maxIdentitySignatureBytes*2 + 2
)

// ErrIMDSv2IdentityUnavailable deliberately does not expose metadata request
// details, response bodies, or the short-lived IMDS token.
var ErrIMDSv2IdentityUnavailable = errors.New("worker instance identity is unavailable")

// IMDSv2IdentityProvider obtains only the EC2 instance identity document and
// its AWS-provided signature. It is intentionally not an AWS SDK client and
// never reads environment credentials, role credentials, or user data.
//
// Production instances always use the link-local IMDS address. The only
// alternate endpoint constructor is package-private and accepts loopback
// endpoints exclusively for focused tests.
type IMDSv2IdentityProvider struct {
	endpoint *url.URL
	client   *http.Client
}

// NewIMDSv2IdentityProvider creates the production-only provider. Its
// endpoint is a fixed IPv4 link-local literal rather than an environment or
// configuration value, so a Worker cannot be pointed at a remote metadata
// substitute by launch configuration.
func NewIMDSv2IdentityProvider() (*IMDSv2IdentityProvider, error) {
	return newIMDSv2IdentityProvider(productionIMDSEndpoint, nil, false)
}

// Fetch returns the opaque identity material required by ClaimRequest. The
// document is encoded exactly once from its raw signed JSON bytes. IMDS
// exposes the signature as standard base64 text, so it is normalized and
// validated but never double encoded.
func (provider *IMDSv2IdentityProvider) Fetch(ctx context.Context) (InstanceIdentityProof, error) {
	if provider == nil || provider.endpoint == nil || provider.client == nil || ctx == nil {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}
	tokenBody, err := provider.request(ctx, http.MethodPut, imdsTokenPath, "", maxIMDSTokenBytes, "text/plain")
	if err != nil || !validIMDSToken(string(tokenBody)) {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}
	token := string(tokenBody)

	document, err := provider.request(ctx, http.MethodGet, imdsIdentityDocumentPath, token, maxIdentityDocumentBytes, "application/json", "text/plain")
	if err != nil || !validIMDSIdentityDocument(document) {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}
	signatureBody, err := provider.request(ctx, http.MethodGet, imdsIdentitySignaturePath, token, maxIMDSSignatureBodyBytes, "text/plain", "application/octet-stream")
	if err != nil {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}
	signature := strings.TrimSpace(string(signatureBody))
	if !validCanonicalBase64(signature, maxIdentitySignatureBytes) {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}

	proof := InstanceIdentityProof{
		DocumentB64:  base64.StdEncoding.EncodeToString(document),
		SignatureB64: signature,
	}
	if err := proof.Validate(); err != nil {
		return InstanceIdentityProof{}, ErrIMDSv2IdentityUnavailable
	}
	return proof, nil
}

func newIMDSv2IdentityProviderForTest(endpoint string, client *http.Client) (*IMDSv2IdentityProvider, error) {
	return newIMDSv2IdentityProvider(endpoint, client, true)
}

func newIMDSv2IdentityProvider(endpoint string, source *http.Client, allowLoopback bool) (*IMDSv2IdentityProvider, error) {
	parsed, err := parseIMDSEndpoint(endpoint, allowLoopback)
	if err != nil {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	client, err := secureIMDSHTTPClient(source)
	if err != nil {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	return &IMDSv2IdentityProvider{endpoint: parsed, client: client}, nil
}

func (provider *IMDSv2IdentityProvider) request(ctx context.Context, method, path, token string, maximum int, contentTypes ...string) ([]byte, error) {
	endpoint := *provider.endpoint
	endpoint.Path = path
	endpoint.RawPath = ""
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), nil)
	if err != nil {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	request.Header.Set("Accept", contentTypes[0])
	request.Header.Set("Accept-Encoding", "identity")
	if token != "" {
		request.Header.Set(imdsTokenHeader, token)
	} else {
		request.Header.Set(imdsTokenTTLHeader, imdsTokenTTLSeconds)
	}
	response, err := provider.client.Do(request)
	if err != nil {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK || !validIMDSContentType(response.Header.Get("Content-Type"), contentTypes...) ||
		!validIMDSContentEncoding(response.Header.Get("Content-Encoding")) ||
		response.ContentLength == 0 || response.ContentLength > int64(maximum) {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(maximum)+1))
	if err != nil || len(body) == 0 || len(body) > maximum {
		return nil, ErrIMDSv2IdentityUnavailable
	}
	return body, nil
}

func secureIMDSHTTPClient(source *http.Client) (*http.Client, error) {
	client, err := secureHTTPClient(source)
	if err != nil {
		return nil, err
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		return nil, errors.New("worker IMDS HTTP transport is invalid")
	}
	transport.DisableCompression = true
	client.Jar = nil
	return client, nil
}

func parseIMDSEndpoint(value string, allowLoopback bool) (*url.URL, error) {
	if value == "" || strings.TrimSpace(value) != value {
		return nil, errors.New("worker IMDS endpoint is invalid")
	}
	endpoint, err := url.Parse(value)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Opaque != "" ||
		(endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errors.New("worker IMDS endpoint is invalid")
	}
	host := endpoint.Hostname()
	if !allowLoopback {
		if host != "169.254.169.254" || endpoint.Port() != "" {
			return nil, errors.New("worker IMDS endpoint is invalid")
		}
	} else if parsed := net.ParseIP(host); parsed == nil || !parsed.IsLoopback() {
		return nil, errors.New("worker IMDS endpoint is invalid")
	}
	endpoint.Path = ""
	endpoint.RawPath = ""
	return endpoint, nil
}

func validIMDSToken(value string) bool {
	if len(value) == 0 || len(value) > maxIMDSTokenBytes || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func validIMDSIdentityDocument(value []byte) bool {
	if len(value) == 0 || len(value) > maxIdentityDocumentBytes || !json.Valid(value) {
		return false
	}
	trimmed := bytes.TrimSpace(value)
	return len(trimmed) >= 2 && trimmed[0] == '{' && trimmed[len(trimmed)-1] == '}'
}

func validIMDSContentType(value string, allowed ...string) bool {
	if value == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return false
	}
	mediaType = strings.ToLower(mediaType)
	for _, expected := range allowed {
		if mediaType == expected {
			return true
		}
	}
	return false
}

func validIMDSContentEncoding(value string) bool {
	return value == "" || strings.EqualFold(strings.TrimSpace(value), "identity")
}
