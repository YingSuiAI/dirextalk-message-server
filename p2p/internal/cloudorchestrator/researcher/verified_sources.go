package researcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	cloudcontracts "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/cloudorchestrator/runtime"
)

const (
	githubAPIBaseURL       = "https://api.github.com"
	maxGitHubMetadataBytes = 1 << 20
	maxGitHubArchiveBytes  = 512 << 20
	maxVerifiedSources     = 4
)

var (
	errSourceVerificationUnavailable = errors.New("official source verification is unavailable")
	errSourceVerificationFailed      = errors.New("official source verification failed")
	githubNamePattern                = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,98}[A-Za-z0-9])?$`)
	gitCommitPattern                 = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// SourceVerifier turns model-proposed provenance into a content-addressed,
// independently observed source record. It never receives a cloud approval,
// provider credential, service secret, or executable Recipe artifact.
type SourceVerifier interface {
	Verify(context.Context, cloudcontracts.RecipeSourceV1) (cloudcontracts.RecipeSourceV1, error)
}

// VerifiedSourcePlanner keeps model discovery separate from source trust. New
// Recipes are rewritten with verifier-observed archive digests, SPDX license
// and retrieval time before the Orchestrator can persist them. A previously
// selected private Recipe is already immutable and is not fetched or changed.
type VerifiedSourcePlanner struct {
	planner  runtime.Planner
	verifier SourceVerifier
}

func NewVerifiedSourcePlanner(planner runtime.Planner, verifier SourceVerifier) (*VerifiedSourcePlanner, error) {
	if planner == nil || verifier == nil {
		return nil, errors.New("verified source planner configuration is invalid")
	}
	return &VerifiedSourcePlanner{planner: planner, verifier: verifier}, nil
}

func (planner *VerifiedSourcePlanner) Research(ctx context.Context, input runtime.ResearchInput) (runtime.ResearchOutput, error) {
	if planner == nil || planner.planner == nil || planner.verifier == nil || input.Validate() != nil {
		return runtime.ResearchOutput{}, errors.New("verified source planner is unavailable")
	}
	output, err := planner.planner.Research(ctx, input)
	if err != nil {
		return runtime.ResearchOutput{}, err
	}
	if output.ValidateFor(input) != nil {
		return runtime.ResearchOutput{}, errSourceVerificationFailed
	}
	if input.SelectedRecipe != nil {
		return output, nil
	}
	if len(output.Recipe.Sources) == 0 || len(output.Recipe.Sources) > maxVerifiedSources {
		return runtime.ResearchOutput{}, errSourceVerificationFailed
	}
	verified := make([]cloudcontracts.RecipeSourceV1, len(output.Recipe.Sources))
	for index, source := range output.Recipe.Sources {
		verified[index], err = planner.verifier.Verify(ctx, source)
		if err != nil {
			if errors.Is(err, errSourceVerificationUnavailable) {
				return runtime.ResearchOutput{}, runtime.Retryable("source_verification_unavailable", errSourceVerificationUnavailable)
			}
			return runtime.ResearchOutput{}, errSourceVerificationFailed
		}
	}
	output.Recipe.Sources = verified
	if output.ValidateFor(input) != nil {
		return runtime.ResearchOutput{}, errSourceVerificationFailed
	}
	return output, nil
}

// GitHubSourceVerifier supports the first generic compiler provenance path:
// an exact public GitHub repository and full immutable commit. It verifies the
// commit through the GitHub API, hashes the immutable source archive and reads
// the repository SPDX identifier. Product names and repository identities are
// never hard-coded.
type GitHubSourceVerifier struct {
	client  *http.Client
	apiBase string
	now     func() time.Time
}

func NewGitHubSourceVerifier(client *http.Client, now func() time.Time) (*GitHubSourceVerifier, error) {
	if now == nil {
		now = time.Now
	}
	if client == nil {
		client = githubVerificationHTTPClient()
	}
	if client == nil || client.Transport == nil {
		return nil, errors.New("GitHub source verifier configuration is invalid")
	}
	copyClient := *client
	return &GitHubSourceVerifier{client: &copyClient, apiBase: githubAPIBaseURL, now: now}, nil
}

func (verifier *GitHubSourceVerifier) Verify(ctx context.Context, source cloudcontracts.RecipeSourceV1) (cloudcontracts.RecipeSourceV1, error) {
	owner, repository, err := exactGitHubRepository(source.URL)
	if verifier == nil || verifier.client == nil || verifier.now == nil || err != nil || !gitCommitPattern.MatchString(source.Commit) {
		return cloudcontracts.RecipeSourceV1{}, errSourceVerificationFailed
	}
	base := strings.TrimRight(verifier.apiBase, "/") + "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repository)
	commit, err := verifier.getCommit(ctx, base+"/commits/"+source.Commit)
	if err != nil {
		return cloudcontracts.RecipeSourceV1{}, err
	}
	if commit != source.Commit {
		return cloudcontracts.RecipeSourceV1{}, errSourceVerificationFailed
	}
	license, err := verifier.getLicense(ctx, base+"/license")
	if err != nil {
		return cloudcontracts.RecipeSourceV1{}, err
	}
	digest, err := verifier.hashArchive(ctx, base+"/tarball/"+source.Commit)
	if err != nil {
		return cloudcontracts.RecipeSourceV1{}, err
	}
	verified := source
	verified.URL = "https://github.com/" + owner + "/" + repository
	verified.Commit = commit
	verified.ArtifactDigest = digest
	verified.License = license
	verified.RetrievedAt = verifier.now().UTC()
	return verified, nil
}

func (verifier *GitHubSourceVerifier) getCommit(ctx context.Context, endpoint string) (string, error) {
	var response struct {
		SHA string `json:"sha"`
	}
	if err := verifier.getJSON(ctx, endpoint, &response); err != nil {
		return "", err
	}
	response.SHA = strings.ToLower(strings.TrimSpace(response.SHA))
	if !gitCommitPattern.MatchString(response.SHA) {
		return "", errSourceVerificationFailed
	}
	return response.SHA, nil
}

func (verifier *GitHubSourceVerifier) getLicense(ctx context.Context, endpoint string) (string, error) {
	var response struct {
		License struct {
			SPDXID string `json:"spdx_id"`
		} `json:"license"`
	}
	if err := verifier.getJSON(ctx, endpoint, &response); err != nil {
		return "", err
	}
	spdx := strings.TrimSpace(response.License.SPDXID)
	if spdx == "" || spdx == "NOASSERTION" || len(spdx) > 160 || strings.ContainsAny(spdx, "\r\n\x00") {
		return "", errSourceVerificationFailed
	}
	return spdx, nil
}

func (verifier *GitHubSourceVerifier) getJSON(ctx context.Context, endpoint string, target any) error {
	response, err := verifier.do(ctx, endpoint, "application/vnd.github+json")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxGitHubMetadataBytes+1))
	if decoder.Decode(target) != nil {
		return errSourceVerificationFailed
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return errSourceVerificationFailed
	}
	return nil
}

func (verifier *GitHubSourceVerifier) hashArchive(ctx context.Context, endpoint string) (string, error) {
	response, err := verifier.do(ctx, endpoint, "application/octet-stream")
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(response.Body, maxGitHubArchiveBytes+1))
	if err != nil {
		return "", errSourceVerificationUnavailable
	}
	if written == 0 || written > maxGitHubArchiveBytes {
		return "", errSourceVerificationFailed
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func (verifier *GitHubSourceVerifier) do(ctx context.Context, endpoint, accept string) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, errSourceVerificationFailed
	}
	request.Header.Set("Accept", accept)
	request.Header.Set("User-Agent", "Dirextalk-Cloud-Researcher/1")
	response, err := verifier.client.Do(request)
	if err != nil {
		return nil, errSourceVerificationUnavailable
	}
	if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
		response.Body.Close()
		return nil, errSourceVerificationUnavailable
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, errSourceVerificationFailed
	}
	return response, nil
}

func exactGitHubRepository(raw string) (string, string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || strings.ToLower(parsed.Hostname()) != "github.com" || parsed.Port() != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", "", errSourceVerificationFailed
	}
	parts := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	if len(parts) != 2 {
		return "", "", errSourceVerificationFailed
	}
	owner, err := url.PathUnescape(parts[0])
	if err != nil {
		return "", "", errSourceVerificationFailed
	}
	repository, err := url.PathUnescape(strings.TrimSuffix(parts[1], ".git"))
	if err != nil || !githubNamePattern.MatchString(owner) || !githubNamePattern.MatchString(repository) {
		return "", "", errSourceVerificationFailed
	}
	return owner, repository, nil
}

func githubVerificationHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           publicHTTPSDialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 20 * time.Second,
		IdleConnTimeout:       30 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   5 * time.Minute,
		CheckRedirect: func(request *http.Request, via []*http.Request) error {
			if len(via) >= 3 || request.URL.Scheme != "https" {
				return errSourceVerificationFailed
			}
			host := strings.ToLower(request.URL.Hostname())
			if host != "api.github.com" && host != "codeload.github.com" {
				return errSourceVerificationFailed
			}
			return nil
		},
	}
}

func publicHTTPSDialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port != "443" {
		return nil, errSourceVerificationFailed
	}
	addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, errSourceVerificationUnavailable
	}
	for _, address := range addresses {
		if !publicSourceIP(address.IP) {
			return nil, errSourceVerificationFailed
		}
	}
	dialer := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	var last error
	for _, resolved := range addresses {
		connection, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
		if dialErr == nil {
			return connection, nil
		}
		last = dialErr
	}
	return nil, fmt.Errorf("%w: %v", errSourceVerificationUnavailable, last)
}

func publicSourceIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() {
		return false
	}
	if ipv4 := ip.To4(); ipv4 != nil && ipv4[0] == 100 && ipv4[1]&0xc0 == 64 {
		return false
	}
	return true
}
