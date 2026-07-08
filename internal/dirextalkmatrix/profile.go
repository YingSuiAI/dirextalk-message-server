package dirextalkmatrix

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Profile struct {
	DisplayName string
	AvatarURL   string
}

type HTTPProfileResolver struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPProfileResolver(baseURL string, client *http.Client) *HTTPProfileResolver {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &HTTPProfileResolver{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Client:  client,
	}
}

func (r *HTTPProfileResolver) ResolveMatrixProfile(ctx context.Context, userID string) (Profile, error) {
	if strings.TrimSpace(r.BaseURL) == "" {
		return Profile{}, fmt.Errorf("matrix base URL is required")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return Profile{}, fmt.Errorf("matrix user ID is required")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	path := r.BaseURL + "/_matrix/client/v3/profile/" + url.PathEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return Profile{}, err
	}
	res, err := client.Do(req)
	if err != nil {
		return Profile{}, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode == http.StatusNotFound {
		return Profile{}, nil
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return Profile{}, fmt.Errorf("matrix profile failed with status %d", res.StatusCode)
	}
	var payload struct {
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return Profile{}, err
	}
	return Profile{
		DisplayName: strings.TrimSpace(payload.DisplayName),
		AvatarURL:   strings.TrimSpace(payload.AvatarURL),
	}, nil
}
