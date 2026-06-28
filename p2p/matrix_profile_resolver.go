package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type matrixUserProfile struct {
	DisplayName string
	AvatarURL   string
}

type matrixProfileResolver interface {
	ResolveMatrixProfile(ctx context.Context, userID string) (matrixUserProfile, error)
}

type HTTPMatrixProfileResolver struct {
	BaseURL string
	Client  *http.Client
}

func NewHTTPMatrixProfileResolver(baseURL string, client *http.Client) *HTTPMatrixProfileResolver {
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &HTTPMatrixProfileResolver{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Client:  client,
	}
}

func (r *HTTPMatrixProfileResolver) ResolveMatrixProfile(ctx context.Context, userID string) (matrixUserProfile, error) {
	if strings.TrimSpace(r.BaseURL) == "" {
		return matrixUserProfile{}, fmt.Errorf("matrix base URL is required")
	}
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return matrixUserProfile{}, fmt.Errorf("matrix user ID is required")
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	path := r.BaseURL + "/_matrix/client/v3/profile/" + url.PathEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return matrixUserProfile{}, err
	}
	res, err := client.Do(req)
	if err != nil {
		return matrixUserProfile{}, err
	}
	defer func() {
		_ = res.Body.Close()
	}()
	if res.StatusCode == http.StatusNotFound {
		return matrixUserProfile{}, nil
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return matrixUserProfile{}, fmt.Errorf("matrix profile failed with status %d", res.StatusCode)
	}
	var payload struct {
		DisplayName string `json:"displayname"`
		AvatarURL   string `json:"avatar_url"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return matrixUserProfile{}, err
	}
	return matrixUserProfile{
		DisplayName: strings.TrimSpace(payload.DisplayName),
		AvatarURL:   strings.TrimSpace(payload.AvatarURL),
	}, nil
}
