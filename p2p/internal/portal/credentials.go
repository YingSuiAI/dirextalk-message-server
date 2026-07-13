package portal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Credentials struct {
	Version      int       `json:"version"`
	GeneratedAt  time.Time `json:"generated_at"`
	OwnerUserID  string    `json:"owner_user_id"`
	UserID       string    `json:"user_id"`
	Homeserver   string    `json:"homeserver"`
	AccessToken  string    `json:"access_token"`
	DeviceID     string    `json:"device_id"`
	AgentToken   string    `json:"agent_token"`
	Password     string    `json:"password"`
	AgentRoomID  string    `json:"agent_room_id"`
	SystemRoomID string    `json:"system_room_id"`
}

type DeletedCredentials struct {
	Version       int       `json:"version"`
	Deprovisioned bool      `json:"deprovisioned"`
	DeletedAt     time.Time `json:"deleted_at"`
	OwnerUserID   string    `json:"owner_user_id"`
	UserID        string    `json:"user_id"`
	Homeserver    string    `json:"homeserver"`
}

func CredentialsFilePath() string {
	return strings.TrimSpace(os.Getenv("P2P_PORTAL_CREDENTIALS_FILE"))
}

func WriteCurrent(path string, credentials Credentials) error {
	return writeAtomicJSON(path, ".p2p-portal-*.json", "portal credentials", credentials)
}

func WriteDeleted(path string, credentials DeletedCredentials) error {
	return writeAtomicJSON(path, ".p2p-portal-deleted-*.json", "portal deprovision marker", credentials)
}

func writeAtomicJSON(path, pattern, contentName string, value any) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	path = filepath.Clean(path)
	if path == "." || filepath.Base(path) == "." {
		return fmt.Errorf("portal credentials file path is required")
	}
	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create portal credentials directory: %w", err)
	}
	temp, err := os.CreateTemp(parent, pattern)
	if err != nil {
		return fmt.Errorf("create portal credentials temp file: %w", err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()
	_ = temp.Chmod(0o600)
	encoder := json.NewEncoder(temp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode %s: %w", contentName, err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("flush %s: %w", contentName, err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", contentName, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish %s: %w", contentName, err)
	}
	_ = os.Chmod(path, 0o600)
	cleanup = false
	return nil
}
