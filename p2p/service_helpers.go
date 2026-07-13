package p2p

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

func (s *Service) sessionLocked() map[string]any {
	return map[string]any{
		"access_token":   s.accessToken,
		"device_id":      cleanMatrixDeviceID(s.matrixDeviceID),
		"agent_token":    s.agentToken,
		"user_id":        s.ownerMXID,
		"homeserver":     s.homeserver,
		"agent_room_id":  s.agentRoomID,
		"system_room_id": s.systemRoomID,
		"password":       s.password,
		"initialized":    s.initialized,
	}
}

func (s *Service) portalStateLocked() portalState {
	return portalState{
		Initialized:    s.initialized,
		Password:       s.password,
		AccessToken:    s.accessToken,
		MatrixDeviceID: cleanMatrixDeviceID(s.matrixDeviceID),
		AgentToken:     s.agentToken,
		OwnerMXID:      s.ownerMXID,
		AgentRoomID:    s.agentRoomID,
		SystemRoomID:   s.systemRoomID,
		Profile:        s.profile,
		AgentConfig:    s.agentConfig,
		ClientBuild:    s.clientBuild,
	}
}

func trimString(value any) string {
	return actionbase.String(value)
}

// cloneAnyMap and jsonValue are shared root transport/Agent helpers. They stay
// outside the plugin module because MCP, realtime, release, and Native Agent
// adapters also depend on their legacy shallow-copy and JSON behavior.
func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func jsonValue(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(data)
}

func normalizedStringSlice(values []string) []string {
	return actionbase.Strings(values)
}

func stringSliceParam(value any) []string {
	return actionbase.Strings(value)
}

func contactRequestRemark(params map[string]any) string {
	for _, key := range []string{"remark", "request_message", "message", "reason"} {
		if value := trimString(params[key]); value != "" {
			return value
		}
	}
	return ""
}

func int64Param(value any) int64 {
	return actionbase.Int64(value)
}

func channelJoinServerNames(value any, roomID string) []string {
	names := stringSliceParam(value)
	result := make([]string, 0, len(names))
	for _, name := range names {
		if text := strings.TrimSpace(name); text != "" {
			result = append(result, text)
		}
	}
	if len(result) > 0 {
		return result
	}
	if serverName, ok := roomServerFromMatrixRoomID(roomID); ok {
		return []string{serverName}
	}
	return nil
}

func boolParam(value any) bool {
	return actionbase.Bool(value)
}

func boolMapParam(value any) map[string]bool {
	return actionbase.BoolMap(value)
}

func domainFromMXID(mxid string) string {
	return dirextalkdomain.DomainFromMXID(mxid)
}

func localpartFromMXID(mxid string) string {
	localpart := strings.TrimPrefix(strings.TrimSpace(mxid), "@")
	if idx := strings.Index(localpart, ":"); idx >= 0 {
		localpart = localpart[:idx]
	}
	return strings.TrimSpace(localpart)
}

func domainFromMatrixID(id, sigil string) string {
	trimmed := strings.TrimPrefix(id, sigil)
	if idx := strings.Index(trimmed, ":"); idx >= 0 {
		idx += len(id) - len(trimmed)
		if idx+1 >= len(id) {
			return ""
		}
		return id[idx+1:]
	}
	return ""
}

func displayNameFromMXID(mxid string) string {
	return dirextalkdomain.DisplayNameFromMXID(mxid)
}

func firstMemberID(params map[string]any) string {
	values := actionbase.Params(params)
	if userID := values.FirstString("user_id", "user_mxid", "peer_mxid", "mxid"); userID != "" {
		return userID
	}
	return values.FirstListString("user_ids", "user_mxids", "peer_mxids", "invitees")
}

func memberHidden(membership string) bool {
	return dirextalkdomain.MemberHidden(membership)
}

func memberRemoved(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "remove", "removed", "ban", "banned":
		return true
	default:
		return false
	}
}

func memberLeft(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "leave", "left":
		return true
	default:
		return false
	}
}

func contactAccepted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "accepted")
}

func contactDeleted(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "deleted")
}

func contactPendingInbound(status string) bool {
	return strings.EqualFold(strings.TrimSpace(status), "pending_inbound")
}

func randomToken(prefix string) string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return prefix + "_" + hex.EncodeToString(buf[:])
}

func randomNumericPassword() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return fmt.Sprintf("%08d", time.Now().UnixNano()%100000000)
	}
	for i := range buf {
		buf[i] = '0' + (buf[i] % 10)
	}
	return string(buf[:])
}

func cleanMatrixDeviceID(deviceID string) string {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return matrixPortalDeviceID
	}
	return deviceID
}

func requestedMatrixDeviceID(params map[string]any) string {
	deviceID := strings.TrimSpace(trimString(params["device_id"]))
	if deviceID != "" {
		return cleanMatrixDeviceID(deviceID)
	}
	return "PORTALIM" + strings.TrimPrefix(randomToken("device"), "device_")
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	return fallback
}

func defaultPortalPassword() string {
	if password := strings.TrimSpace(os.Getenv("P2P_PORTAL_PASSWORD")); password != "" {
		return password
	}
	return randomNumericPassword()
}

func portalCredentialsFilePath() string {
	return strings.TrimSpace(os.Getenv("P2P_PORTAL_CREDENTIALS_FILE"))
}

func (s *Service) writePortalCredentialsFile() error {
	path := strings.TrimSpace(portalCredentialsFilePath())
	if path == "" {
		return nil
	}
	path = filepath.Clean(path)
	if path == "." || filepath.Base(path) == "." {
		return fmt.Errorf("portal credentials file path is required")
	}
	s.mu.Lock()
	credentials := portalCredentialsFile{
		Version:      1,
		GeneratedAt:  time.Now().UTC(),
		OwnerUserID:  s.ownerMXID,
		UserID:       s.ownerMXID,
		Homeserver:   s.homeserver,
		AccessToken:  s.accessToken,
		DeviceID:     matrixPortalDeviceID,
		AgentToken:   s.agentToken,
		Password:     s.password,
		AgentRoomID:  s.agentRoomID,
		SystemRoomID: s.systemRoomID,
	}
	s.mu.Unlock()

	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create portal credentials directory: %w", err)
	}
	temp, err := os.CreateTemp(parent, ".p2p-portal-*.json")
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
	if err := encoder.Encode(credentials); err != nil {
		return fmt.Errorf("encode portal credentials: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("flush portal credentials: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close portal credentials: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish portal credentials: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	cleanup = false
	return nil
}

func (s *Service) writeAccountDeletedCredentialsFile() error {
	path := strings.TrimSpace(portalCredentialsFilePath())
	if path == "" {
		return nil
	}
	path = filepath.Clean(path)
	if path == "." || filepath.Base(path) == "." {
		return fmt.Errorf("portal credentials file path is required")
	}
	s.mu.Lock()
	tombstone := map[string]any{
		"version":       1,
		"deprovisioned": true,
		"deleted_at":    time.Now().UTC(),
		"owner_user_id": s.ownerMXID,
		"user_id":       s.ownerMXID,
		"homeserver":    s.homeserver,
	}
	s.mu.Unlock()

	parent := filepath.Dir(path)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return fmt.Errorf("create portal credentials directory: %w", err)
	}
	temp, err := os.CreateTemp(parent, ".p2p-portal-deleted-*.json")
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
	if err := encoder.Encode(tombstone); err != nil {
		return fmt.Errorf("encode portal deprovision marker: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("flush portal deprovision marker: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close portal deprovision marker: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("publish portal deprovision marker: %w", err)
	}
	_ = os.Chmod(path, 0o600)
	cleanup = false
	return nil
}
