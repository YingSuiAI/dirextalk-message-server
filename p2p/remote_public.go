package p2p

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"

	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
	channelsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/channels"
	membersmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/members"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func newRemotePublicHTTPClient(insecureSkipTLSVerify bool) *http.Client {
	transport := &http.Transport{}
	if insecureSkipTLSVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // explicitly enabled for local self-signed test nodes
	}
	return &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

func matrixRoomIDQuery(query string) bool {
	trimmed := strings.TrimSpace(query)
	_, ok := roomServerFromMatrixRoomID(trimmed)
	return ok
}

type publicJoinPreflightChannelContextKey struct{}

func withPublicJoinPreflightChannel(ctx context.Context, value channel) context.Context {
	return context.WithValue(ctx, publicJoinPreflightChannelContextKey{}, value)
}

func publicJoinPreflightChannel(ctx context.Context, roomID string) (channel, bool) {
	value, ok := ctx.Value(publicJoinPreflightChannelContextKey{}).(channel)
	return value, ok && value.RoomID == strings.TrimSpace(roomID)
}

func (s *Service) remotePublicChannelGet(ctx context.Context, channelID, roomID string, params map[string]any) (channel, bool, *apiError) {
	roomID = strings.TrimSpace(roomID)
	if roomID == "" {
		return channel{}, false, nil
	}
	roomServer, ok := roomServerFromMatrixRoomID(roomID)
	if !ok {
		return channel{}, false, badRequest("valid Matrix room_id is required")
	}
	if roomServer == s.serverName {
		return channel{}, false, nil
	}
	var ch channel
	status, err := s.remotePublicAction(ctx, roomServer, "channels.public.get", map[string]any{
		"room_id":              roomID,
		"channel_id":           fallbackString(channelID, roomID),
		"remote_node_base_url": remoteNodeBaseURLParam(params),
	}, &ch)
	if status == http.StatusNotFound {
		return channel{}, false, nil
	}
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return channel{}, false, statusError(status, err.Error())
		}
		return channel{}, false, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		return channel{}, false, statusError(status, "target node public channel lookup failed")
	}
	if !strings.EqualFold(ch.Visibility, "public") {
		return channel{}, false, nil
	}
	if ch.RoomID == "" {
		ch.RoomID = roomID
	}
	if ch.ChannelID == "" {
		ch.ChannelID = ch.RoomID
	}
	if err := s.saveChannel(ctx, ch); err != nil {
		return channel{}, false, internalError(err)
	}
	return ch, true, nil
}

func (s *Service) remoteUserPublicChannels(ctx context.Context, userID string, params map[string]any) (channelsmodule.RemoteUserChannelsResult, *apiError) {
	ownerNode := domainFromMXID(userID)
	if ownerNode == "" {
		return channelsmodule.RemoteUserChannelsResult{}, badRequest("valid user_id is required")
	}
	var remote struct {
		UserID   string    `json:"user_id"`
		Channels []channel `json:"channels"`
		Results  []channel `json:"results"`
	}
	status, err := s.remotePublicAction(ctx, ownerNode, "users.public_channels", params, &remote)
	if err != nil {
		if status != 0 && status != http.StatusBadGateway {
			return channelsmodule.RemoteUserChannelsResult{}, statusError(status, err.Error())
		}
		return channelsmodule.RemoteUserChannelsResult{}, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		return channelsmodule.RemoteUserChannelsResult{}, statusError(status, "target node public channels lookup failed")
	}
	channels := remote.Channels
	if channels == nil {
		channels = remote.Results
	}
	return channelsmodule.RemoteUserChannelsResult{UserID: remote.UserID, Channels: channels}, nil
}

func (s *Service) remoteChannelJoinRequest(ctx context.Context, params map[string]any) (map[string]any, bool, *apiError) {
	roomID := trimString(params["room_id"])
	if roomID == "" {
		return nil, false, nil
	}
	roomServer, ok := roomServerFromMatrixRoomID(roomID)
	if !ok {
		return nil, false, badRequest("valid Matrix room_id is required")
	}
	if roomServer == s.serverName {
		return nil, false, nil
	}
	settlementCtx, cancel := actionbase.SettlementContext(ctx)
	defer cancel()
	ctx = settlementCtx
	ch, found := publicJoinPreflightChannel(ctx, roomID)
	if !found {
		var apiErr *apiError
		ch, found, apiErr = s.remotePublicChannelGet(ctx, trimString(params["channel_id"]), roomID, params)
		if apiErr != nil {
			return nil, false, apiErr
		}
	}
	if !found {
		return nil, false, statusError(http.StatusNotFound, "channel not found")
	}
	userID := firstMemberID(params)
	if userID == "" {
		s.mu.Lock()
		userID = s.ownerMXID
		s.mu.Unlock()
	}
	if userID == "" {
		return nil, false, badRequest("user_id is required")
	}
	localMember, ok, err := s.lookupMember(ctx, roomID, userID)
	if err != nil {
		return nil, false, internalError(err)
	}
	previousRequestID := localMember.RequestID
	previousMembership := localMember.Membership
	restartGeneration := false
	if !ok {
		localMember = s.memberRecordFor(roomID, ch.ChannelID, userID)
		localMember.Membership = "pending"
	} else if strings.EqualFold(strings.TrimSpace(localMember.Membership), "join") ||
		strings.EqualFold(strings.TrimSpace(localMember.Membership), "joined") {
		joined, joinedErr := s.matrixMemberJoined(ctx, roomID, userID)
		if joinedErr != nil {
			return nil, false, internalError(joinedErr)
		}
		if joined {
			localMember.ChannelID = ch.ChannelID
			ch.MemberStatus = "join"
			ch.Role = normalizeProductMemberRole(localMember.Role)
			ch.IsOwned = productOwnerRole(localMember.Role)
			return map[string]any{"status": "joined", "room_id": roomID, "member": localMember, "channel": ch}, true, nil
		}
		localMember.Membership = "joining"
	}
	localMember.RoomID = roomID
	localMember.ChannelID = ch.ChannelID
	requestID := trimString(params["request_id"])
	if requestID == "" {
		if operation, ok := recoverableOperationSnapshot(ctx); ok {
			requestID = fallbackString(operation.RequestID, operation.OperationID)
		}
	}
	if requestID != "" && localMember.RequestID != requestID && channelJoinGenerationMayRestart(localMember.Membership) {
		restartGeneration = true
		localMember.RequestID = requestID
		localMember.JoinedAt = time.Now().UTC().UnixMilli()
		localMember.Membership = "pending"
	} else if localMember.RequestID == "" {
		localMember.RequestID = requestID
	} else {
		requestID = localMember.RequestID
	}
	if strings.TrimSpace(localMember.Membership) == "" {
		localMember.Membership = "pending"
	}
	localMember.Role = fallbackString(localMember.Role, "member")
	currentRequesterBaseURL := s.publicP2PBaseURL()
	if currentRequesterBaseURL != "" && (restartGeneration || localMember.RequesterNodeBaseURL == "") {
		localMember.RequesterNodeBaseURL = currentRequesterBaseURL
	}
	membersmodule.ApplyMemberProfile(&localMember, actionbase.Params(params))
	if restartGeneration {
		saved, saveErr := s.saveMemberIfState(ctx, localMember, previousRequestID, previousMembership)
		if saveErr != nil {
			return nil, false, internalError(saveErr)
		}
		if !saved {
			current, found, lookupErr := s.lookupMember(ctx, roomID, userID)
			if lookupErr != nil {
				return nil, false, internalError(lookupErr)
			}
			if !found {
				return nil, false, internalError(errors.New("channel join generation disappeared during restart"))
			}
			result, currentErr := s.currentChannelJoinResult(ctx, current)
			return result, true, currentErr
		}
	} else if ok {
		saved, saveErr := s.saveMemberIfState(ctx, localMember, previousRequestID, previousMembership)
		if saveErr != nil {
			return nil, false, internalError(saveErr)
		}
		if !saved {
			current, found, lookupErr := s.lookupMember(ctx, roomID, userID)
			if lookupErr != nil {
				return nil, false, internalError(lookupErr)
			}
			if !found {
				return nil, false, internalError(errors.New("channel join generation disappeared before dispatch"))
			}
			result, currentErr := s.currentChannelJoinResult(ctx, current)
			return result, true, currentErr
		}
	} else if saveErr := s.saveMember(ctx, localMember); saveErr != nil {
		return nil, false, internalError(saveErr)
	}
	forwardParams := cloneParams(params)
	if requestID != "" {
		forwardParams["request_id"] = requestID
	}
	if trimString(forwardParams["requester_node_base_url"]) == "" && localMember.RequesterNodeBaseURL != "" {
		forwardParams["requester_node_base_url"] = localMember.RequesterNodeBaseURL
	}
	var remote struct {
		Status    string       `json:"status"`
		Member    memberRecord `json:"member"`
		Channel   channel      `json:"channel"`
		Error     string       `json:"error"`
		ErrorCode string       `json:"error_code"`
	}
	status, err := s.remotePublicAction(ctx, roomServer, "channels.public.join_request", forwardParams, &remote)
	if err != nil {
		if terminalErr := remoteJoinResultTerminalError(status, err); terminalErr != nil {
			return nil, false, terminalErr
		}
		if status != 0 && status != http.StatusBadGateway {
			return nil, false, statusError(status, err.Error())
		}
		return nil, false, statusError(http.StatusBadGateway, err.Error())
	}
	if status != http.StatusOK {
		return nil, false, statusError(status, "target node public channel join request failed")
	}
	member := remote.Member
	if member.RoomID == "" {
		member.RoomID = roomID
	}
	if member.ChannelID == "" {
		member.ChannelID = ch.ChannelID
	}
	if member.Membership == "" {
		member.Membership = fallbackString(remote.Status, "pending")
	}
	if member.Role == "" {
		member.Role = "member"
	}
	// The requester owns the durable generation. Older owner nodes may omit
	// request_id, and a remote response must never replace the local canonical
	// value selected before dispatch.
	member.RequestID = localMember.RequestID
	if member.RequesterNodeBaseURL == "" {
		member.RequesterNodeBaseURL = localMember.RequesterNodeBaseURL
	}
	remoteStatus := fallbackString(remote.Status, member.Membership)
	if strings.EqualFold(remoteStatus, "join_failed") || strings.EqualFold(remoteStatus, "approved") ||
		strings.EqualFold(remoteStatus, "joining") || strings.EqualFold(remoteStatus, "joined") {
		localJoin := localMember
		localJoin.RoomID = roomID
		localJoin.ChannelID = ch.ChannelID
		localJoin.Membership = "approved"
		localJoin.Role = fallbackString(localJoin.Role, "member")
		membersmodule.ApplyMemberProfile(&localJoin, actionbase.Params(params))
		joinParams := cloneParams(params)
		joinParams["server_names"] = channelJoinServerNames(params["server_names"], roomID)
		attempt, apiErr := s.joinAndProjectRetainedRoomGeneration(ctx, "channel", &localJoin, joinParams)
		localJoin = attempt.Member
		if attempt.Stale {
			result, currentErr := s.currentChannelJoinResult(ctx, localJoin)
			return result, true, currentErr
		}
		if apiErr == nil {
			ch.MemberStatus = "join"
			ch.Role = normalizeProductMemberRole(localJoin.Role)
			ch.IsOwned = productOwnerRole(localJoin.Role)
			return map[string]any{"status": "joined", "room_id": localJoin.RoomID, "member": localJoin, "channel": ch}, true, nil
		}
		if !attempt.Final {
			return nil, false, apiErr
		}
		if attempt.Busy || strings.EqualFold(strings.TrimSpace(localJoin.Membership), "joining") {
			return map[string]any{
				"status": "joining", "member": localJoin, "channel": ch,
				"error": apiErr.Error, "error_code": fallbackString(apiErr.Code, actionbase.MatrixJoinUnconfirmedCode),
			}, true, nil
		}
		if strings.EqualFold(strings.TrimSpace(localJoin.Membership), "join") ||
			strings.EqualFold(strings.TrimSpace(localJoin.Membership), "joined") {
			return map[string]any{
				"status": "joined", "room_id": localJoin.RoomID, "member": localJoin,
				"channel": ch, "error": apiErr.Error, "error_code": actionbase.OperationRecoveryCode,
			}, true, nil
		}
		return map[string]any{
			"status": "join_failed", "member": localJoin, "channel": ch,
			"error": apiErr.Error, "error_code": actionbase.MatrixJoinFailedCode,
		}, true, nil
	}
	if current, stale, apiErr := s.persistRemoteChannelJoinGeneration(
		ctx,
		member,
		localMember.RequestID,
		localMember.Membership,
	); apiErr != nil {
		return nil, false, apiErr
	} else if stale {
		return current, true, nil
	}
	if remote.Channel.ChannelID != "" {
		ch = remote.Channel
		if err := s.saveChannel(ctx, ch); err != nil {
			return nil, false, internalError(err)
		}
	}
	result := map[string]any{"status": remoteStatus, "member": member, "channel": ch}
	if remote.Error != "" {
		result["error"] = remote.Error
	}
	if remote.ErrorCode != "" {
		result["error_code"] = remote.ErrorCode
	}
	return result, true, nil
}

func channelJoinGenerationMayRestart(membership string) bool {
	switch strings.ToLower(strings.TrimSpace(membership)) {
	case "reject", "rejected", "leave", "left":
		return true
	default:
		return false
	}
}

func (s *Service) remotePublicAction(ctx context.Context, serverName, action string, params map[string]any, out any) (int, error) {
	base, apiErr := s.remoteNodeBaseURL(serverName, params)
	if apiErr != nil {
		return apiErr.Status, fmt.Errorf("%s", apiErr.Error)
	}
	uri := *base
	basePath := strings.TrimRight(uri.Path, "/")
	uri.Path = basePath + "/query"
	body, err := json.Marshal(envelope{Action: action, Params: remotePublicForwardParams(params)})
	if err != nil {
		return http.StatusBadRequest, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri.String(), bytes.NewReader(body))
	if err != nil {
		return http.StatusBadGateway, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	client := s.remoteHTTPClient
	if client == nil {
		client = newRemotePublicHTTPClient(false)
	}
	res, err := client.Do(req)
	if err != nil {
		return http.StatusBadGateway, err
	}
	if res.Body != nil {
		defer func() { _ = res.Body.Close() }()
	}
	if res.StatusCode != http.StatusOK {
		return res.StatusCode, remotePublicError(res.Body, res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		return http.StatusBadGateway, err
	}
	return res.StatusCode, nil
}

func (s *Service) remoteNodeBaseURL(serverName string, params map[string]any) (*url.URL, *apiError) {
	serverName = strings.TrimSpace(serverName)
	if serverName == "" {
		return nil, badRequest("valid Matrix room_id is required")
	}
	raw := remoteNodeBaseURLParam(params)
	if raw == "" {
		return nil, badRequest("remote_node_base_url is required for remote Matrix room_id")
	}
	base, ok := normalizeRemoteNodeBaseURL(raw)
	if !ok {
		return nil, badRequest("valid remote_node_base_url is required")
	}
	if !s.remoteAllowPrivate && remoteNodeBaseURLUsesPrivateHost(base) {
		return nil, badRequest("remote_node_base_url host must be public")
	}
	return base, nil
}

func normalizeRemoteNodeBaseURL(raw string) (*url.URL, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return nil, false
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, false
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/_p2p"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, true
}

func remoteNodeBaseURLParam(params map[string]any) string {
	return trimString(params["remote_node_base_url"])
}

func remotePublicForwardParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	delete(out, "remote_node_base_url")
	return out
}

func remoteNodeBaseURLUsesPrivateHost(base *url.URL) bool {
	if base == nil {
		return true
	}
	host := strings.TrimSpace(base.Hostname())
	if host == "" {
		return true
	}
	lower := strings.ToLower(host)
	if lower == "localhost" || strings.HasSuffix(lower, ".localhost") {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsPrivate() ||
		addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsUnspecified() ||
		!addr.IsGlobalUnicast()
}

func roomServerFromMatrixRoomID(roomID string) (string, bool) {
	parsed, err := spec.NewRoomID(strings.TrimSpace(roomID))
	if err != nil || parsed == nil {
		return "", false
	}
	serverName := string(parsed.Domain())
	if strings.TrimSpace(serverName) == "" || strings.Contains(serverName, "://") || strings.ContainsAny(serverName, "/?#@") {
		return "", false
	}
	return serverName, true
}

func remotePublicError(body io.Reader, status int) error {
	raw, _ := io.ReadAll(io.LimitReader(body, 4096))
	message := strings.TrimSpace(string(raw))
	responseErr := &remotePublicActionError{Status: status}
	if message != "" {
		var payload map[string]any
		if json.Unmarshal(raw, &payload) == nil {
			for _, key := range []string{"error", "message"} {
				if value := trimString(payload[key]); value != "" {
					message = value
					break
				}
			}
			responseErr.Code = fallbackString(trimString(payload["error_code"]), trimString(payload["code"]))
			responseErr.OperationID = trimString(payload["operation_id"])
			responseErr.CurrentRoomID = trimString(payload["current_room_id"])
		}
	}
	if message == "" {
		message = http.StatusText(status)
	}
	responseErr.Message = message
	return responseErr
}

type remotePublicActionError struct {
	Status        int
	Message       string
	Code          string
	OperationID   string
	CurrentRoomID string
}

func (e *remotePublicActionError) Error() string {
	return fmt.Sprintf("target node public action failed: status=%d error=%s", e.Status, e.Message)
}
