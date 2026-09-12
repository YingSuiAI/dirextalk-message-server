package p2p

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkmcp"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/types"
	"github.com/google/uuid"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func (s *Service) groupAgentStore() (dirextalkdomain.GroupAgentStore, error) {
	store, ok := s.store.(dirextalkdomain.GroupAgentStore)
	if !ok {
		return nil, errors.New("group Agent store is unavailable")
	}
	return store, nil
}

func (s *Service) groupAgentRoom(ctx context.Context, roomID string) (dirextalktransport.GroupAgentRoom, error) {
	if reader, ok := s.transport.(dirextalktransport.GroupAgentReadPort); ok {
		return reader.ReadGroupAgentRoom(ctx, roomID)
	}
	if s.transport != nil {
		return dirextalktransport.GroupAgentRoom{}, errors.New("authoritative group Agent Matrix reader is unavailable")
	}
	// Only the explicit no-transport test service uses projections. Production
	// construction requires PostgreSQL and the Dendrite transport/read boundary.
	_, found, err := s.groupByRoom(ctx, roomID)
	if err != nil {
		return dirextalktransport.GroupAgentRoom{}, err
	}
	out := dirextalktransport.GroupAgentRoom{RoomID: roomID, IsGroup: found, Joined: map[string]bool{}}
	members, err := s.store.ListMembers(ctx, roomID, "")
	if err != nil {
		return out, err
	}
	for _, m := range members {
		out.Joined[m.UserID] = m.Membership == "join"
		if m.Role == "owner" && m.Membership == "join" {
			out.OwnerMXID = m.UserID
		}
	}
	return out, nil
}

func groupYingMXID(owner string) string {
	_, server, err := gomatrixserverlib.SplitID('@', owner)
	if err != nil || server == "" {
		return ""
	}
	return "@ying:" + string(server)
}
func defaultGroupAgentBinding(roomID, owner string) dirextalkdomain.GroupAgentBinding {
	return dirextalkdomain.GroupAgentBinding{RoomID: roomID, OwnerMXID: owner, AgentMXID: groupYingMXID(owner), DisplayName: "Ying", MemberPolicy: "all_joined", Status: "disabled"}
}

func (s *Service) groupAgentGet(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if _, err := spec.NewRoomID(roomID); err != nil {
		return nil, badRequest("valid room_id is required")
	}
	room, err := s.groupAgentRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if !room.IsGroup || room.Dissolved || !room.Joined[s.OwnerMXID()] {
		return nil, statusError(403, "joined group membership is required")
	}
	store, err := s.groupAgentStore()
	if err != nil {
		return nil, internalError(err)
	}
	b, found, err := store.GetGroupAgentBinding(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	if found && b.OwnerMXID == s.OwnerMXID() {
		if b.Enabled && (room.OwnerMXID != b.OwnerMXID || !room.Joined[b.AgentMXID] && s.transport != nil || b.AccountGeneration != s.accountGeneration) {
			if err = s.invalidateGroupAgent(ctx, roomID); err != nil {
				return nil, internalError(err)
			}
			b.Enabled = false
			b.Status = "disabled"
			b.Revision++
		}
		return b, nil
	}
	if room.Binding != nil {
		b = *room.Binding
		if b.RoomID == roomID && b.OwnerMXID == room.OwnerMXID && b.AgentMXID == groupYingMXID(room.OwnerMXID) && b.MemberPolicy == "all_joined" && b.Revision > 0 && room.Joined[b.AgentMXID] {
			return b, nil
		}
	}
	return defaultGroupAgentBinding(roomID, room.OwnerMXID), nil
}

func (s *Service) groupAgentUpdate(ctx context.Context, params map[string]any) (any, *apiError) {
	roomID := trimString(params["room_id"])
	if _, err := spec.NewRoomID(roomID); err != nil {
		return nil, badRequest("valid room_id is required")
	}
	enabled, ok := params["enabled"].(bool)
	if !ok {
		return nil, badRequest("enabled must be a boolean")
	}
	for key := range params {
		if key != "room_id" && key != "enabled" && key != "expected_revision" {
			return nil, badRequest("unknown group Agent field")
		}
	}
	expected := int64(-1)
	if raw, exists := params["expected_revision"]; exists {
		var valid bool
		expected, valid = groupAgentInteger(raw)
		if !valid || expected < 0 {
			return nil, badRequest("expected_revision must be a non-negative integer")
		}
	}
	room, err := s.groupAgentRoom(ctx, roomID)
	if err != nil {
		return nil, internalError(err)
	}
	owner := s.OwnerMXID()
	if !room.IsGroup || room.Dissolved || !room.Joined[owner] || room.OwnerMXID != owner {
		return nil, statusError(403, "current joined group owner is required")
	}
	if enabled && s.accountGeneration <= 0 {
		return nil, statusError(503, "Native Ying account generation is unavailable")
	}
	store, err := s.groupAgentStore()
	if err != nil {
		return nil, internalError(err)
	}
	var result dirextalkdomain.GroupAgentBinding
	err = store.MutateGroupAgentBinding(ctx, roomID, func(b *dirextalkdomain.GroupAgentBinding) error {
		if expected >= 0 && b.Revision != expected {
			return dirextalkdomain.ErrGroupAgentConflict
		}
		if b.OwnerMXID != "" && b.OwnerMXID != owner && b.Enabled {
			return dirextalkdomain.ErrGroupAgentConflict
		}
		if b.Revision == 0 {
			*b = defaultGroupAgentBinding(roomID, owner)
		}
		if b.Enabled != enabled || b.OwnerMXID != owner || b.AccountGeneration != s.accountGeneration {
			if enabled {
				if err := s.ensureGroupYingJoined(ctx, roomID, owner); err != nil {
					return err
				}
			}
			b.Revision++
			b.Enabled = enabled
			b.OwnerMXID = owner
			b.AgentMXID = groupYingMXID(owner)
			b.AccountGeneration = s.accountGeneration
			b.Status = "disabled"
			if enabled {
				b.Status = "enabled"
				b.EnabledAt = time.Now().UnixMilli()
			}
		}
		result = *b
		return nil
	})
	if errors.Is(err, dirextalkdomain.ErrGroupAgentConflict) {
		return nil, statusError(409, err.Error())
	}
	if err != nil {
		return nil, internalError(err)
	}
	// Local revocation commits before Matrix projection/leave, so a failed
	// projection can never keep accepting or publishing late requests.
	if err = s.publishGroupAgentState(ctx, result); err != nil {
		return nil, codedError(502, "group_agent_state_sync_failed", "group Agent setting was saved but Matrix state sync failed")
	}
	if !enabled && s.transport != nil && room.Joined[result.AgentMXID] {
		if err = s.transport.LeaveRoom(ctx, LeaveRoomRequest{RoomID: roomID, UserMXID: result.AgentMXID}); err != nil {
			return nil, codedError(502, "group_agent_state_sync_failed", "group Agent disabled but Matrix leave is pending")
		}
	}
	return result, nil
}

func groupAgentInteger(raw any) (int64, bool) {
	switch n := raw.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		if n >= 0 && n < math.MaxInt64 && math.Trunc(n) == n {
			return int64(n), true
		}
	}
	return 0, false
}

func (s *Service) ensureGroupYingJoined(ctx context.Context, roomID, owner string) error {
	if s.transport == nil {
		return nil
	}
	agent := groupYingMXID(owner)
	updater, ok := s.sessions.(interface {
		EnsureNativeGroupAgentIdentity(context.Context, string) error
	})
	if !ok {
		return errors.New("Native Ying Matrix identity provisioner is unavailable")
	}
	// Creates the real Matrix account/profile, without issuing any login token.
	s.matrixSessionMu.Lock()
	identityErr := updater.EnsureNativeGroupAgentIdentity(ctx, agent)
	s.matrixSessionMu.Unlock()
	if err := identityErr; err != nil {
		return err
	}
	room, err := s.groupAgentRoom(ctx, roomID)
	if err != nil {
		return err
	}
	if room.Joined[agent] {
		return nil
	}
	if err = s.transport.InviteUser(ctx, InviteUserRequest{RoomID: roomID, InviterMXID: owner, InviteeMXID: agent, Reason: "Owner enabled group Ying", GroupAgentControl: true}); err != nil {
		return err
	}
	if _, err = s.transport.JoinRoom(ctx, JoinRoomRequest{RoomIDOrAlias: roomID, UserMXID: agent, DisplayName: "Ying"}); err != nil {
		return err
	}
	room, err = s.groupAgentRoom(ctx, roomID)
	if err != nil {
		return err
	}
	if !room.Joined[agent] {
		return errors.New("Native Ying Matrix join is not yet authoritative")
	}
	return nil
}

func (s *Service) publishGroupAgentState(ctx context.Context, b dirextalkdomain.GroupAgentBinding) error {
	if s.transport == nil {
		return nil
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	var content map[string]any
	if err = json.Unmarshal(raw, &content); err != nil {
		return err
	}
	return s.transport.SendStateEvent(ctx, SendStateEventRequest{RoomID: b.RoomID, SenderMXID: s.OwnerMXID(), Event: RoomStateEvent{Type: dirextalkdomain.GroupAgentStateEventType, StateKey: "", Content: content}})
}

func (s *Service) invalidateGroupAgent(ctx context.Context, roomID string) error {
	store, err := s.groupAgentStore()
	if err != nil {
		return err
	}
	return store.MutateGroupAgentBinding(ctx, roomID, func(b *dirextalkdomain.GroupAgentBinding) error {
		if b.Enabled {
			b.Enabled = false
			b.Status = "disabled"
			b.Revision++
		}
		return nil
	})
}

func (s *Service) createGroupAgentBinding(ctx context.Context, roomID string, enabled bool) (*dirextalkdomain.GroupAgentBinding, *apiError) {
	if !enabled {
		b := defaultGroupAgentBinding(roomID, s.OwnerMXID())
		return &b, nil
	}
	value, apiErr := s.groupAgentUpdate(ctx, map[string]any{"room_id": roomID, "enabled": true})
	if apiErr != nil {
		b := defaultGroupAgentBinding(roomID, s.OwnerMXID())
		if store, err := s.groupAgentStore(); err == nil {
			if saved, found, e := store.GetGroupAgentBinding(ctx, roomID); e == nil && found {
				b = saved
			}
		}
		return &b, apiErr
	}
	b := value.(dirextalkdomain.GroupAgentBinding)
	return &b, nil
}

func isHumanGroupActor(mxid string) bool {
	local, _, err := gomatrixserverlib.SplitID('@', mxid)
	return err == nil && local != "ying" && local != "agent" && local != "system"
}

func (s *Service) projectGroupAgentEvent(ctx context.Context, event *types.HeaderedEvent) error {
	if event == nil {
		return nil
	}
	store, err := s.groupAgentStore()
	if err != nil {
		return err
	}
	b, found, err := store.GetGroupAgentBinding(ctx, event.RoomID().String())
	if err != nil || !found || !b.Enabled {
		return err
	}
	room, err := s.groupAgentRoom(ctx, b.RoomID)
	if err != nil {
		return err
	}
	if !room.IsGroup || room.Dissolved || room.OwnerMXID != b.OwnerMXID || !room.Joined[b.OwnerMXID] || s.transport != nil && !room.Joined[b.AgentMXID] || b.AccountGeneration != s.accountGeneration {
		return s.invalidateGroupAgent(ctx, b.RoomID)
	}
	if s.transport != nil && !groupAgentMatrixBindingMatches(room, b) {
		return s.invalidateGroupAgent(ctx, b.RoomID)
	}
	if event.Type() == spec.MRoomMember && event.StateKey() != nil {
		var c struct {
			Membership string `json:"membership"`
		}
		if json.Unmarshal(event.Content(), &c) == nil && c.Membership != "join" && c.Membership != "invite" {
			user := *event.StateKey()
			if event.StateKeyResolved != nil {
				user = *event.StateKeyResolved
			}
			if _, err := spec.NewUserID(user, true); err != nil {
				return s.invalidateGroupAgent(ctx, b.RoomID)
			}
			return store.CancelGroupAgentRequests(ctx, b.RoomID, user, "")
		}
	}
	if event.Type() != "m.room.message" || event.StateKey() != nil {
		return nil
	}
	reader, ok := s.transport.(dirextalktransport.GroupAgentReadPort)
	if !ok {
		return nil
	}
	message, err := reader.ReadGroupAgentMessage(ctx, b.RoomID, event.EventID())
	if err != nil {
		return err
	}
	if message.Body == "" || message.Generated || !isHumanGroupActor(message.SenderMXID) || !room.Joined[message.SenderMXID] || message.OriginServerTS < b.EnabledAt || len(message.Body) > 16000 {
		return nil
	}
	mentioned := false
	for _, user := range message.Mentions {
		if user == b.AgentMXID {
			mentioned = true
			break
		}
	}
	if !mentioned {
		return nil
	}
	r := dirextalkdomain.GroupAgentRequest{RequestID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("dirextalk:group-agent:"+b.RoomID+":"+message.EventID)).String(), RoomID: b.RoomID, EventID: message.EventID, SenderMXID: message.SenderMXID, OwnerMXID: b.OwnerMXID, AgentMXID: b.AgentMXID, BindingRevision: b.Revision, AccountGeneration: b.AccountGeneration, OriginServerTS: message.OriginServerTS}
	_, err = store.EnqueueGroupAgentRequest(ctx, r)
	return err
}

// InvokeGroupAgentCapability is private service work, not a model tool or
// owner-ticket API. The caller supplies only a persisted request identity;
// room, sender, owner, generation and Ying identity are server-derived.
func (s *Service) InvokeGroupAgentCapability(ctx context.Context, operation string, raw []byte) (any, error) {
	ctx, finish := s.beginAccountOperation(ctx)
	defer finish()
	if s.accountIsDeprovisioned() {
		return nil, dirextalkdomain.ErrGroupAgentConflict
	}
	store, err := s.groupAgentStore()
	if err != nil {
		return nil, err
	}
	var p struct {
		RequestID       string `json:"request_id"`
		BindingRevision int64  `json:"binding_revision"`
		Limit           int    `json:"limit"`
		Body            string `json:"body"`
		Status          string `json:"status"`
		Kind            string `json:"kind"`
		AfterRequestID  string `json:"after_request_id"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&p); err != nil {
		return nil, fmt.Errorf("invalid group Agent request: %w", err)
	}
	if operation == "pull" {
		if p.RequestID != "" || p.BindingRevision != 0 || p.Body != "" || p.Status != "" || p.Kind != "" {
			return nil, dirextalkdomain.ErrGroupAgentConflict
		}
		if p.Limit == 0 {
			p.Limit = 10
		}
		if p.Limit < 1 || p.Limit > 10 {
			return nil, errors.New("limit must be 1 to 10")
		}
		if p.AfterRequestID != "" {
			id, e := uuid.Parse(p.AfterRequestID)
			if e != nil || id.String() != p.AfterRequestID {
				return nil, errors.New("after_request_id must be a canonical UUID")
			}
		}
		requests, err := store.ListGroupAgentRequests(ctx, s.OwnerMXID(), s.accountGeneration, p.Limit+1, p.AfterRequestID)
		if err != nil {
			return nil, err
		}
		out := make([]dirextalkdomain.GroupAgentRequest, 0, len(requests))
		hasMore := len(requests) > p.Limit
		if hasMore {
			requests = requests[:p.Limit]
		}
		next := ""
		if len(requests) > 0 {
			next = requests[len(requests)-1].RequestID
		}
		for _, r := range requests {
			b, found, e := store.GetGroupAgentBinding(ctx, r.RoomID)
			if e != nil {
				return nil, e
			}
			if !found {
				continue
			}
			message, valid, e := s.validateGroupAgentRequest(ctx, b, r, r.BindingRevision)
			if e != nil {
				return nil, e
			}
			if valid {
				r.Body = message.Body
				out = append(out, r)
			}
		}
		return map[string]any{"requests": out, "has_more": hasMore, "next_after_request_id": next}, nil
	}
	id, parseErr := uuid.Parse(p.RequestID)
	if parseErr != nil || id.String() != p.RequestID || p.BindingRevision <= 0 || p.AfterRequestID != "" {
		return nil, errors.New("request_id and binding_revision are required")
	}
	result := map[string]any{"allowed": false, "reason": "revoked"}
	invoke := func(b dirextalkdomain.GroupAgentBinding, r *dirextalkdomain.GroupAgentRequest) error {
		if p.BindingRevision != r.BindingRevision || r.AccountGeneration != s.accountGeneration || r.OwnerMXID != s.OwnerMXID() {
			return dirextalkdomain.ErrGroupAgentConflict
		}
		if operation == "publish" && r.Status == "published" {
			if p.Kind == "progress" {
				result = map[string]any{"status": "cancelled", "replayed": true}
				return nil
			}
			digest := sha256.Sum256([]byte(p.Body))
			if r.ReplyDigest != hex.EncodeToString(digest[:]) {
				return dirextalkdomain.ErrGroupAgentConflict
			}
			result = map[string]any{"status": "published", "event_id": r.ReplyEventID, "replayed": true}
			return nil
		}
		_, valid, e := s.validateGroupAgentRequest(ctx, b, *r, p.BindingRevision)
		if e != nil {
			return e
		}
		if !valid {
			if operation == "history" {
				return dirextalkdomain.ErrGroupAgentConflict
			}
			if r.Status == "pending" {
				r.Status = "cancelled"
			}
			if operation != "validate" {
				result = map[string]any{"status": "cancelled", "replayed": false}
			}
			return nil
		}
		switch operation {
		case "validate":
			if p.Body != "" || p.Status != "" || p.Kind != "" || p.Limit != 0 {
				return errors.New("validate accepts only request identity")
			}
			result = map[string]any{"allowed": true}
		case "history":
			if p.Limit == 0 {
				p.Limit = 20
			}
			if p.Limit < 1 || p.Limit > 50 || p.Body != "" || p.Status != "" || p.Kind != "" {
				return errors.New("invalid history parameters")
			}
			if s.matrixMessages == nil {
				return errors.New("Matrix history reader unavailable")
			}
			room, e := s.groupAgentRoom(ctx, r.RoomID)
			if e != nil {
				return e
			}
			fromTS := b.EnabledAt
			if room.JoinedAt[r.SenderMXID] > fromTS {
				fromTS = room.JoinedAt[r.SenderMXID]
			}
			page, e := s.matrixMessages.ListOrdinaryMessages(ctx, r.RoomID, dirextalkmcp.Page{FromTS: fromTS, SnapshotTS: time.Now().UnixMilli(), Limit: p.Limit})
			if e != nil {
				return e
			}
			messages := make([]dirextalktransport.GroupAgentMessage, 0, len(page.Messages))
			for _, m := range page.Messages {
				sender := m.SenderMXID
				if sender == "" {
					sender = m.Sender
				}
				if _, e := spec.NewUserID(sender, true); e != nil {
					continue
				}
				if m.OriginServerTS >= fromTS && m.EventID != "" && len(m.Msg) <= 16000 {
					messages = append(messages, dirextalktransport.GroupAgentMessage{EventID: m.EventID, SenderMXID: sender, Body: m.Msg, OriginServerTS: m.OriginServerTS})
				}
			}
			if _, stillValid, e := s.validateGroupAgentRequest(ctx, b, *r, p.BindingRevision); e != nil {
				return e
			} else if !stillValid {
				return dirextalkdomain.ErrGroupAgentConflict
			}
			result = map[string]any{"messages": messages}
		case "complete":
			if p.Status != "failed" && p.Status != "cancelled" {
				return errors.New("complete status must be failed or cancelled")
			}
			if p.Body != "" || p.Kind != "" || p.Limit != 0 {
				return errors.New("invalid complete parameters")
			}
			r.Status = p.Status
			result = map[string]any{"status": p.Status}
		case "publish":
			if strings.TrimSpace(p.Body) == "" || len(p.Body) > 16000 || p.Limit != 0 {
				return errors.New("reply body must contain 1 to 16000 bytes")
			}
			if p.Kind == "" {
				p.Kind = "final"
			}
			if p.Kind != "final" && p.Kind != "progress" {
				return errors.New("invalid reply kind")
			}
			if p.Kind == "progress" && p.Status != "working" && p.Status != "waiting_owner" {
				return errors.New("progress requires working or waiting_owner status")
			}
			if p.Kind == "final" {
				if p.Status == "" {
					p.Status = "completed"
				}
				if p.Status != "completed" && p.Status != "failed" {
					return errors.New("final requires completed or failed status")
				}
			}
			port, ok := s.transport.(dirextalktransport.PreparedMessagePort)
			if !ok || s.preparedMatrixStore == nil {
				return errors.New("durable group Agent Matrix publisher unavailable")
			}
			opID := r.RequestID
			if p.Kind == "progress" {
				opID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(r.RequestID+":"+p.Status)).String()
			}
			digest := sha256.Sum256([]byte(p.Body))
			rootDigest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%s:%s", r.RequestID, r.BindingRevision, p.Kind, p.Status, p.Body)))
			if e := store.AdmitGroupAgentPublication(ctx, opID, *r, rootDigest[:]); e != nil {
				return e
			}
			preparedBefore, _ := s.preparedMatrixStore.GetMatrixPreparedEvent(ctx, opID, r.OwnerMXID, r.AccountGeneration, rootDigest[:])
			msgType := "group_agent_reply"
			if p.Kind == "progress" {
				msgType = "group_agent_status"
			}
			content := map[string]any{"msgtype": "m.text", "body": p.Body, "msg_type": msgType, "io.dirextalk.group_agent": map[string]any{"request_id": r.RequestID, "binding_revision": r.BindingRevision, "owner_mxid": r.OwnerMXID, "agent_mxid": r.AgentMXID, "kind": p.Kind, "status": p.Status}, "m.relates_to": map[string]any{"m.in_reply_to": map[string]any{"event_id": r.EventID}}}
			receipt, e := dirextalktransport.ExecutePreparedMatrixMutation(ctx, port, s.preparedMatrixStore, dirextalktransport.CapabilityOperationContext{OperationID: opID, OwnerID: r.OwnerMXID, Generation: r.AccountGeneration, RootDigest: rootDigest[:]}, "product.group_agent.v1", "publish", SendMessageRequest{SenderMXID: r.AgentMXID, RoomID: r.RoomID, EventType: "m.room.message", MessageType: "m.text", Content: content, LogicalID: r.RequestID})
			if e != nil {
				return e
			}
			if p.Kind == "final" {
				r.Status = "published"
				r.ReplyEventID = receipt.EventID
				r.ReplyDigest = hex.EncodeToString(digest[:])
			}
			result = map[string]any{"status": "published", "event_id": receipt.EventID, "replayed": preparedBefore != nil}
		default:
			return errors.New("unknown private group Agent operation")
		}
		return nil
	}
	if operation == "validate" || operation == "history" {
		r, found, e := store.GetGroupAgentRequest(ctx, p.RequestID)
		if e != nil {
			return nil, e
		}
		if !found {
			return result, nil
		}
		b, found, e := store.GetGroupAgentBinding(ctx, r.RoomID)
		if e != nil {
			return nil, e
		}
		if !found {
			return result, nil
		}
		err = invoke(b, &r)
	} else {
		err = store.MutateGroupAgentRequest(ctx, p.RequestID, invoke)
	}
	return result, err
}

func (s *Service) validateGroupAgentRequest(ctx context.Context, b dirextalkdomain.GroupAgentBinding, r dirextalkdomain.GroupAgentRequest, revision int64) (dirextalktransport.GroupAgentMessage, bool, error) {
	message := dirextalktransport.GroupAgentMessage{}
	if !b.Enabled || b.Revision != revision || r.BindingRevision != revision || b.OwnerMXID != s.OwnerMXID() || r.OwnerMXID != b.OwnerMXID || b.AccountGeneration != s.accountGeneration || r.AccountGeneration != s.accountGeneration || r.AgentMXID != b.AgentMXID || r.Status != "pending" {
		return message, false, nil
	}
	room, err := s.groupAgentRoom(ctx, r.RoomID)
	if err != nil {
		return message, false, err
	}
	if !room.IsGroup || room.Dissolved || room.OwnerMXID != b.OwnerMXID || !room.Joined[b.OwnerMXID] || !room.Joined[b.AgentMXID] || !room.Joined[r.SenderMXID] || !isHumanGroupActor(r.SenderMXID) {
		return message, false, nil
	}
	if !groupAgentMatrixBindingMatches(room, b) {
		return message, false, nil
	}
	reader, ok := s.transport.(dirextalktransport.GroupAgentReadPort)
	if !ok {
		return message, false, errors.New("group Agent source reader unavailable")
	}
	message, err = reader.ReadGroupAgentMessage(ctx, r.RoomID, r.EventID)
	if err != nil {
		return message, false, err
	}
	if message.Body == "" || message.EventID != r.EventID || message.RoomID != r.RoomID || message.SenderMXID != r.SenderMXID || message.Generated {
		return message, false, nil
	}
	return message, true, nil
}

func groupAgentMatrixBindingMatches(room dirextalktransport.GroupAgentRoom, b dirextalkdomain.GroupAgentBinding) bool {
	return room.Binding != nil && room.Binding.RoomID == b.RoomID && room.Binding.Enabled && room.Binding.Revision == b.Revision && room.Binding.OwnerMXID == b.OwnerMXID && room.Binding.AgentMXID == b.AgentMXID && room.Binding.MemberPolicy == "all_joined"
}
