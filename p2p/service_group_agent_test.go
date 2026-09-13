package p2p

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalktransport"
	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	p2pstorage "github.com/YingSuiAI/dirextalk-message-server/p2p/storage"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
	"github.com/google/uuid"
)

type groupAgentMatrixFixture struct {
	*recordingTransport
	mu          sync.Mutex
	room        dirextalktransport.GroupAgentRoom
	sources     map[string]dirextalktransport.GroupAgentMessage
	sent        map[string]dirextalktransport.PreparedMessage
	prepared    []dirextalktransport.SendMessageRequest
	sourceErr   error
	loseReceipt bool
}

func (m *groupAgentMatrixFixture) ReadGroupAgentRoom(context.Context, string) (dirextalktransport.GroupAgentRoom, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.room
	r.Joined = map[string]bool{}
	for id, v := range m.room.Joined {
		r.Joined[id] = v
	}
	return r, nil
}
func (m *groupAgentMatrixFixture) ReadGroupAgentMessage(_ context.Context, room, event string) (dirextalktransport.GroupAgentMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sourceErr != nil {
		return dirextalktransport.GroupAgentMessage{}, m.sourceErr
	}
	r, ok := m.sources[event]
	if !ok || r.RoomID != room {
		return r, errors.New("source absent")
	}
	return r, nil
}
func (m *groupAgentMatrixFixture) JoinRoom(_ context.Context, r JoinRoomRequest) (JoinRoomResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.room.Joined[r.UserMXID] = true
	return JoinRoomResult{RoomID: r.RoomIDOrAlias}, nil
}
func (m *groupAgentMatrixFixture) LeaveRoom(_ context.Context, r LeaveRoomRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.room.Joined[r.UserMXID] = false
	return nil
}

func (m *groupAgentMatrixFixture) SendStateEvent(_ context.Context, r SendStateEventRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateEvents = append(m.stateEvents, r)
	if r.Event.Type == dirextalkdomain.GroupAgentStateEventType {
		raw, _ := json.Marshal(r.Event.Content)
		var b dirextalkdomain.GroupAgentBinding
		if err := json.Unmarshal(raw, &b); err != nil {
			return err
		}
		m.room.Binding = &b
	}
	return nil
}
func (m *groupAgentMatrixFixture) PrepareMessage(_ context.Context, r dirextalktransport.SendMessageRequest) (dirextalktransport.PreparedMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prepared = append(m.prepared, r)
	raw, _ := json.Marshal(r.Content)
	digest := sha256.Sum256(raw)
	return dirextalktransport.PreparedMessage{EventID: "$ying-" + uuid.NewString(), RoomID: r.RoomID, SenderMXID: r.SenderMXID, EventType: "m.room.message", MessageType: "m.text", OriginServerTS: time.Now().UnixMilli(), RoomVersion: "11", EventJSON: raw, ContentDigest: digest[:]}, nil
}
func (m *groupAgentMatrixFixture) SendPreparedMessage(_ context.Context, p dirextalktransport.PreparedMessage) (dirextalktransport.SendMessageResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent[p.EventID] = p
	if m.loseReceipt {
		m.loseReceipt = false
		return dirextalktransport.SendMessageResult{}, errors.New("lost Matrix receipt")
	}
	return dirextalktransport.SendMessageResult{EventID: p.EventID, OriginServerTS: p.OriginServerTS}, nil
}
func (m *groupAgentMatrixFixture) EventExists(_ context.Context, _, event string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.sent[event]
	return exists, nil
}

type groupAgentPreparedFixture struct {
	rows map[string]dirextalktransport.PreparedMatrixEvent
}

type groupAgentIdentityFixture struct{ recordingMatrixSessionIssuer }

func (*groupAgentIdentityFixture) EnsureNativeGroupAgentIdentity(context.Context, string) error {
	return nil
}

func (m *groupAgentPreparedFixture) PrepareMatrixEvent(_ context.Context, r dirextalktransport.PreparedMatrixEvent) error {
	m.rows[r.OperationID] = r
	return nil
}
func (m *groupAgentPreparedFixture) GetMatrixPreparedEvent(_ context.Context, id, owner string, generation int64, digest []byte) (*dirextalktransport.PreparedMatrixEvent, error) {
	r, ok := m.rows[id]
	if !ok {
		return nil, sql.ErrNoRows
	}
	if r.OwnerID != owner || r.Generation != generation || string(r.RootDigest) != string(digest) {
		return nil, dirextalkdomain.ErrGroupAgentConflict
	}
	return &r, nil
}
func (m *groupAgentPreparedFixture) DeleteMatrixPreparedEvent(context.Context, string, string, int64, []byte) error {
	return nil
}
func (m *groupAgentPreparedFixture) DeleteMatrixPreparedEventByOperation(context.Context, string) error {
	return nil
}

func groupAgentFixture(t *testing.T) (*Service, *groupAgentMatrixFixture, string) {
	t.Helper()
	s := NewService(Config{ServerName: "example.test", AccountGeneration: 7})
	roomID := "!group:example.test"
	owner := s.OwnerMXID()
	m := &groupAgentMatrixFixture{recordingTransport: &recordingTransport{roomID: roomID}, room: dirextalktransport.GroupAgentRoom{RoomID: roomID, IsGroup: true, OwnerMXID: owner, Joined: map[string]bool{owner: true, "@member:remote.test": true}}, sources: map[string]dirextalktransport.GroupAgentMessage{}, sent: map[string]dirextalktransport.PreparedMessage{}}
	s.transport = m
	s.sessions = &groupAgentIdentityFixture{}
	s.preparedMatrixStore = &groupAgentPreparedFixture{rows: map[string]dirextalktransport.PreparedMatrixEvent{}}
	if err := s.saveGroup(context.Background(), groupRecord{RoomID: roomID, Name: "Group"}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveOwnerMember(context.Background(), roomID, ""); err != nil {
		t.Fatal(err)
	}
	return s, m, roomID
}
func groupAgentCall(t *testing.T, s *Service, operation string, p map[string]any) map[string]any {
	t.Helper()
	raw, _ := json.Marshal(p)
	r, err := s.InvokeGroupAgentCapability(context.Background(), operation, raw)
	if err != nil {
		t.Fatalf("%s failed: %v", operation, err)
	}
	return r.(map[string]any)
}
func groupAgentEnqueue(t *testing.T, s *Service, m *groupAgentMatrixFixture, roomID string) dirextalkdomain.GroupAgentRequest {
	t.Helper()
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": roomID, "enabled": true})
	r := dirextalkdomain.GroupAgentRequest{RequestID: uuid.NewString(), RoomID: roomID, EventID: "$source-" + uuid.NewString(), SenderMXID: "@member:remote.test", OwnerMXID: s.OwnerMXID(), AgentMXID: b.AgentMXID, BindingRevision: b.Revision, AccountGeneration: 7, OriginServerTS: time.Now().UnixMilli()}
	m.sources[r.EventID] = dirextalktransport.GroupAgentMessage{RoomID: roomID, EventID: r.EventID, SenderMXID: r.SenderMXID, Body: "@Ying help", OriginServerTS: r.OriginServerTS, Mentions: []string{b.AgentMXID}}
	inserted, err := s.store.EnqueueGroupAgentRequest(context.Background(), r)
	if err != nil || !inserted {
		t.Fatalf("enqueue fixture: %t %v", inserted, err)
	}
	return r
}

func TestGroupYingRoomDisplayNameIsSharedNotOwnerScoped(t *testing.T) {
	for _, owner := range []string{"Ott", "  ", "李娜"} {
		if got := groupYingRoomDisplayName(owner); got != "Ying" {
			t.Fatalf("group Ying label for %q = %q", owner, got)
		}
	}
}

// The shared group Agent is the owner's Ying to every client, including builds
// that can only render room membership. A label refresh must never be able to
// block the owner's switch.
func TestGroupAgentRoomMemberCarriesTheOwnerLabel(t *testing.T) {
	s, m, room := groupAgentFixture(t)
	m.room.OwnerDisplayName = "Ott"
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": room, "enabled": true, "expected_revision": 0})
	if !b.Enabled || b.Revision != 1 {
		t.Fatalf("enable=%#v", b)
	}
	if len(m.profileRequests) == 0 {
		t.Fatal("group Agent member label was never published")
	}
	label := m.profileRequests[len(m.profileRequests)-1]
	if label.UserMXID != b.AgentMXID || label.RoomID != room || label.DisplayName != "Ying" {
		t.Fatalf("group Agent label = %#v", label)
	}
	b = mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": room, "enabled": false, "expected_revision": b.Revision})
	if b.Enabled || b.Revision != 2 {
		t.Fatalf("disable=%#v", b)
	}
	m.profileErrors = map[string]error{room: errors.New("profile endpoint unavailable")}
	b = mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": room, "enabled": true, "expected_revision": b.Revision})
	if !b.Enabled || b.Revision != 3 {
		t.Fatalf("enable with stale label=%#v", b)
	}
}

func TestGroupAgentOwnerOnlyDefaultOffAndRevisionCAS(t *testing.T) {
	s, m, room := groupAgentFixture(t)
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.get", map[string]any{"room_id": room})
	if b.Enabled || b.Revision != 0 || b.AgentMXID != "@ying:example.test" {
		t.Fatalf("default=%#v", b)
	}
	b = mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": room, "enabled": true, "expected_revision": 0})
	if !b.Enabled || b.Revision != 1 || !m.room.Joined[b.AgentMXID] {
		t.Fatalf("enable=%#v", b)
	}
	if _, err := s.Handle(context.Background(), "groups.agent.update", map[string]any{"room_id": room, "enabled": false, "expected_revision": 0}); err == nil || err.Status != 409 {
		t.Fatalf("stale CAS=%#v", err)
	}
	m.room.OwnerMXID = "@member:remote.test"
	if _, err := s.Handle(context.Background(), "groups.agent.update", map[string]any{"room_id": room, "enabled": true}); err == nil || err.Status != 403 {
		t.Fatalf("former owner updated=%#v", err)
	}
	b = mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.get", map[string]any{"room_id": room})
	if b.Enabled || b.Revision != 2 {
		t.Fatalf("transfer did not revoke=%#v", b)
	}
}

// The Product HTTP/WS envelope decodes numbers with json.Number, so a fixture
// that passes a Go int never exercises the real parameter path. Drive the
// owner toggle through the actual HTTP handler.
func TestGroupAgentUpdateOverHTTPAcceptsDecodedRevision(t *testing.T) {
	s, m, room := groupAgentFixture(t)
	router := newP2PTestRouter(s)
	post := func(params map[string]any) (int, map[string]any) {
		t.Helper()
		req := jsonRequest(t, "/_p2p/command", map[string]any{"action": "groups.agent.update", "params": params})
		req.Header.Set("Authorization", "Bearer "+s.AccessToken())
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		var payload map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &payload)
		return rec.Code, payload
	}

	code, payload := post(map[string]any{"room_id": room, "enabled": true, "expected_revision": 0})
	if code != http.StatusOK || payload["enabled"] != true || payload["revision"].(float64) != 1 {
		t.Fatalf("enable over HTTP -> %d %#v", code, payload)
	}
	if !m.room.Joined["@ying:example.test"] {
		t.Fatal("HTTP enable did not join Ying to the room")
	}
	if code, payload := post(map[string]any{"room_id": room, "enabled": false, "expected_revision": 0}); code == http.StatusOK {
		t.Fatalf("stale revision over HTTP was accepted: %#v", payload)
	}
	for _, revision := range []any{json.Number("-1"), json.Number("2.5"), "seven"} {
		if code, payload := post(map[string]any{"room_id": room, "enabled": true, "expected_revision": revision}); code == http.StatusOK {
			t.Fatalf("expected_revision %#v over HTTP was accepted: %#v", revision, payload)
		}
	}

	readback := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.get", map[string]any{"room_id": room})
	if !readback.Enabled || readback.Revision != 1 {
		t.Fatalf("rejected updates changed the binding: %#v", readback)
	}
}

func TestGroupAgentPullPublishReplayAndLateReplyFence(t *testing.T) {
	s, m, room := groupAgentFixture(t)
	r := groupAgentEnqueue(t, s, m, room)
	p := map[string]any{"request_id": r.RequestID, "binding_revision": r.BindingRevision}
	for i := 0; i < 2; i++ {
		pulled := groupAgentCall(t, s, "pull", map[string]any{})
		requests := pulled["requests"].([]dirextalkdomain.GroupAgentRequest)
		if len(requests) != 1 || requests[0].SenderMXID != r.SenderMXID || requests[0].Body != "@Ying help" {
			t.Fatalf("pull=%#v", pulled)
		}
	}
	if !groupAgentCall(t, s, "validate", p)["allowed"].(bool) {
		t.Fatal("valid request denied")
	}
	p["body"] = "Public group answer"
	m.loseReceipt = true
	raw, _ := json.Marshal(p)
	if _, err := s.InvokeGroupAgentCapability(context.Background(), "publish", raw); err == nil {
		t.Fatal("expected uncertain first Matrix receipt")
	}
	result := groupAgentCall(t, s, "publish", p)
	if result["status"] != "published" || len(m.sent) != 1 || len(m.prepared) != 1 {
		t.Fatalf("retry duplicated reply: %#v %#v", result, m.prepared)
	}
	if !groupAgentCall(t, s, "publish", p)["replayed"].(bool) {
		t.Fatal("duplicate final not replayed")
	}
	req := m.prepared[0]
	if req.SenderMXID != "@ying:example.test" || req.Content["msg_type"] != "group_agent_reply" {
		t.Fatalf("wrong visible identity: %#v", req)
	}
	relation := req.Content["m.relates_to"].(map[string]any)["m.in_reply_to"].(map[string]any)
	if relation["event_id"] != r.EventID {
		t.Fatalf("wrong reply relation: %#v", relation)
	}
	r2 := groupAgentEnqueue(t, s, m, room)
	mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": room, "enabled": false})
	late := groupAgentCall(t, s, "publish", map[string]any{"request_id": r2.RequestID, "binding_revision": r2.BindingRevision, "body": "late"})
	if late["status"] != "cancelled" || len(m.sent) != 1 {
		t.Fatalf("late reply published: %#v", late)
	}
	stored, _, _ := s.store.GetGroupAgentRequest(context.Background(), r2.RequestID)
	if stored.Status != "cancelled" {
		t.Fatalf("disable failed to cancel queue: %#v", stored)
	}
}

func TestGroupAgentLiveMembershipAndSourceFences(t *testing.T) {
	for _, name := range []string{"actor left", "owner left", "Ying removed", "transferred", "dissolved", "source sender changed", "generated", "generation"} {
		t.Run(name, func(t *testing.T) {
			s, m, room := groupAgentFixture(t)
			r := groupAgentEnqueue(t, s, m, room)
			switch name {
			case "actor left":
				m.room.Joined[r.SenderMXID] = false
			case "owner left":
				m.room.Joined[r.OwnerMXID] = false
			case "Ying removed":
				m.room.Joined[r.AgentMXID] = false
			case "transferred":
				m.room.OwnerMXID = "@new:remote.test"
			case "dissolved":
				m.room.Dissolved = true
			case "source sender changed":
				msg := m.sources[r.EventID]
				msg.SenderMXID = "@other:remote.test"
				m.sources[r.EventID] = msg
			case "generated":
				msg := m.sources[r.EventID]
				msg.Generated = true
				m.sources[r.EventID] = msg
			case "generation":
				s.accountGeneration++
			}
			if name == "generation" {
				raw, _ := json.Marshal(map[string]any{"request_id": r.RequestID, "binding_revision": r.BindingRevision})
				if _, err := s.InvokeGroupAgentCapability(context.Background(), "validate", raw); err == nil {
					t.Fatal("stale generation accepted")
				}
				return
			}
			result := groupAgentCall(t, s, "validate", map[string]any{"request_id": r.RequestID, "binding_revision": r.BindingRevision})
			if result["allowed"] == true {
				t.Fatalf("fence allowed %s", name)
			}
		})
	}
}

func TestGroupAgentOutputDedupesMentionsAndRetriesMatrixRead(t *testing.T) {
	s, m, _ := groupAgentFixture(t)
	owner := test.NewUser(t)
	room := test.NewRoom(t, owner)
	m.room.RoomID = room.ID
	roomID := room.ID
	if err := s.saveGroup(context.Background(), groupRecord{RoomID: roomID}); err != nil {
		t.Fatal(err)
	}
	if err := s.saveOwnerMember(context.Background(), roomID, ""); err != nil {
		t.Fatal(err)
	}
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": roomID, "enabled": true})
	event := room.CreateAndInsert(t, owner, "m.room.message", map[string]any{"msgtype": "m.text", "body": "@Ying"})
	m.sources[event.EventID()] = dirextalktransport.GroupAgentMessage{RoomID: roomID, EventID: event.EventID(), SenderMXID: "@member:remote.test", Body: "@Ying", OriginServerTS: time.Now().UnixMilli(), Mentions: []string{b.AgentMXID}}
	m.sourceErr = errors.New("temporary Matrix read unavailable")
	if err := s.projectGroupAgentEvent(context.Background(), event); err == nil {
		t.Fatal("source read failure was acknowledged/lost")
	}
	m.sourceErr = nil
	for i := 0; i < 2; i++ {
		if err := s.projectGroupAgentEvent(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.store.ListGroupAgentRequests(context.Background(), s.OwnerMXID(), 7, 10, "")
	if err != nil || len(rows) != 1 || rows[0].Body != "" {
		t.Fatalf("event dedupe=%#v %v", rows, err)
	}
}

func TestGroupAgentPostgresPreparedReplySurvivesRestart(t *testing.T) {
	ctx := context.Background()
	conn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	opts := config.DatabaseOptions{ConnectionString: config.DataSource(conn)}
	store, err := p2pstorage.NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, opts), &opts)
	if err != nil {
		t.Fatal(err)
	}
	s, m, room := groupAgentFixture(t)
	s.store = store
	s.preparedMatrixStore = dirextalktransport.NewPostgresPreparedMatrixMutationStore(store.DB())
	r := groupAgentEnqueue(t, s, m, room)
	p := map[string]any{"request_id": r.RequestID, "binding_revision": r.BindingRevision, "body": "exact durable reply"}
	raw, _ := json.Marshal(p)
	m.loseReceipt = true
	if _, err = s.InvokeGroupAgentCapability(ctx, "publish", raw); err == nil {
		t.Fatal("expected missing Matrix receipt")
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = p2pstorage.NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, opts), &opts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s.store = store
	s.preparedMatrixStore = dirextalktransport.NewPostgresPreparedMatrixMutationStore(store.DB())
	result := groupAgentCall(t, s, "publish", p)
	if result["status"] != "published" || len(m.sent) != 1 || len(m.prepared) != 1 {
		t.Fatalf("restart reply was rebuilt: %#v prepared=%d sent=%d", result, len(m.prepared), len(m.sent))
	}
	stored, _, err := store.GetGroupAgentRequest(ctx, r.RequestID)
	if err != nil || stored.Status != "published" || stored.ReplyEventID == "" {
		t.Fatalf("receipt not durable: %#v %v", stored, err)
	}
	var ledgerState string
	if err = store.DB().QueryRowContext(ctx, `SELECT state FROM p2p_capability_operations WHERE operation_id=$1`, r.RequestID).Scan(&ledgerState); err != nil || ledgerState != "completed" {
		t.Fatalf("hidden ledger=%s %v", ledgerState, err)
	}
}

// The owner reads everything since the Agent was enabled; any other member
// keeps the Matrix visibility floor of their own join.
func TestGroupAgentHistoryHonoursOwnerAndMemberVisibility(t *testing.T) {
	s, m, roomID := groupAgentFixture(t)
	reader := &fakeMCPMessageReader{}
	s.SetMatrixMessageReader(reader)
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": roomID, "enabled": true, "expected_revision": 0})
	owner := s.OwnerMXID()
	member := "@member:remote.test"
	m.room.JoinedAt = map[string]int64{owner: b.EnabledAt, member: b.EnabledAt + 5_000}

	enqueue := func(sender string) dirextalkdomain.GroupAgentRequest {
		t.Helper()
		r := dirextalkdomain.GroupAgentRequest{RequestID: uuid.NewString(), RoomID: roomID, EventID: "$source-" + uuid.NewString(),
			SenderMXID: sender, OwnerMXID: owner, AgentMXID: b.AgentMXID, BindingRevision: b.Revision,
			AccountGeneration: 7, OriginServerTS: time.Now().UnixMilli()}
		m.sources[r.EventID] = dirextalktransport.GroupAgentMessage{RoomID: roomID, EventID: r.EventID, SenderMXID: sender,
			Body: "@Ying what did we decide?", OriginServerTS: r.OriginServerTS, Mentions: []string{b.AgentMXID}}
		inserted, err := s.store.EnqueueGroupAgentRequest(context.Background(), r)
		if err != nil || !inserted {
			t.Fatalf("enqueue: %t %v", inserted, err)
		}
		return r
	}
	history := func(r dirextalkdomain.GroupAgentRequest) map[string]any {
		t.Helper()
		reader.calls = 0
		result := groupAgentCall(t, s, "history", map[string]any{"request_id": r.RequestID, "binding_revision": b.Revision, "limit": 5})
		if reader.calls != 1 {
			t.Fatalf("history reads = %d", reader.calls)
		}
		return result
	}

	ownerRead := history(enqueue(owner))
	if reader.lastPage.FromTS != b.EnabledAt {
		t.Fatalf("owner floor = %d, want %d", reader.lastPage.FromTS, b.EnabledAt)
	}
	if ownerRead["has_more"] != false {
		t.Fatalf("empty owner read has_more=%v", ownerRead["has_more"])
	}
	memberRead := history(enqueue(member))
	if reader.lastPage.FromTS != b.EnabledAt+5_000 {
		t.Fatalf("member floor = %d, want %d", reader.lastPage.FromTS, b.EnabledAt+5_000)
	}
	if _, ok := memberRead["messages"]; !ok {
		t.Fatalf("member read lost its messages: %#v", memberRead)
	}
}

// The Agent's own summary sweep lists only its enabled groups and reads them
// without a member ticket, and only for the current binding revision.
func TestGroupAgentBindingsAndTranscriptAreOwnerScoped(t *testing.T) {
	s, m, roomID := groupAgentFixture(t)
	reader := &fakeMCPMessageReader{messages: []mcpMessageSummary{{EventID: "$m", Sender: "@owner:example.test", SenderMXID: "@owner:example.test", Msg: "hello group", OriginServerTS: time.Now().UnixMilli()}}}
	s.SetMatrixMessageReader(reader)
	b := mustHandle[dirextalkdomain.GroupAgentBinding](t, s, "groups.agent.update", map[string]any{"room_id": roomID, "enabled": true, "expected_revision": 0})

	listed := groupAgentCall(t, s, "bindings", map[string]any{})
	bindings, ok := listed["bindings"].([]dirextalkdomain.GroupAgentBinding)
	if !ok || len(bindings) != 1 || bindings[0].RoomID != roomID || !bindings[0].Enabled {
		t.Fatalf("bindings = %#v", listed["bindings"])
	}
	if listed["owner_mxid"] != s.OwnerMXID() {
		t.Fatalf("bindings owner = %#v", listed["owner_mxid"])
	}

	transcript := groupAgentCall(t, s, "transcript", map[string]any{"room_id": roomID, "binding_revision": b.Revision, "limit": 10})
	messages, ok := transcript["messages"].([]dirextalktransport.GroupAgentMessage)
	if !ok || len(messages) != 1 || messages[0].Body != "hello group" {
		t.Fatalf("transcript = %#v", transcript["messages"])
	}

	for _, params := range []map[string]any{
		{"room_id": roomID, "binding_revision": b.Revision + 1, "limit": 10},
		{"room_id": "!other:example.test", "binding_revision": b.Revision, "limit": 10},
	} {
		raw, _ := json.Marshal(params)
		if _, err := s.InvokeGroupAgentCapability(context.Background(), "transcript", raw); !errors.Is(err, dirextalkdomain.ErrGroupAgentConflict) {
			t.Fatalf("transcript %#v err = %v", params, err)
		}
	}
	m.room.Joined[b.AgentMXID] = false
	raw, _ := json.Marshal(map[string]any{"room_id": roomID, "binding_revision": b.Revision, "limit": 10})
	if _, err := s.InvokeGroupAgentCapability(context.Background(), "transcript", raw); !errors.Is(err, dirextalkdomain.ErrGroupAgentConflict) {
		t.Fatalf("disabled Agent transcript err = %v", err)
	}
}
