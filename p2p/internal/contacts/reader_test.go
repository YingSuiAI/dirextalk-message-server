package contacts

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sort"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
)

type testStore struct {
	records   []dirextalkdomain.ContactRecord
	listErr   error
	listCalls int
}

func (s *testStore) UpsertContact(context.Context, dirextalkdomain.ContactRecord) error {
	return nil
}

func (s *testStore) ListContacts(context.Context) ([]dirextalkdomain.ContactRecord, error) {
	s.listCalls++
	if s.listErr != nil {
		return nil, s.listErr
	}
	return append([]dirextalkdomain.ContactRecord(nil), s.records...), nil
}

func TestListRawAndVisiblePreserveContactSelection(t *testing.T) {
	records := []dirextalkdomain.ContactRecord{
		{PeerMXID: "@deleted:example.com", RoomID: "!deleted:example.com", DisplayName: "Deleted", Status: "deleted"},
		{PeerMXID: "@alice:example.com", RoomID: "!alice-rejected:example.com", DisplayName: "Rejected", Status: "rejected"},
		{PeerMXID: "@alice:example.com", RoomID: "!alice-outbound:example.com", DisplayName: "Outbound", Status: "pending_outbound"},
		{PeerMXID: "@alice:example.com", RoomID: "!alice-inbound:example.com", DisplayName: "Inbound", Status: "pending_inbound"},
		{PeerMXID: "@alice:example.com", RoomID: "!alice:example.com", DisplayName: "Zulu", Status: "accepted"},
		{PeerMXID: "@bob:example.com", RoomID: "!bob:example.com", DisplayName: "beta", Status: "accepted"},
		{PeerMXID: "@bob:example.com", RoomID: "!ignored:example.com", AvatarURL: "mxc://example.com/bob", Domain: "example.com", Remark: "known here", Status: "accepted"},
		{RoomID: " !room-only:example.com ", DisplayName: "old room", Status: "unknown"},
		{RoomID: "!room-only:example.com", DisplayName: "Alpha", Status: "accepted"},
		{PeerMXID: "@z:example.com", RoomID: "!z:example.com", DisplayName: "same", Status: "accepted"},
		{PeerMXID: "@a:example.com", RoomID: "!a:example.com", DisplayName: "same", Status: "accepted"},
		{DisplayName: "no identity", Status: "accepted"},
	}
	store := &testStore{records: records}
	module := New(store, nil, Config{})

	raw, err := module.ListRaw(context.Background())
	if err != nil {
		t.Fatalf("ListRaw() error = %v", err)
	}
	if !reflect.DeepEqual(raw, records) {
		t.Fatalf("ListRaw() = %#v, want %#v", raw, records)
	}

	visible, err := module.ListVisible(context.Background())
	if err != nil {
		t.Fatalf("ListVisible() error = %v", err)
	}
	want := []dirextalkdomain.ContactRecord{
		{RoomID: "!room-only:example.com", DisplayName: "Alpha", Status: "accepted"},
		{
			PeerMXID: "@bob:example.com", RoomID: "!bob:example.com", DisplayName: "beta",
			AvatarURL: "mxc://example.com/bob", Domain: "example.com", Remark: "known here", Status: "accepted",
		},
		{PeerMXID: "@a:example.com", RoomID: "!a:example.com", DisplayName: "same", Status: "accepted"},
		{PeerMXID: "@z:example.com", RoomID: "!z:example.com", DisplayName: "same", Status: "accepted"},
		{PeerMXID: "@alice:example.com", RoomID: "!alice:example.com", DisplayName: "Zulu", Status: "accepted"},
	}
	if !reflect.DeepEqual(visible, want) {
		t.Fatalf("ListVisible() = %#v, want %#v", visible, want)
	}
	if store.listCalls != 2 {
		t.Fatalf("ListContacts calls = %d, want 2", store.listCalls)
	}
}

func TestListVisibleKeepsSingleContactWithoutIdentity(t *testing.T) {
	want := []dirextalkdomain.ContactRecord{{DisplayName: "legacy contact", Status: "accepted"}}
	module := New(&testStore{records: want}, nil, Config{})

	got, err := module.ListVisible(context.Background())
	if err != nil {
		t.Fatalf("ListVisible() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListVisible() = %#v, want %#v", got, want)
	}
}

func TestContactLookupsPreserveRawSelection(t *testing.T) {
	store := &testStore{records: []dirextalkdomain.ContactRecord{
		{PeerMXID: "@alice:example.com", RoomID: "!first:example.com", DisplayName: "First", Status: "pending_inbound"},
		{PeerMXID: "@alice:example.com", RoomID: "!accepted:example.com", DisplayName: "Accepted first", Status: "accepted"},
		{PeerMXID: "@alice:example.com", RoomID: "!accepted-later:example.com", DisplayName: "Accepted later", Status: "accepted"},
		{PeerMXID: "@deleted:example.com", RoomID: "!deleted:example.com", Status: "deleted"},
	}}
	module := New(store, nil, Config{})

	byRoom, ok, err := module.LookupByRoom(context.Background(), "  !deleted:example.com  ")
	if err != nil || !ok || byRoom.PeerMXID != "@deleted:example.com" {
		t.Fatalf("LookupByRoom() = (%#v, %t, %v)", byRoom, ok, err)
	}
	byPeer, ok, err := module.LookupByPeer(context.Background(), "  @alice:example.com  ")
	if err != nil || !ok || byPeer.RoomID != "!accepted:example.com" {
		t.Fatalf("LookupByPeer() = (%#v, %t, %v)", byPeer, ok, err)
	}

	before := store.listCalls
	if got, found, lookupErr := module.LookupByRoom(context.Background(), " "); lookupErr != nil || found || got != (dirextalkdomain.ContactRecord{}) {
		t.Fatalf("LookupByRoom(empty) = (%#v, %t, %v)", got, found, lookupErr)
	}
	if got, found, lookupErr := module.LookupByPeer(context.Background(), " "); lookupErr != nil || found || got != (dirextalkdomain.ContactRecord{}) {
		t.Fatalf("LookupByPeer(empty) = (%#v, %t, %v)", got, found, lookupErr)
	}
	if store.listCalls != before {
		t.Fatalf("empty lookups read Store: calls = %d, want %d", store.listCalls, before)
	}
}

type legacyContactContract struct {
	PeerMXID            string                            `json:"peer_mxid"`
	DisplayName         string                            `json:"display_name"`
	DisplayNameOverride bool                              `json:"display_name_override,omitempty"`
	AvatarURL           string                            `json:"avatar_url"`
	Domain              string                            `json:"domain"`
	RoomID              string                            `json:"room_id"`
	Status              string                            `json:"status"`
	Remark              string                            `json:"remark,omitempty"`
	Operation           map[string]any                    `json:"operation,omitempty"`
	Conversation        *dirextalkdomain.ConversationView `json:"conversation,omitempty"`
	OperationID         string                            `json:"operation_id,omitempty"`
	CurrentRoomID       string                            `json:"current_room_id,omitempty"`
	ErrorCode           string                            `json:"error_code,omitempty"`
	RequestID           string                            `json:"-"`
}

func TestViewMatchesLegacyContactJSONContractAndRecordRoundTrip(t *testing.T) {
	gotType := reflect.TypeOf(View{})
	wantType := reflect.TypeOf(legacyContactContract{})
	if gotType.NumField() != wantType.NumField() {
		t.Fatalf("View fields = %d, want %d", gotType.NumField(), wantType.NumField())
	}
	for index := 0; index < wantType.NumField(); index++ {
		got := gotType.Field(index)
		want := wantType.Field(index)
		if got.Name != want.Name || got.Type != want.Type || got.Tag != want.Tag || got.Anonymous != want.Anonymous {
			t.Fatalf("View field %d = %#v, want %#v", index, got, want)
		}
	}

	record := dirextalkdomain.ContactRecord{
		PeerMXID:            "@alice:example.com",
		DisplayName:         "Alice",
		DisplayNameOverride: true,
		AvatarURL:           "mxc://example.com/alice",
		Domain:              "example.com",
		RoomID:              "!direct:example.com",
		Status:              "accepted",
		Remark:              "known contact",
	}
	view := ViewFromRecord(record)
	if got := RecordFromView(view); got != record {
		t.Fatalf("RecordFromView(ViewFromRecord()) = %#v, want %#v", got, record)
	}
	if view.Operation != nil || view.Conversation != nil {
		t.Fatalf("durable record unexpectedly created presentation fields: %#v", view)
	}

	conversation := &dirextalkdomain.ConversationView{ConversationID: "conv", MatrixRoomID: record.RoomID}
	view.Operation = map[string]any{"action": "contacts.update"}
	view.Conversation = conversation
	if got := RecordFromView(view); got != record {
		t.Fatalf("presentation fields leaked into durable record: %#v", got)
	}
}

func TestViewSliceConversionsPreserveConcreteEmptyArrays(t *testing.T) {
	for _, records := range [][]dirextalkdomain.ContactRecord{nil, {}} {
		views := ViewsFromRecords(records)
		if views == nil || len(views) != 0 {
			t.Fatalf("ViewsFromRecords(%#v) = %#v, want non-nil empty slice", records, views)
		}
		raw, err := json.Marshal(views)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != "[]" {
			t.Fatalf("empty views JSON = %s, want []", raw)
		}
	}
	for _, views := range [][]View{nil, {}} {
		records := RecordsFromViews(views)
		if records == nil || len(records) != 0 {
			t.Fatalf("RecordsFromViews(%#v) = %#v, want non-nil empty slice", views, records)
		}
	}
}

func TestHandlersOwnContactActionsAndReturnConcreteViews(t *testing.T) {
	record := dirextalkdomain.ContactRecord{
		PeerMXID: "@alice:example.com", DisplayName: "Alice", RoomID: "!direct:example.com", Status: "accepted",
	}
	module := New(&testStore{records: []dirextalkdomain.ContactRecord{
		record,
		{PeerMXID: "@deleted:example.com", RoomID: "!deleted:example.com", Status: "deleted"},
	}}, nil, Config{})
	handlers := module.Handlers()
	names := make([]string, 0, len(handlers))
	for name, handler := range handlers {
		if handler == nil {
			t.Fatalf("handler %q is nil", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	if want := []string{"contacts.delete", "contacts.list", "contacts.reactivate", "contacts.request", "contacts.requests.accept", "contacts.requests.delete", "contacts.requests.reject", "contacts.update"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("handler names = %v, want %v", names, want)
	}

	result, apiErr := handlers["contacts.list"](context.Background(), map[string]any{"ignored": true})
	if apiErr != nil {
		t.Fatalf("contacts.list error = %#v", apiErr)
	}
	response, ok := result.(map[string]any)
	if !ok || len(response) != 1 {
		t.Fatalf("contacts.list result = %#v", result)
	}
	views, ok := response["contacts"].([]View)
	if !ok || !reflect.DeepEqual(views, []View{ViewFromRecord(record)}) {
		t.Fatalf("contacts.list contacts = %#v", response["contacts"])
	}
}

func TestContactsListHandlerReturnsEmptyArrayAndMapsStoreErrors(t *testing.T) {
	emptyModule := New(&testStore{}, nil, Config{})
	result, apiErr := emptyModule.Handlers()["contacts.list"](context.Background(), nil)
	if apiErr != nil {
		t.Fatalf("empty contacts.list error = %#v", apiErr)
	}
	views, ok := result.(map[string]any)["contacts"].([]View)
	if !ok || views == nil || len(views) != 0 {
		t.Fatalf("empty contacts.list contacts = %#v", result)
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"contacts":[]}` {
		t.Fatalf("empty contacts.list JSON = %s", raw)
	}

	wantErr := errors.New("read contacts")
	errorModule := New(&testStore{listErr: wantErr}, nil, Config{})
	result, apiErr = errorModule.Handlers()["contacts.list"](context.Background(), nil)
	if result != nil || apiErr == nil || apiErr.Status != http.StatusInternalServerError || apiErr.Error != "internal error: read contacts" {
		t.Fatalf("contacts.list Store failure = (%#v, %#v)", result, apiErr)
	}
}
