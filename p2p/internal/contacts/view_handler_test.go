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

func TestHandlersOwnOnlyContactsListAndReturnConcreteViews(t *testing.T) {
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
	if want := []string{"contacts.delete", "contacts.list", "contacts.reactivate", "contacts.requests.accept", "contacts.requests.delete", "contacts.requests.reject", "contacts.update"}; !reflect.DeepEqual(names, want) {
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
