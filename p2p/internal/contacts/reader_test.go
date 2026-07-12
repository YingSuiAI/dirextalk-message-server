package contacts

import (
	"context"
	"errors"
	"reflect"
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

func TestContactReadersPropagateStoreErrors(t *testing.T) {
	wantErr := errors.New("read contacts")
	module := New(&testStore{listErr: wantErr}, nil, Config{})

	tests := []struct {
		name string
		read func() error
	}{
		{name: "raw", read: func() error { _, err := module.ListRaw(context.Background()); return err }},
		{name: "visible", read: func() error { _, err := module.ListVisible(context.Background()); return err }},
		{name: "room lookup", read: func() error { _, _, err := module.LookupByRoom(context.Background(), "!room:example.com"); return err }},
		{name: "peer lookup", read: func() error { _, _, err := module.LookupByPeer(context.Background(), "@alice:example.com"); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.read(); !errors.Is(err, wantErr) {
				t.Fatalf("error = %v, want %v", err, wantErr)
			}
		})
	}
}
