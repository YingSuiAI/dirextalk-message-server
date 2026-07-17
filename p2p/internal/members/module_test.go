package members

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	actionbase "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/action"
)

type listCall struct {
	roomID    string
	channelID string
}

type testStore struct {
	records []dirextalkdomain.MemberRecord
	err     error
	calls   []listCall
}

func (s *testStore) ListMembers(_ context.Context, roomID, channelID string) ([]dirextalkdomain.MemberRecord, error) {
	s.calls = append(s.calls, listCall{roomID: roomID, channelID: channelID})
	return append([]dirextalkdomain.MemberRecord(nil), s.records...), s.err
}

func (s *testStore) UpsertChannelInviteGrant(context.Context, dirextalkdomain.ChannelInviteGrant) error {
	return nil
}

func (s *testStore) ListChannelInviteGrants(context.Context) ([]dirextalkdomain.ChannelInviteGrant, error) {
	return nil, nil
}

func TestSharedMemberListHandlersReturnEmptyArrays(t *testing.T) {
	module := New(&testStore{}, Config{})
	handlers := module.Handlers()
	for _, name := range []string{"channels.members", "groups.members"} {
		result, actionErr := handlers[name](context.Background(), nil)
		if actionErr != nil {
			t.Fatalf("%s error = %#v", name, actionErr)
		}
		members, ok := result.(map[string]any)["members"].([]dirextalkdomain.MemberRecord)
		if !ok || members == nil || len(members) != 0 {
			t.Fatalf("%s empty members = %#v", name, result)
		}
	}
}

func TestListFiltersHiddenMembersNormalizesRolesAndSortsCopy(t *testing.T) {
	records := []dirextalkdomain.MemberRecord{
		{RoomID: "!room:example.com", UserID: "@zero:example.com", Membership: "join", Role: "legacy-admin"},
		{RoomID: "!room:example.com", UserID: "@owner:example.com", Membership: " JOIN ", Role: " OWNER ", JoinedAt: 30},
		{RoomID: "!room:example.com", UserID: "@early:example.com", Membership: "invite", Role: "", JoinedAt: 10},
		{RoomID: "!room:example.com", UserID: "@late:example.com", Membership: "pending", Role: "member", JoinedAt: 20},
		{RoomID: "!room:example.com", UserID: "@left:example.com", Membership: " Left ", Role: "owner", JoinedAt: 1},
		{RoomID: "!room:example.com", UserID: "@removed:example.com", Membership: "BANNED", Role: "member", JoinedAt: 2},
	}
	original := append([]dirextalkdomain.MemberRecord(nil), records...)
	store := &testStore{records: records}
	module := New(store, Config{})

	result, actionErr := module.List(context.Background(), map[string]any{
		"room_id": " !room:example.com ", "channel_id": " channel-1 ",
	})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	members := result.(map[string]any)["members"].([]dirextalkdomain.MemberRecord)
	if got, want := memberIDs(members), []string{"@early:example.com", "@late:example.com", "@owner:example.com", "@zero:example.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("member order = %#v, want %#v; records=%#v", got, want, members)
	}
	for _, member := range members {
		wantRole := "member"
		if member.UserID == "@owner:example.com" {
			wantRole = "owner"
		}
		if member.Role != wantRole {
			t.Fatalf("member %s role = %q, want %q", member.UserID, member.Role, wantRole)
		}
	}
	if !reflect.DeepEqual(store.calls, []listCall{{roomID: "!room:example.com", channelID: "channel-1"}}) {
		t.Fatalf("store calls = %#v", store.calls)
	}
	if !reflect.DeepEqual(store.records, original) {
		t.Fatalf("list mutated stored records: got %#v want %#v", store.records, original)
	}

	raw, err := json.Marshal(members[0])
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"user_id", "user_mxid", "membership", "status"} {
		if _, ok := fields[field]; !ok {
			t.Fatalf("legacy member JSON field %q missing from %s", field, raw)
		}
	}
}

func TestListStatusAliasAndRoleFilters(t *testing.T) {
	store := &testStore{records: []dirextalkdomain.MemberRecord{
		{UserID: "@owner:example.com", Membership: "pending", Role: "owner", JoinedAt: 30},
		{UserID: "@pending:example.com", Membership: "PENDING", Role: "legacy", JoinedAt: 20},
		{UserID: "@joined:example.com", Membership: "join", Role: "legacy", JoinedAt: 10},
	}}
	module := New(store, Config{})
	tests := []struct {
		name string
		raw  map[string]any
		want []string
	}{
		{name: "membership alias", raw: map[string]any{"membership": " pending ", "role": " member "}, want: []string{"@pending:example.com"}},
		{name: "status wins", raw: map[string]any{"status": " join ", "membership": "pending"}, want: []string{"@joined:example.com"}},
		{name: "owner role", raw: map[string]any{"role": "OWNER"}, want: []string{"@owner:example.com"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, actionErr := module.List(context.Background(), tt.raw)
			if actionErr != nil {
				t.Fatal(actionErr)
			}
			members := result.(map[string]any)["members"].([]dirextalkdomain.MemberRecord)
			if got := memberIDs(members); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("members = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestSortByJoinOrderIgnoresRoleAndPlacesZeroLast(t *testing.T) {
	members := []dirextalkdomain.MemberRecord{
		{UserID: "@zero-b:example.com", Role: "member"},
		{UserID: "@same-b:example.com", Role: "member", JoinedAt: 10},
		{UserID: "@owner:member.example.com", Role: "OWNER", JoinedAt: 100},
		{UserID: "@owner:creator.example.com", Role: "member", JoinedAt: 5},
		{UserID: "@same-a:example.com", Role: "member", JoinedAt: 10},
		{UserID: "@zero-a:example.com", Role: "member"},
	}
	SortByJoinOrder(members)
	if got, want := memberIDs(members), []string{"@owner:creator.example.com", "@same-a:example.com", "@same-b:example.com", "@owner:member.example.com", "@zero-a:example.com", "@zero-b:example.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("member order = %#v, want %#v", got, want)
	}
}

func TestListFallsBackToJoinOrderWhenProjectedOwnerMissing(t *testing.T) {
	const roomID = "!group:creator.example.com"
	store := &testStore{records: []dirextalkdomain.MemberRecord{
		{RoomID: roomID, UserID: "@owner:member.example.com", Membership: "join", Role: "owner", JoinedAt: 30},
		{RoomID: roomID, UserID: "@owner:creator.example.com", Membership: "join", Role: "member", JoinedAt: 10},
	}}
	resolvedRoomID := ""
	module := New(store, Config{
		ResolveRoomOwner: func(_ context.Context, roomID string) (string, error) {
			resolvedRoomID = roomID
			return "", nil
		},
	})

	result, actionErr := module.List(context.Background(), map[string]any{"room_id": roomID})
	if actionErr != nil {
		t.Fatal(actionErr)
	}
	members := result.(map[string]any)["members"].([]dirextalkdomain.MemberRecord)
	if resolvedRoomID != roomID {
		t.Fatalf("resolved room = %q, want %q", resolvedRoomID, roomID)
	}
	if got, want := memberIDs(members), []string{"@owner:creator.example.com", "@owner:member.example.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallback member order = %#v, want %#v", got, want)
	}
}

func TestSortByOwnerThenJoinOrderMatchesExactFullMXID(t *testing.T) {
	members := []dirextalkdomain.MemberRecord{
		{UserID: "@owner:member.example.com", Role: "owner", JoinedAt: 10},
		{UserID: "@alice:example.com", Role: "member", JoinedAt: 20},
		{UserID: "@owner:creator.example.com", Role: "member", JoinedAt: 30},
	}
	SortByOwnerThenJoinOrder(members, " @owner:creator.example.com ")
	if got, want := memberIDs(members), []string{"@owner:creator.example.com", "@owner:member.example.com", "@alice:example.com"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("creator member order = %#v, want %#v", got, want)
	}
}

func TestApplyMemberProfileOnlyMergesNonEmptyNormalizedFields(t *testing.T) {
	member := dirextalkdomain.MemberRecord{
		DisplayName: "Existing", AvatarURL: "mxc://example.com/existing", Domain: "example.com",
	}
	ApplyMemberProfile(&member, actionbase.Params{
		"display_name": " Updated ",
		"avatar_url":   "   ",
		"domain":       " remote.example ",
	})
	if member.DisplayName != "Updated" || member.AvatarURL != "mxc://example.com/existing" || member.Domain != "remote.example" {
		t.Fatalf("profile merge = %#v", member)
	}
}

func memberIDs(members []dirextalkdomain.MemberRecord) []string {
	ids := make([]string, 0, len(members))
	for _, member := range members {
		ids = append(ids, member.UserID)
	}
	return ids
}
