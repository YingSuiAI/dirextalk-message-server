package p2p

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

type recordingAccountDeactivator struct {
	users []string
	err   error
}

func (d *recordingAccountDeactivator) DeactivateAccount(ctx context.Context, localpart string) error {
	d.users = append(d.users, localpart)
	return d.err
}

type recordingAccountDeprovisioner struct {
	calls int
	err   error
}

func (d *recordingAccountDeprovisioner) DeprovisionAccount(ctx context.Context) error {
	d.calls++
	return d.err
}

func TestAccountDeleteRequiresExplicitConfirmation(t *testing.T) {
	service := NewService(Config{ServerName: "example.com"})
	bootstrapService(t, service)

	if _, apiErr := service.Handle(context.Background(), "portal.account.delete", map[string]any{}); apiErr == nil || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("expected account deletion to require confirmation, got %#v", apiErr)
	}
}

func TestAccountDeleteLeavesContactsDissolvesOwnedRoomsAndDeprovisions(t *testing.T) {
	transport := &recordingTransport{}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	deactivator := &recordingAccountDeactivator{}
	deprovisioner := &recordingAccountDeprovisioner{}
	service.SetAccountDeactivator(deactivator)
	service.SetAccountDeprovisioner(deprovisioner)
	bootstrapService(t, service)

	mustSeedAccountDeleteState(t, service)

	result := mustHandle[map[string]any](t, service, "portal.account.delete", map[string]any{
		"confirm": "delete_account",
	})

	if result["status"] != "deprovisioned" {
		t.Fatalf("expected deprovisioned status, got %#v", result)
	}
	expectedLeaves := []string{
		"@owner:example.com from !dm:remote.example",
		"@owner:example.com from !member-group:example.com",
		"@owner:example.com from !member-channel:example.com",
	}
	if len(transport.leaves) != len(expectedLeaves) {
		t.Fatalf("expected leaves %#v, got %#v", expectedLeaves, transport.leaves)
	}
	for _, expected := range expectedLeaves {
		if !hasString(transport.leaves, expected) {
			t.Fatalf("expected leave %q, got %#v", expected, transport.leaves)
		}
	}
	if !hasDissolvedState(transport.stateEvents, "!owned-group:example.com", DirexioRoomTypeGroup) {
		t.Fatalf("expected owned group dissolve state, got %#v", transport.stateEvents)
	}
	if !hasDissolvedState(transport.stateEvents, "!owned-channel:example.com", DirexioRoomTypeChannel) {
		t.Fatalf("expected owned channel dissolve state, got %#v", transport.stateEvents)
	}
	if got, want := deactivator.users, []string{"owner", "agent"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("expected Matrix account deactivation %#v, got %#v", want, got)
	}
	if deprovisioner.calls != 1 {
		t.Fatalf("expected one deprovision call, got %d", deprovisioner.calls)
	}
}

func TestAccountDeleteDoesNotDeprovisionWhenCriticalLeaveFails(t *testing.T) {
	ctx := context.Background()
	transport := &failingLeaveTransport{err: errors.New("leave failed")}
	service := NewServiceWithTransport(Config{ServerName: "example.com"}, transport)
	deprovisioner := &recordingAccountDeprovisioner{}
	service.SetAccountDeprovisioner(deprovisioner)
	bootstrapService(t, service)
	mustSeedAccountDeleteState(t, service)

	if _, apiErr := service.Handle(ctx, "portal.account.delete", map[string]any{
		"confirm": "delete_account",
	}); apiErr == nil || apiErr.Status != http.StatusInternalServerError {
		t.Fatalf("expected leave failure before deprovision, got %#v", apiErr)
	}
	if deprovisioner.calls != 0 {
		t.Fatalf("deprovision must not run after critical leave failure")
	}
}

func mustSeedAccountDeleteState(t *testing.T, service *Service) {
	t.Helper()
	ctx := context.Background()
	if err := service.saveContact(ctx, contactRecord{
		RoomID:   "!dm:remote.example",
		PeerMXID: "@peer:remote.example",
		Status:   "accepted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:     "!dm:remote.example",
		UserID:     "@owner:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	mustHandle[groupRecord](t, service, "groups.create", map[string]any{
		"room_id": "!owned-group:example.com",
		"name":    "Owned Group",
	})
	mustHandle[channel](t, service, "channels.create", map[string]any{
		"channel_id": "owned-channel",
		"room_id":    "!owned-channel:example.com",
		"name":       "Owned Channel",
	})
	if err := service.saveGroup(ctx, groupRecord{
		RoomID: "!member-group:example.com",
		Name:   "Member Group",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:     "!member-group:example.com",
		UserID:     "@owner:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveChannel(ctx, channel{
		ChannelID: "member-channel",
		RoomID:    "!member-channel:example.com",
		Name:      "Member Channel",
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.saveMember(ctx, memberRecord{
		RoomID:     "!member-channel:example.com",
		ChannelID:  "member-channel",
		UserID:     "@owner:example.com",
		Membership: "join",
		Role:       "member",
	}); err != nil {
		t.Fatal(err)
	}
}

func hasDissolvedState(states []SendStateEventRequest, roomID, roomType string) bool {
	for _, state := range states {
		if state.RoomID != roomID || state.Event.Type != DirexioRoomProfileEventType {
			continue
		}
		if state.Event.Content["room_type"] == roomType && state.Event.Content["dissolved"] == true {
			return true
		}
	}
	return false
}

func hasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
