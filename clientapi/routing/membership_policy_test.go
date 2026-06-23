package routing

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/YingSuiAI/direxio-message-server/setup/config"
	dendritetest "github.com/YingSuiAI/direxio-message-server/test"
	userapi "github.com/YingSuiAI/direxio-message-server/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestSendInviteAppliesDirexioProductPolicy(t *testing.T) {
	owner := dendritetest.NewUser(t)
	member := dendritetest.NewUser(t)
	invitee := dendritetest.NewUser(t)
	room := dendritetest.NewRoom(t, owner)
	room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, dendritetest.WithStateKey(member.ID))
	room.CreateAndInsert(t, owner, "io.direxio.room.profile", map[string]any{
		"room_type":     "io.direxio.room.group",
		"invite_policy": "owner",
	}, dendritetest.WithStateKey(""))
	rsAPI := &redactionPolicyRoomserver{
		roomID: room.ID,
		state:  room.CurrentState(),
	}
	req, err := http.NewRequest("POST", "https://domain", io.NopCloser(strings.NewReader(`{"user_id":"`+invitee.ID+`"}`)))
	if err != nil {
		t.Fatal(err)
	}

	resp := SendInvite(req, nil, &userapi.Device{UserID: member.ID}, room.ID, &config.ClientAPI{}, rsAPI, nil)

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected Direxio product policy 403, got %d with %#v", resp.Code, resp.JSON)
	}
}
