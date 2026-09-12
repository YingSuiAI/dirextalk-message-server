package productpolicy

import (
	"context"
	"encoding/json"
	"github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/matrix-org/gomatrixserverlib"
	"strings"
)

// Native Ying is a reserved Matrix service identity on every owner node.
func IsNativeYingMXID(mxid string) bool {
	local, _, err := gomatrixserverlib.SplitID('@', mxid)
	return err == nil && local == "ying"
}

func validateGroupAgentMessage(ctx context.Context, q CurrentStateQuerier, r ClientEventRequest) error {
	marker, present := r.Content["io.dirextalk.group_agent"]
	if !present && !strings.HasPrefix(stringValue(r.Content["msg_type"]), "group_agent_") && !IsNativeYingMXID(r.SenderMXID) {
		return nil
	}
	var origin struct {
		OwnerMXID       string `json:"owner_mxid"`
		AgentMXID       string `json:"agent_mxid"`
		BindingRevision int64  `json:"binding_revision"`
		RequestID       string `json:"request_id"`
	}
	raw, err := json.Marshal(marker)
	if err != nil || json.Unmarshal(raw, &origin) != nil || !IsNativeYingMXID(r.SenderMXID) || origin.AgentMXID != r.SenderMXID || origin.BindingRevision <= 0 || origin.RequestID == "" {
		return Forbidden("group Agent identity cannot be impersonated")
	}
	tuple := gomatrixserverlib.StateKeyTuple{EventType: "io.dirextalk.group_agent", StateKey: ""}
	var state api.QueryCurrentStateResponse
	if err = q.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{RoomID: r.RoomID, StateTuples: []gomatrixserverlib.StateKeyTuple{tuple}}, &state); err != nil {
		return err
	}
	event := state.StateEvents[tuple]
	if event == nil {
		return Forbidden("group Agent binding is unavailable")
	}
	var binding struct {
		Enabled   bool   `json:"enabled"`
		RoomID    string `json:"room_id"`
		OwnerMXID string `json:"owner_mxid"`
		AgentMXID string `json:"agent_mxid"`
		Revision  int64  `json:"revision"`
	}
	if json.Unmarshal(event.Content(), &binding) != nil || !binding.Enabled || binding.RoomID != r.RoomID || binding.OwnerMXID != origin.OwnerMXID || binding.AgentMXID != r.SenderMXID || binding.Revision != origin.BindingRevision {
		return Forbidden("group Agent binding was revoked")
	}
	return nil
}
