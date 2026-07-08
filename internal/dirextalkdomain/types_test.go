package dirextalkdomain

import (
	"encoding/json"
	"testing"
)

func TestMemberRecordJSONIncludesCompatibilityFields(t *testing.T) {
	raw, err := json.Marshal(MemberRecord{
		RoomID:     "!room:example.com",
		UserID:     "@alice:example.com",
		Membership: "join",
		Role:       "member",
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got["user_mxid"] != "@alice:example.com" {
		t.Fatalf("expected user_mxid compatibility field, got %#v", got)
	}
	if got["status"] != "join" {
		t.Fatalf("expected status compatibility field, got %#v", got)
	}
	if _, ok := got["RequesterNodeBaseURL"]; ok {
		t.Fatalf("RequesterNodeBaseURL must not be serialized: %#v", got)
	}
	if _, ok := got["requester_node_base_url"]; ok {
		t.Fatalf("requester_node_base_url must not be serialized: %#v", got)
	}
}

func TestSharedRecordJSONContracts(t *testing.T) {
	raw, err := json.Marshal(struct {
		Marker ReadMarker  `json:"marker"`
		Block  BlockRecord `json:"block"`
	}{
		Marker: ReadMarker{
			RoomID:         "!room:example.com",
			EventID:        "$event:example.com",
			OriginServerTS: 123,
		},
		Block: BlockRecord{
			TargetType:  "contact",
			TargetID:    "@alice:example.com",
			RoomID:      "!direct:example.com",
			PeerMXID:    "@alice:example.com",
			DisplayName: "Alice",
			AvatarURL:   "mxc://avatar",
			CreatedAt:   456,
		},
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got map[string]map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got["marker"]["origin_server_ts"] != float64(123) {
		t.Fatalf("expected read marker origin_server_ts contract, got %#v", got["marker"])
	}
	if got["block"]["target_type"] != "contact" || got["block"]["peer_mxid"] != "@alice:example.com" {
		t.Fatalf("expected block JSON contract, got %#v", got["block"])
	}
	if _, ok := got["block"]["channel_id"]; ok {
		t.Fatalf("empty channel_id should be omitted, got %#v", got["block"])
	}
}
