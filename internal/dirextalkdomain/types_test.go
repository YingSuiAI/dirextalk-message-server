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
