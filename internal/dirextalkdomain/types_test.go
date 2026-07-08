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

func TestSocialCallReportRecordJSONContracts(t *testing.T) {
	raw, err := json.Marshal(struct {
		Call     CallRecord     `json:"call"`
		Favorite FavoriteRecord `json:"favorite"`
		Follow   FollowRecord   `json:"follow"`
		Reaction ReactionRecord `json:"reaction"`
		Report   ReportRecord   `json:"report"`
	}{
		Call: CallRecord{
			CallID:        "call_1",
			RoomID:        "!room:example.com",
			RoomType:      "direct",
			MediaType:     "video",
			CreatedByMXID: "@owner:example.com",
			State:         "connected",
			CreatedAt:     "2026-07-08T00:00:00Z",
		},
		Favorite: FavoriteRecord{
			ID:             7,
			EventID:        "$event:example.com",
			RoomID:         "!room:example.com",
			SenderID:       "@alice:example.com",
			SenderName:     "Alice",
			Content:        "hello",
			MessageType:    "text",
			OriginServerTS: 123,
			CreatedAt:      "2026-07-08T00:00:00Z",
		},
		Follow: FollowRecord{
			Domain:    "remote.example",
			CreatedAt: "2026-07-08T00:00:00Z",
		},
		Reaction: ReactionRecord{
			TargetType: "post",
			TargetID:   "post_1",
			ChannelID:  "channel_1",
			PostID:     "post_1",
			Reaction:   "like",
			UserID:     "@owner:example.com",
			Active:     true,
			CreatedAt:  "2026-07-08T00:00:00Z",
		},
		Report: ReportRecord{
			ReportID:            "report_1",
			TargetType:          "group",
			TargetRoomID:        "!room:example.com",
			TargetName:          "Group",
			ReporterMXID:        "@owner:example.com",
			ReporterDisplayName: "Owner",
			Reason:              "spam",
			Body:                "body",
			ImageURLs:           []string{"mxc://image"},
			SystemRoomID:        "!system:example.com",
			EventID:             "$report:example.com",
			OriginServerTS:      456,
			CreatedAt:           "2026-07-08T00:00:00Z",
		},
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var got map[string]map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if got["call"]["call_id"] != "call_1" || got["call"]["created_by_mxid"] != "@owner:example.com" {
		t.Fatalf("expected call JSON contract, got %#v", got["call"])
	}
	if _, ok := got["call"]["answered_at"]; ok {
		t.Fatalf("empty call answered_at should be omitted, got %#v", got["call"])
	}
	if got["favorite"]["origin_server_ts"] != float64(123) {
		t.Fatalf("expected favorite origin_server_ts contract, got %#v", got["favorite"])
	}
	if got["follow"]["domain"] != "remote.example" {
		t.Fatalf("expected follow JSON contract, got %#v", got["follow"])
	}
	if got["reaction"]["active"] != true || got["reaction"]["post_id"] != "post_1" {
		t.Fatalf("expected reaction JSON contract, got %#v", got["reaction"])
	}
	if got["report"]["reporter_mxid"] != "@owner:example.com" || got["report"]["origin_server_ts"] != float64(456) {
		t.Fatalf("expected report JSON contract, got %#v", got["report"])
	}
	if _, ok := got["report"]["target_channel_id"]; ok {
		t.Fatalf("empty report target_channel_id should be omitted, got %#v", got["report"])
	}
}
