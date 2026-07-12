package storage

import (
	"context"
	"testing"
)

func TestMemoryStoreResetAccountStateClearsProductDataAndKeepsPlugins(t *testing.T) {
	store := NewMemoryStore()
	store.mu.Lock()
	store.portal = &portalState{MatrixDeviceID: "DEVICE"}
	store.readMarks["room"] = readMarker{RoomID: "room"}
	store.conversations["conversation"] = conversationRecord{ConversationID: "conversation"}
	store.channels["channel"] = channel{ChannelID: "channel"}
	store.inviteGrants["grant"] = channelInviteGrant{GrantID: "grant"}
	store.posts = []channelPostRecord{{PostID: "post"}}
	store.comments = []channelCommentRecord{{CommentID: "comment"}}
	store.contacts["room"] = contactRecord{RoomID: "room"}
	store.blocks["contact|user"] = blockRecord{TargetID: "user"}
	store.groups["room"] = groupRecord{RoomID: "room"}
	store.calls["call"] = callRecord{CallID: "call"}
	store.favorites[1] = favoriteRecord{ID: 1}
	store.follows["example.com"] = followRecord{Domain: "example.com"}
	store.reactions["reaction"] = reactionRecord{TargetID: "target"}
	store.members["room|user"] = memberRecord{RoomID: "room", UserID: "user"}
	store.events = []p2pEvent{{Seq: 1, DedupeKey: "reusable"}}
	store.eventSeq[1] = struct{}{}
	store.eventDedupe["reusable"] = 1
	store.reports["report"] = reportRecord{ReportID: "report"}
	store.plugins["plugin"] = pluginInstance{ID: "plugin"}
	store.pluginJobs["job"] = pluginJob{JobID: "job", PluginID: "plugin"}
	store.pluginSecrets["plugin"] = map[string]pluginSecret{"token": {PluginID: "plugin", Name: "token"}}
	store.mu.Unlock()

	store.ResetAccountState()

	store.mu.RLock()
	cleared := map[string]int{
		"read markers": len(store.readMarks), "conversations": len(store.conversations), "channels": len(store.channels),
		"invite grants": len(store.inviteGrants), "posts": len(store.posts), "comments": len(store.comments),
		"contacts": len(store.contacts), "blocks": len(store.blocks), "groups": len(store.groups), "calls": len(store.calls),
		"favorites": len(store.favorites), "follows": len(store.follows), "reactions": len(store.reactions), "members": len(store.members),
		"events": len(store.events), "event seq": len(store.eventSeq), "event dedupe": len(store.eventDedupe), "reports": len(store.reports),
	}
	portal := store.portal
	pluginCount, jobCount, secretCount := len(store.plugins), len(store.pluginJobs), len(store.pluginSecrets["plugin"])
	store.mu.RUnlock()

	if portal != nil {
		t.Fatalf("portal was not reset: %#v", portal)
	}
	for name, count := range cleared {
		if count != 0 {
			t.Fatalf("%s count after reset = %d", name, count)
		}
	}
	if pluginCount != 1 || jobCount != 1 || secretCount != 1 {
		t.Fatalf("plugin state was not preserved: plugins=%d jobs=%d secrets=%d", pluginCount, jobCount, secretCount)
	}
	inserted, err := store.InsertEvent(context.Background(), p2pEvent{Seq: 2, DedupeKey: "reusable"})
	if err != nil || !inserted {
		t.Fatalf("event dedupe index survived reset: inserted=%v err=%v", inserted, err)
	}
}
