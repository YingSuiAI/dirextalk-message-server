package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"

type groupStorageRecord = dirextalkdomain.GroupRecord

func groupStorageRecordFromGroup(group groupRecord) groupStorageRecord {
	return groupStorageRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}
}

func groupRecordFromStorage(group groupStorageRecord) groupRecord {
	return groupRecord{
		RoomID:       group.RoomID,
		Name:         group.Name,
		Topic:        group.Topic,
		AvatarURL:    group.AvatarURL,
		MemberCount:  group.MemberCount,
		InvitePolicy: group.InvitePolicy,
		Muted:        group.Muted,
	}
}

func groupRecordsFromStorage(groups []groupStorageRecord) []groupRecord {
	if len(groups) == 0 {
		return []groupRecord{}
	}
	result := make([]groupRecord, 0, len(groups))
	for _, group := range groups {
		result = append(result, groupRecordFromStorage(group))
	}
	return result
}
