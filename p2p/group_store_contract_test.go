package p2p

import "context"

type groupOnlyStore struct{}

func (groupOnlyStore) UpsertGroup(context.Context, groupStorageRecord) error {
	return nil
}

func (groupOnlyStore) DeleteGroup(context.Context, string) error {
	return nil
}

func (groupOnlyStore) ListGroups(context.Context) ([]groupStorageRecord, error) {
	return nil, nil
}

func (groupOnlyStore) GetGroupByRoom(context.Context, string) (groupStorageRecord, bool, error) {
	return groupStorageRecord{}, false, nil
}

func (groupOnlyStore) ListJoinedGroupsForUser(context.Context, string) ([]groupStorageRecord, error) {
	return nil, nil
}

var _ groupStore = groupOnlyStore{}
