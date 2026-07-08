package p2p

import "context"

type groupOnlyStore struct{}

func (groupOnlyStore) UpsertGroup(context.Context, groupRecord) error {
	return nil
}

func (groupOnlyStore) DeleteGroup(context.Context, string) error {
	return nil
}

func (groupOnlyStore) ListGroups(context.Context) ([]groupRecord, error) {
	return nil, nil
}

func (groupOnlyStore) GetGroupByRoom(context.Context, string) (groupRecord, bool, error) {
	return groupRecord{}, false, nil
}

func (groupOnlyStore) ListJoinedGroupsForUser(context.Context, string) ([]groupRecord, error) {
	return nil, nil
}

var _ groupStore = groupOnlyStore{}
