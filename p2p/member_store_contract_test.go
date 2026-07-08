package p2p

import "context"

type memberOnlyStore struct{}

func (memberOnlyStore) UpsertMember(context.Context, memberRecord) error {
	return nil
}

func (memberOnlyStore) LookupMember(context.Context, string, string) (memberRecord, bool, error) {
	return memberRecord{}, false, nil
}

func (memberOnlyStore) ListMembers(context.Context, string, string) ([]memberRecord, error) {
	return nil, nil
}

func (memberOnlyStore) ListMembersForUser(context.Context, string) ([]memberRecord, error) {
	return nil, nil
}

func (memberOnlyStore) CountProductMembers(context.Context, string, string) (joined, pending int64, err error) {
	return 0, 0, nil
}

func (memberOnlyStore) CountJoinedMembers(context.Context, string, string) (int64, error) {
	return 0, nil
}

var _ memberStore = memberOnlyStore{}
