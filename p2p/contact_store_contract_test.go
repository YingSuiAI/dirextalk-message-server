package p2p

import "context"

type contactOnlyStore struct{}

func (contactOnlyStore) UpsertContact(context.Context, contactStorageRecord) error {
	return nil
}

func (contactOnlyStore) ListContacts(context.Context) ([]contactStorageRecord, error) {
	return nil, nil
}

func (contactOnlyStore) UpsertChannelInviteGrant(context.Context, channelInviteGrant) error {
	return nil
}

func (contactOnlyStore) ListChannelInviteGrants(context.Context) ([]channelInviteGrant, error) {
	return nil, nil
}

var _ contactStore = contactOnlyStore{}
