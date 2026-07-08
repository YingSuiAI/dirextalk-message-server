package p2p

import "context"

type callOnlyStore struct{}

func (callOnlyStore) UpsertCall(context.Context, callRecord) error {
	return nil
}

func (callOnlyStore) ListCalls(context.Context, string, bool) ([]callRecord, error) {
	return nil, nil
}

var _ callStore = callOnlyStore{}
