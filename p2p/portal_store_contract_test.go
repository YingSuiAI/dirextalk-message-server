package p2p

import "context"

type portalOnlyStore struct{}

func (portalOnlyStore) LoadPortal(context.Context) (portalState, bool, error) {
	return portalState{}, false, nil
}

func (portalOnlyStore) SavePortal(context.Context, portalState) error {
	return nil
}

var _ portalStore = portalOnlyStore{}
