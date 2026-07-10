package p2p

import "context"

type portalOnlyStore struct{}

func (portalOnlyStore) LoadPortal(context.Context) (portalState, bool, error) {
	return portalState{}, false, nil
}

func (portalOnlyStore) SavePortal(context.Context, portalState) error {
	return nil
}

func (portalOnlyStore) SaveClientBuild(context.Context, string, clientBuild) (bool, error) {
	return true, nil
}

var _ portalStore = portalOnlyStore{}
