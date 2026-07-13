package p2p

import (
	"context"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	releasemodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/release"
)

const (
	clientSessionStaleCode = releasemodule.ClientSessionStaleCode
	updaterUnavailableCode = releasemodule.UpdaterUnavailableCode
)

type serviceReleasePort struct{ service *Service }

func (p serviceReleasePort) Session(ctx context.Context) (releasemodule.Session, bool) {
	session, ok := portalActionSessionFromContext(ctx)
	return releasemodule.Session{DeviceID: session.DeviceID, Generation: session.Generation}, ok
}

func (p serviceReleasePort) Snapshot() releasemodule.Snapshot {
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	return releasemodule.Snapshot{
		DeviceID:   cleanMatrixDeviceID(p.service.matrixDeviceID),
		Generation: p.service.portalSessionGeneration,
		Client:     p.service.clientBuild,
		Controller: p.service.releaseController,
	}
}

func (p serviceReleasePort) SaveClientBuild(ctx context.Context, expectedDeviceID string, build dirextalkdomain.ClientBuild) (bool, error) {
	store := p.service.portalStore()
	if store == nil {
		return true, nil
	}
	return store.SaveClientBuild(ctx, expectedDeviceID, build)
}

func (p serviceReleasePort) CommitClientBuild(build dirextalkdomain.ClientBuild) string {
	p.service.mu.Lock()
	defer p.service.mu.Unlock()
	p.service.clientBuild = build
	return cleanMatrixDeviceID(p.service.matrixDeviceID)
}
