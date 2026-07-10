package p2p

import "context"

type portalActionSession struct {
	DeviceID   string
	Generation uint64
}

type portalActionSessionContextKey struct{}

func withPortalActionSession(ctx context.Context, session portalActionSession) context.Context {
	return context.WithValue(ctx, portalActionSessionContextKey{}, session)
}

func portalActionSessionFromContext(ctx context.Context) (portalActionSession, bool) {
	session, ok := ctx.Value(portalActionSessionContextKey{}).(portalActionSession)
	return session, ok && session.DeviceID != "" && session.Generation != 0
}
