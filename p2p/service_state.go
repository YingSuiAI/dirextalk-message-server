package p2p

import "github.com/YingSuiAI/dirextalk-message-server/internal/realtime"

type servicePortalState struct {
	initialized             bool
	password                string
	accessToken             string
	matrixDeviceID          string
	agentToken              string
	ownerMXID               string
	agentRoomID             string
	systemRoomID            string
	profile                 ownerProfile
	agentConfig             agentConfig
	clientBuild             clientBuild
	portalSessionGeneration uint64
}

type serviceRealtimeState struct {
	realtimeSessions  *realtime.SessionStore
	realtimeWSTickets map[string]realtimeWSTicket
}

func newServiceRealtimeState(sessions *realtime.SessionStore) serviceRealtimeState {
	return serviceRealtimeState{
		realtimeSessions:  sessions,
		realtimeWSTickets: map[string]realtimeWSTicket{},
	}
}
