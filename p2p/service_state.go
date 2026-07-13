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

type serviceReadModelState struct {
	readMarkers map[string]readMarker
}

type serviceEventState struct {
	nextEventSeq int64
	eventNotify  chan struct{}
}

type serviceRealtimeState struct {
	realtimeSessions  *realtime.SessionStore
	realtimeWSTickets map[string]realtimeWSTicket
}

func newServiceReadModelState() serviceReadModelState {
	return serviceReadModelState{
		readMarkers: map[string]readMarker{},
	}
}

func newServiceEventState() serviceEventState {
	return serviceEventState{eventNotify: make(chan struct{})}
}

func newServiceRealtimeState(sessions *realtime.SessionStore) serviceRealtimeState {
	return serviceRealtimeState{
		realtimeSessions:  sessions,
		realtimeWSTickets: map[string]realtimeWSTicket{},
	}
}
