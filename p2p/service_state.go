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
	readMarkers   map[string]readMarker
	channels      map[string]channel
	posts         []channelPostRecord
	comments      []channelCommentRecord
	contacts      map[string]contactRecord
	blocks        map[string]blockRecord
	groups        map[string]groupRecord
	calls         map[string]callRecord
	favorites     map[int64]favoriteRecord
	follows       map[string]followRecord
	reactions     map[string]reactionRecord
	members       map[string]memberRecord
	conversations map[string]conversationRecord
	inviteGrants  map[string]channelInviteGrant
	plugins       map[string]pluginInstance
	pluginJobs    map[string]pluginJob
	pluginSecrets map[string]map[string]pluginSecret
}

type serviceEventState struct {
	events       []p2pEvent
	nextEventSeq int64
	eventNotify  chan struct{}
}

type serviceRealtimeState struct {
	realtimeSessions  *realtime.SessionStore
	realtimeWSTickets map[string]realtimeWSTicket
}

func newServiceReadModelState() serviceReadModelState {
	return serviceReadModelState{
		readMarkers:   map[string]readMarker{},
		channels:      map[string]channel{},
		contacts:      map[string]contactRecord{},
		blocks:        map[string]blockRecord{},
		groups:        map[string]groupRecord{},
		calls:         map[string]callRecord{},
		favorites:     map[int64]favoriteRecord{},
		follows:       map[string]followRecord{},
		reactions:     map[string]reactionRecord{},
		members:       map[string]memberRecord{},
		conversations: map[string]conversationRecord{},
		inviteGrants:  map[string]channelInviteGrant{},
		plugins:       map[string]pluginInstance{},
		pluginJobs:    map[string]pluginJob{},
		pluginSecrets: map[string]map[string]pluginSecret{},
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
