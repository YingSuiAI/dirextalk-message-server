package p2p

type aggregateFocusedStore struct {
	portalOnlyStore
	readMarkerOnlyStore
	channelOnlyStore
	channelContentOnlyStore
	contactOnlyStore
	blockOnlyStore
	groupOnlyStore
	callOnlyStore
	socialOnlyStore
	reactionOnlyStore
	memberOnlyStore
	conversationOnlyStore
	eventOnlyStore
	pluginOnlyStore
	reportOnlyStore
}

var _ Store = aggregateFocusedStore{}
