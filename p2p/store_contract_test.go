package p2p

import "context"

type blockOnlyStore struct{}

func (blockOnlyStore) UpsertBlock(context.Context, blockRecord) error {
	return nil
}

func (blockOnlyStore) DeleteBlock(context.Context, string, string) (bool, error) {
	return false, nil
}

func (blockOnlyStore) ListBlocks(context.Context) ([]blockRecord, error) {
	return nil, nil
}

var _ blockStore = blockOnlyStore{}

type callOnlyStore struct{}

func (callOnlyStore) UpsertCall(context.Context, callRecord) error {
	return nil
}

func (callOnlyStore) ListCalls(context.Context, string, bool) ([]callRecord, error) {
	return nil, nil
}

var _ callStore = callOnlyStore{}

type channelContentOnlyStore struct{}

func (channelContentOnlyStore) InsertChannelPost(context.Context, channelPostStorageRecord) error {
	return nil
}

func (channelContentOnlyStore) GetChannelPostByID(context.Context, string, string) (channelPostStorageRecord, bool, error) {
	return channelPostStorageRecord{}, false, nil
}

func (channelContentOnlyStore) GetChannelPostByEventID(context.Context, string, string) (channelPostStorageRecord, bool, error) {
	return channelPostStorageRecord{}, false, nil
}

func (channelContentOnlyStore) ListChannelPosts(context.Context, string) ([]channelPostStorageRecord, error) {
	return nil, nil
}

func (channelContentOnlyStore) ListChannelPostsPage(context.Context, string, int64, int64, int64, string, int) ([]channelPostStorageRecord, bool, error) {
	return nil, false, nil
}

func (channelContentOnlyStore) InsertChannelComment(context.Context, channelCommentStorageRecord) error {
	return nil
}

func (channelContentOnlyStore) GetChannelCommentByID(context.Context, string, string) (channelCommentStorageRecord, bool, error) {
	return channelCommentStorageRecord{}, false, nil
}

func (channelContentOnlyStore) GetChannelCommentByEventID(context.Context, string, string) (channelCommentStorageRecord, bool, error) {
	return channelCommentStorageRecord{}, false, nil
}

func (channelContentOnlyStore) ListChannelComments(context.Context, string) ([]channelCommentStorageRecord, error) {
	return nil, nil
}

func (channelContentOnlyStore) ListChannelCommentsPage(context.Context, string, int64, int64, int64, string, int) ([]channelCommentStorageRecord, bool, error) {
	return nil, false, nil
}

func (channelContentOnlyStore) DeleteChannelPost(context.Context, string) (bool, error) {
	return false, nil
}

func (channelContentOnlyStore) DeleteChannelComment(context.Context, string) (bool, error) {
	return false, nil
}

var _ channelContentStore = channelContentOnlyStore{}

type channelOnlyStore struct{}

func (channelOnlyStore) UpsertChannel(context.Context, channel) error {
	return nil
}

func (channelOnlyStore) DeleteChannel(context.Context, string) error {
	return nil
}

func (channelOnlyStore) ListChannels(context.Context) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) GetChannelByIDOrRoom(context.Context, string, string) (channel, bool, error) {
	return channel{}, false, nil
}

func (channelOnlyStore) ListJoinedChannelsForUser(context.Context, string) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) SearchPublicChannels(context.Context, string, int) ([]channel, error) {
	return nil, nil
}

func (channelOnlyStore) ListPublicChannelsForOwner(context.Context, string) ([]channel, error) {
	return nil, nil
}

var _ channelStore = channelOnlyStore{}

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

type conversationOnlyStore struct{}

func (conversationOnlyStore) UpsertConversation(context.Context, conversationRecord) error {
	return nil
}

func (conversationOnlyStore) GetConversationByID(context.Context, string) (conversationRecord, bool, error) {
	return conversationRecord{}, false, nil
}

func (conversationOnlyStore) GetConversationByRoomID(context.Context, string) (conversationRecord, bool, error) {
	return conversationRecord{}, false, nil
}

func (conversationOnlyStore) ListConversations(context.Context) ([]conversationRecord, error) {
	return nil, nil
}

func (conversationOnlyStore) DeleteConversationByRoomID(context.Context, string) error {
	return nil
}

var _ conversationStore = conversationOnlyStore{}

type eventOnlyStore struct{}

func (eventOnlyStore) InsertEvent(context.Context, p2pEvent) (bool, error) {
	return false, nil
}

func (eventOnlyStore) ListEvents(context.Context, int64, int) ([]p2pEvent, error) {
	return nil, nil
}

func (eventOnlyStore) EventBounds(context.Context) (eventBounds, error) {
	return eventBounds{}, nil
}

func (eventOnlyStore) PruneEventsToMaxRows(context.Context, int64) (int64, error) {
	return 0, nil
}

var _ eventStore = eventOnlyStore{}

type groupOnlyStore struct{}

func (groupOnlyStore) UpsertGroup(context.Context, groupStorageRecord) error {
	return nil
}

func (groupOnlyStore) DeleteGroup(context.Context, string) error {
	return nil
}

func (groupOnlyStore) ListGroups(context.Context) ([]groupStorageRecord, error) {
	return nil, nil
}

func (groupOnlyStore) GetGroupByRoom(context.Context, string) (groupStorageRecord, bool, error) {
	return groupStorageRecord{}, false, nil
}

func (groupOnlyStore) ListJoinedGroupsForUser(context.Context, string) ([]groupStorageRecord, error) {
	return nil, nil
}

var _ groupStore = groupOnlyStore{}

type memberOnlyStore struct{}

func (memberOnlyStore) UpsertMember(context.Context, memberRecord) error {
	return nil
}

func (memberOnlyStore) LookupMember(context.Context, string, string) (memberRecord, bool, error) {
	return memberRecord{}, false, nil
}

func (memberOnlyStore) ListMembers(context.Context, string, string) ([]memberRecord, error) {
	return nil, nil
}

func (memberOnlyStore) ListMembersForUser(context.Context, string) ([]memberRecord, error) {
	return nil, nil
}

func (memberOnlyStore) CountProductMembers(context.Context, string, string) (joined, pending int64, err error) {
	return 0, 0, nil
}

func (memberOnlyStore) CountJoinedMembers(context.Context, string, string) (int64, error) {
	return 0, nil
}

var _ memberStore = memberOnlyStore{}

type pluginOnlyStore struct{}

func (pluginOnlyStore) UpsertPlugin(context.Context, pluginInstance) error {
	return nil
}

func (pluginOnlyStore) ListPlugins(context.Context) ([]pluginInstance, error) {
	return nil, nil
}

func (pluginOnlyStore) GetPlugin(context.Context, string) (pluginInstance, bool, error) {
	return pluginInstance{}, false, nil
}

func (pluginOnlyStore) UpsertPluginJob(context.Context, pluginJob) error {
	return nil
}

func (pluginOnlyStore) GetPluginJob(context.Context, string) (pluginJob, bool, error) {
	return pluginJob{}, false, nil
}

func (pluginOnlyStore) UpsertPluginSecret(context.Context, pluginSecret) error {
	return nil
}

func (pluginOnlyStore) GetPluginSecret(context.Context, string, string) (pluginSecret, bool, error) {
	return pluginSecret{}, false, nil
}

var _ pluginStore = pluginOnlyStore{}

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

type reactionOnlyStore struct{}

func (reactionOnlyStore) UpsertReaction(context.Context, reactionRecord) error {
	return nil
}

func (reactionOnlyStore) GetReaction(context.Context, string, string, string, string) (reactionRecord, bool, error) {
	return reactionRecord{}, false, nil
}

func (reactionOnlyStore) CountActiveReactions(context.Context, string, string, string) (int64, error) {
	return 0, nil
}

func (reactionOnlyStore) ListReactions(context.Context, string) ([]reactionRecord, error) {
	return nil, nil
}

var _ reactionStore = reactionOnlyStore{}

type readMarkerOnlyStore struct{}

func (readMarkerOnlyStore) SaveReadMarker(context.Context, readMarker) error {
	return nil
}

var _ readMarkerStore = readMarkerOnlyStore{}

type reportOnlyStore struct{}

func (reportOnlyStore) InsertReport(context.Context, reportRecord) error {
	return nil
}

var _ reportStore = reportOnlyStore{}

type socialOnlyStore struct{}

func (socialOnlyStore) UpsertFavorite(context.Context, favoriteRecord) error {
	return nil
}

func (socialOnlyStore) FindFavoriteByEvent(context.Context, string, string) (favoriteRecord, bool, error) {
	return favoriteRecord{}, false, nil
}

func (socialOnlyStore) ListFavorites(context.Context, string) ([]favoriteRecord, error) {
	return nil, nil
}

func (socialOnlyStore) DeleteFavorite(context.Context, int64) error {
	return nil
}

func (socialOnlyStore) UpsertFollow(context.Context, followRecord) error {
	return nil
}

func (socialOnlyStore) ListFollows(context.Context) ([]followRecord, error) {
	return nil, nil
}

func (socialOnlyStore) DeleteFollow(context.Context, string) error {
	return nil
}

var _ socialStore = socialOnlyStore{}

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
