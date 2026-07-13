package p2p

import (
	"context"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/internal/dirextalkdomain"
	groupsmodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/groups"
	projectormodule "github.com/YingSuiAI/dirextalk-message-server/p2p/internal/projector"
)

type serviceProjectorChannelPort struct{ service *Service }

func (p serviceProjectorChannelPort) ByIDOrRoom(ctx context.Context, channelID, roomID string) (dirextalkdomain.Channel, bool, error) {
	return p.service.channelsModule.ByIDOrRoom(ctx, channelID, roomID)
}

func (p serviceProjectorChannelPort) UpsertProjection(ctx context.Context, channel dirextalkdomain.Channel) error {
	return p.service.store.UpsertChannel(ctx, channel)
}

func (p serviceProjectorChannelPort) SaveWithConversation(ctx context.Context, channel dirextalkdomain.Channel) error {
	return p.service.channelsModule.Save(ctx, channel)
}

func (p serviceProjectorChannelPort) Delete(ctx context.Context, channelID string) error {
	return p.service.channelsModule.Delete(ctx, channelID)
}

type serviceProjectorGroupPort struct{ service *Service }

func (p serviceProjectorGroupPort) ByRoom(ctx context.Context, roomID string) (dirextalkdomain.GroupRecord, bool, error) {
	group, ok, err := p.service.groupsModule.ByRoom(ctx, roomID)
	return groupsmodule.RecordFromView(group), ok, err
}

func (p serviceProjectorGroupPort) Save(ctx context.Context, group dirextalkdomain.GroupRecord) error {
	return p.service.groupsModule.Save(ctx, groupsmodule.ViewFromRecord(group))
}

func (p serviceProjectorGroupPort) Delete(ctx context.Context, roomID string) error {
	return p.service.groupsModule.Delete(ctx, roomID)
}

type serviceProjectorContactPort struct{ service *Service }

func (p serviceProjectorContactPort) WithPeer(peerMXID string, fn func() error) error {
	var workflowErr error
	p.service.contactsModule.SerializePeer(peerMXID, func() { workflowErr = fn() })
	return workflowErr
}

func (p serviceProjectorContactPort) ListRaw(ctx context.Context) ([]dirextalkdomain.ContactRecord, error) {
	return p.service.contactsModule.ListRaw(ctx)
}

func (p serviceProjectorContactPort) LookupByRoom(ctx context.Context, roomID string) (dirextalkdomain.ContactRecord, bool, error) {
	return p.service.contactsModule.LookupByRoom(ctx, roomID)
}

func (p serviceProjectorContactPort) Save(ctx context.Context, contact dirextalkdomain.ContactRecord) error {
	return p.service.contactsModule.Save(ctx, contact)
}

func (p serviceProjectorContactPort) SaveProjectionIfCurrent(
	ctx context.Context,
	contact,
	expected dirextalkdomain.ContactRecord,
) (bool, error) {
	return p.service.contactsModule.SaveProjectionIfCurrent(ctx, contact, expected)
}

type serviceProjectorMemberPort struct{ service *Service }

func (p serviceProjectorMemberPort) Lookup(ctx context.Context, roomID, userID string) (dirextalkdomain.MemberRecord, bool, error) {
	return p.service.lookupMember(ctx, roomID, userID)
}

func (p serviceProjectorMemberPort) Save(ctx context.Context, member dirextalkdomain.MemberRecord) error {
	return p.service.saveMember(ctx, member)
}

func (p serviceProjectorMemberPort) SaveProjectionIfAbsent(
	ctx context.Context,
	member dirextalkdomain.MemberRecord,
) (bool, error) {
	return p.service.saveMemberIfAbsent(ctx, member)
}

func (p serviceProjectorMemberPort) SaveProjectionIfCurrent(
	ctx context.Context,
	member,
	expected dirextalkdomain.MemberRecord,
) (bool, error) {
	return p.service.saveMemberIfState(ctx, member, expected.RequestID, expected.Membership)
}

type serviceProjectorDirectRoomPort struct{ service *Service }

func (p serviceProjectorDirectRoomPort) ReinviteAcceptedContact(
	ctx context.Context, contact dirextalkdomain.ContactRecord, identity projectormodule.IdentitySnapshot,
) (projectormodule.ReinviteDisposition, error) {
	if p.service.transport == nil || strings.TrimSpace(contact.RoomID) == "" || strings.TrimSpace(contact.PeerMXID) == "" {
		return projectormodule.ReinviteRetained, nil
	}
	directName := fallbackString(identity.OwnerDisplayName, identity.OwnerMXID)
	err := p.service.transport.InviteUser(ctx, InviteUserRequest{
		RoomID:      contact.RoomID,
		InviterMXID: identity.OwnerMXID,
		InviteeMXID: contact.PeerMXID,
		IsDirect:    true,
		InviteRoomState: []RoomStateEvent{
			roomProfileForDirect(
				directName, identity.OwnerMXID, contact.PeerMXID,
				identity.OwnerDisplayName, identity.OwnerAvatarURL, "", false,
			),
		},
	})
	if err == nil {
		return projectormodule.ReinviteRetained, nil
	}
	if isAlreadyJoinedRoomError(err) || isSenderNotJoinedDirextalkRoom(err) {
		return projectormodule.ReinviteReplacementRequired, nil
	}
	return projectormodule.ReinviteReplacementRequired, err
}

func (p serviceProjectorDirectRoomPort) JoinReplacementRoom(
	ctx context.Context, roomID string, identity projectormodule.IdentitySnapshot,
) (string, error) {
	roomID = strings.TrimSpace(roomID)
	if p.service.transport == nil {
		return roomID, nil
	}
	join, err := p.service.transport.JoinRoom(ctx, JoinRoomRequest{
		RoomIDOrAlias: roomID,
		UserMXID:      identity.OwnerMXID,
		DisplayName:   identity.OwnerDisplayName,
		AvatarURL:     identity.OwnerAvatarURL,
		ServerNames:   retainedRoomServerNames(nil, roomID),
	})
	if err != nil && !isAlreadyJoinedRoomError(err) {
		return "", err
	}
	if joinedRoomID := strings.TrimSpace(join.RoomID); joinedRoomID != "" {
		return joinedRoomID, nil
	}
	return roomID, nil
}
