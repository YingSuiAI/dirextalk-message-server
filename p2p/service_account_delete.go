package p2p

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

const accountDeleteConfirmValue = "delete_account"

type accountDeleteSummary struct {
	ContactsLeft      int
	GroupsLeft        int
	GroupsDissolved   int
	ChannelsLeft      int
	ChannelsDissolved int
	AccountsDeleted   int
}

func (s *Service) deleteAccount(ctx context.Context, params map[string]any) (any, *apiError) {
	if trimString(params["confirm"]) != accountDeleteConfirmValue {
		return nil, badRequest("confirm must be delete_account")
	}
	if !s.beginAccountDeletion() {
		return nil, statusError(http.StatusConflict, "account deletion already in progress")
	}
	success := false
	defer func() {
		if !success {
			s.finishAccountDeletion()
		}
	}()

	summary := accountDeleteSummary{}
	if apiErr := s.leaveAccountContacts(ctx, &summary); apiErr != nil {
		return nil, apiErr
	}
	if apiErr := s.leaveOrDissolveAccountRooms(ctx, &summary); apiErr != nil {
		return nil, apiErr
	}
	if apiErr := s.deactivateAccountUsers(ctx, &summary); apiErr != nil {
		return nil, apiErr
	}
	if err := s.writeAccountDeletedCredentialsFile(); err != nil {
		return nil, internalError(err)
	}

	s.mu.Lock()
	deprovisioner := s.accountDeprovisioner
	s.mu.Unlock()
	if deprovisioner == nil {
		return nil, statusError(http.StatusServiceUnavailable, "account deprovisioner unavailable")
	}
	if err := deprovisioner.DeprovisionAccount(ctx); err != nil {
		return nil, internalError(err)
	}

	s.clearAccountStateInMemory()
	success = true
	return map[string]any{
		"status":               "deprovisioned",
		"contacts_left":        summary.ContactsLeft,
		"groups_left":          summary.GroupsLeft,
		"groups_dissolved":     summary.GroupsDissolved,
		"channels_left":        summary.ChannelsLeft,
		"channels_dissolved":   summary.ChannelsDissolved,
		"accounts_deactivated": summary.AccountsDeleted,
		"database_reset":       true,
	}, nil
}

func (s *Service) beginAccountDeletion() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.accountDeletionInProgress {
		return false
	}
	s.accountDeletionInProgress = true
	return true
}

func (s *Service) finishAccountDeletion() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accountDeletionInProgress = false
}

func (s *Service) leaveAccountContacts(ctx context.Context, summary *accountDeleteSummary) *apiError {
	contacts, err := s.listContacts(ctx)
	if err != nil {
		return internalError(err)
	}
	for _, contact := range contacts {
		if contact.RoomID == "" || contactDeleted(contact.Status) || !contactAccepted(contact.Status) {
			continue
		}
		if apiErr := s.publishAccountDeletedDirectState(ctx, contact); apiErr != nil {
			return apiErr
		}
		if _, apiErr := s.contactMutation(ctx, "contacts.delete", map[string]any{
			"room_id":   contact.RoomID,
			"peer_mxid": contact.PeerMXID,
		}); apiErr != nil {
			return apiErr
		}
		summary.ContactsLeft++
	}
	return nil
}

func (s *Service) publishAccountDeletedDirectState(ctx context.Context, contact contactRecord) *apiError {
	if s.transport == nil || strings.TrimSpace(contact.RoomID) == "" {
		return nil
	}
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	ownerDisplayName := s.profile.DisplayName
	ownerAvatarURL := s.profile.AvatarURL
	s.mu.Unlock()
	if strings.TrimSpace(ownerMXID) == "" {
		return nil
	}
	directName := fallbackString(ownerDisplayName, ownerMXID)
	event := roomProfileForDirect(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark, true)
	event.Content["account_deleted"] = true
	event.Content["deleted_mxid"] = ownerMXID
	if err := s.transport.SendStateEvent(ctx, SendStateEventRequest{
		RoomID:     contact.RoomID,
		SenderMXID: ownerMXID,
		Event:      event,
	}); err != nil {
		return transportWriteError(err)
	}
	return nil
}

func (s *Service) leaveOrDissolveAccountRooms(ctx context.Context, summary *accountDeleteSummary) *apiError {
	s.mu.Lock()
	ownerMXID := s.ownerMXID
	s.mu.Unlock()
	members, err := s.membersForUser(ctx, ownerMXID)
	if err != nil {
		return internalError(err)
	}
	seen := map[string]struct{}{}
	for _, member := range members {
		if member.RoomID == "" || !strings.EqualFold(strings.TrimSpace(member.Membership), "join") {
			continue
		}
		key := member.RoomID + "|" + member.ChannelID
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if member.ChannelID != "" {
			ch, ok, err := s.channelByIDOrRoom(ctx, member.ChannelID, member.RoomID)
			if err != nil {
				return internalError(err)
			}
			if !ok {
				continue
			}
			if productOwnerRole(member.Role) {
				if _, apiErr := s.dissolveChannel(ctx, map[string]any{
					"channel_id": ch.ChannelID,
					"room_id":    ch.RoomID,
				}); apiErr != nil {
					return apiErr
				}
				summary.ChannelsDissolved++
			} else {
				if _, apiErr := s.memberMutation(ctx, "channel", "channels.leave", map[string]any{
					"channel_id": ch.ChannelID,
					"room_id":    ch.RoomID,
				}); apiErr != nil {
					return apiErr
				}
				summary.ChannelsLeft++
			}
			continue
		}
		group, ok, err := s.groupByRoom(ctx, member.RoomID)
		if err != nil {
			return internalError(err)
		}
		if !ok {
			continue
		}
		if productOwnerRole(member.Role) {
			if _, apiErr := s.dissolveGroup(ctx, map[string]any{"room_id": group.RoomID}); apiErr != nil {
				return apiErr
			}
			summary.GroupsDissolved++
		} else {
			if _, apiErr := s.memberMutation(ctx, "group", "groups.leave", map[string]any{"room_id": group.RoomID}); apiErr != nil {
				return apiErr
			}
			summary.GroupsLeft++
		}
	}
	return nil
}

func (s *Service) deactivateAccountUsers(ctx context.Context, summary *accountDeleteSummary) *apiError {
	s.mu.Lock()
	deactivator := s.accountDeactivator
	s.mu.Unlock()
	if deactivator == nil {
		return statusError(http.StatusServiceUnavailable, "account deactivator unavailable")
	}
	for _, localpart := range []string{ownerLocalpart, agentLocalpart} {
		if err := deactivator.DeactivateAccount(ctx, localpart); err != nil {
			return internalError(fmt.Errorf("deactivate %s account: %w", localpart, err))
		}
		summary.AccountsDeleted++
	}
	return nil
}

func (s *Service) clearAccountStateInMemory() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initialized = false
	s.password = ""
	s.accessToken = ""
	s.matrixDeviceID = ""
	s.agentToken = ""
	s.profile = ownerProfile{UserID: s.ownerMXID, Domain: s.serverName}
	s.agentConfig = agentConfig{}
	s.readMarkers = map[string]readMarker{}
	s.channels = map[string]channel{}
	s.posts = nil
	s.comments = nil
	s.contacts = map[string]contactRecord{}
	s.blocks = map[string]blockRecord{}
	s.groups = map[string]groupRecord{}
	s.calls = map[string]callRecord{}
	s.favorites = map[int64]favoriteRecord{}
	s.follows = map[string]followRecord{}
	s.reactions = map[string]reactionRecord{}
	s.members = map[string]memberRecord{}
	s.conversations = map[string]conversationRecord{}
	s.inviteGrants = map[string]channelInviteGrant{}
	s.events = nil
	s.nextEventSeq = 0
	s.realtimeWSTickets = map[string]realtimeWSTicket{}
}
