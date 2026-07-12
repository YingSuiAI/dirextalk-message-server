package p2p

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/internal/releasecontrol"
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
	if apiErr := s.setAccountDesiredStateDeprovisioned(ctx); apiErr != nil {
		return nil, apiErr
	}
	result, apiErr := s.deleteAccountAfterDesiredState(ctx)
	if apiErr != nil {
		if restoreErr := s.restoreAccountDesiredStateRunning(); restoreErr != nil {
			return nil, restoreErr
		}
		return nil, apiErr
	}
	success = true
	return result, nil
}

func (s *Service) deleteAccountAfterDesiredState(ctx context.Context) (any, *apiError) {
	summary := accountDeleteSummary{}
	if apiErr := s.leaveAccountContacts(ctx, &summary); apiErr != nil {
		return nil, apiErr
	}
	if apiErr := s.leaveOrDissolveAccountRooms(ctx, &summary); apiErr != nil {
		return nil, apiErr
	}

	// Matrix leave/dissolve writes must complete before taking the reset barrier,
	// because their roomserver output is projected asynchronously. From this
	// point through database reset and the terminal state flip, no ProductCore,
	// MCP, or projector operation may remain in flight.
	s.accountOperationMu.Lock()
	defer s.accountOperationMu.Unlock()

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

	s.clearAccountStateAfterDeprovision()
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

func (s *Service) setAccountDesiredStateDeprovisioned(ctx context.Context) *apiError {
	return s.setAccountDesiredState(ctx, releasecontrol.DesiredStateDeprovisioned)
}

func (s *Service) setAccountDesiredState(ctx context.Context, state releasecontrol.DesiredState) *apiError {
	s.mu.Lock()
	controller := s.releaseController
	s.mu.Unlock()
	if controller == nil {
		return codedError(http.StatusServiceUnavailable, updaterUnavailableCode, "updater is unavailable")
	}
	if err := controller.SetDesiredState(ctx, state); err != nil {
		return releaseControllerAPIError(err)
	}
	return nil
}

func (s *Service) restoreAccountDesiredStateRunning() *apiError {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if apiErr := s.setAccountDesiredState(ctx, releasecontrol.DesiredStateRunning); apiErr != nil {
		return codedError(http.StatusServiceUnavailable, "account_delete_watchdog_restore_failed", "account deletion failed and watchdog could not be restored")
	}
	return nil
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
	event := accountDeletedDirectProfile(directName, ownerMXID, contact.PeerMXID, ownerDisplayName, ownerAvatarURL, contact.Remark)
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
	s.accountOperationMu.Lock()
	defer s.accountOperationMu.Unlock()
	s.clearAccountStateAfterDeprovision()
}

func (s *Service) clearAccountStateAfterDeprovision() {
	s.mu.Lock()
	s.accountDeprovisioned = true
	s.initialized = false
	s.password = ""
	s.accessToken = ""
	s.matrixDeviceID = ""
	s.portalSessionGeneration++
	s.agentToken = ""
	s.profile = ownerProfile{UserID: s.ownerMXID, Domain: s.serverName}
	s.agentConfig = agentConfig{}
	s.clientBuild = clientBuild{}
	s.readMarkers = map[string]readMarker{}
	s.channels = map[string]channel{}
	s.posts = nil
	s.comments = nil
	s.groups = map[string]groupRecord{}
	s.reactions = map[string]reactionRecord{}
	s.members = map[string]memberRecord{}
	s.inviteGrants = map[string]channelInviteGrant{}
	s.nextEventSeq = 0
	s.realtimeWSTickets = map[string]realtimeWSTicket{}
	resetter, _ := s.store.(interface{ ResetAccountState() })
	s.mu.Unlock()
	if resetter != nil {
		resetter.ResetAccountState()
	}
}
