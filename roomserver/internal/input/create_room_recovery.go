// Copyright 2026 YingSuiAI
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/YingSuiAI/dirextalk-message-server/roomserver/api"
	"github.com/YingSuiAI/dirextalk-message-server/setup/jetstream"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const (
	roomInputQueueLockCount         = 256
	roomInputControlHeader          = "roomserver_control"
	roomInputControlPurgeCreateOnly = "purge_create_only"
)

type roomInputControlResponse struct {
	Purged bool   `json:"purged,omitempty"`
	Error  string `json:"error,omitempty"`
}

func (w *worker) processPurgeCreateOnlyRoom(msg *nats.Msg) {
	response := roomInputControlResponse{}
	var request api.PerformAdminPurgeCreateOnlyRoomRequest
	if err := json.Unmarshal(msg.Data, &request); err != nil {
		response.Error = fmt.Sprintf("decode create-only purge request: %v", err)
	} else {
		purged, err := w.r.purgeCreateOnlyRoom(w.r.ProcessContext.Context(), &request)
		response.Purged = purged
		if err != nil {
			response.Error = err.Error()
		}
	}
	_ = msg.AckSync()
	if replyTo := msg.Header.Get("sync"); replyTo != "" {
		data, err := json.Marshal(response)
		if err == nil {
			err = w.r.NATSClient.Publish(replyTo, data)
		}
		if err != nil {
			logrus.WithError(err).WithField("room_id", w.roomID).Warn("Roomserver failed to respond for create-only purge")
		}
	}
}

func (r *Inputer) purgeCreateOnlyRoom(ctx context.Context, request *api.PerformAdminPurgeCreateOnlyRoomRequest) (bool, error) {
	roomID, creatorID, err := validateCreateOnlyPurgeRequest(request)
	if err != nil {
		return false, err
	}
	if r.Queryer == nil {
		return false, errors.New("roomserver query API is unavailable")
	}
	var latest api.QueryLatestEventsAndStateResponse
	if err = r.Queryer.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID.String(),
	}, &latest); err != nil {
		return false, err
	}
	if !latest.RoomExists || latest.RoomVersion == "" || len(latest.LatestEvents) != 1 || len(latest.StateEvents) != 1 {
		return false, nil
	}
	createEvent := latest.StateEvents[0]
	if createEvent == nil || createEvent.Type() != spec.MRoomCreate || createEvent.StateKey() == nil || *createEvent.StateKey() != "" {
		return false, nil
	}
	if createEvent.RoomID() != *roomID || createEvent.EventID() != latest.LatestEvents[0] || createEvent.Depth() != 1 {
		return false, nil
	}
	if len(createEvent.PrevEventIDs()) != 0 || len(createEvent.AuthEventIDs()) != 0 {
		return false, nil
	}
	if string(createEvent.SenderID()) != creatorID.String() {
		creator, queryErr := r.Queryer.QueryUserIDForSender(ctx, *roomID, createEvent.SenderID())
		if queryErr != nil {
			return false, queryErr
		}
		if creator == nil || *creator != *creatorID {
			return false, nil
		}
	}
	var content map[string]any
	if err = json.Unmarshal(createEvent.Content(), &content); err != nil {
		return false, nil
	}
	marker, ok := content[request.CreateEventContentKey].(string)
	if !ok || marker != request.CreateEventContentValue {
		return false, nil
	}

	purger, ok := r.DB.(interface {
		PurgeRoom(context.Context, string) error
	})
	if !ok {
		return false, errors.New("roomserver database does not support room purge")
	}
	if err = purger.PurgeRoom(ctx, roomID.String()); err != nil {
		return false, err
	}
	if r.OutputProducer == nil {
		return true, errors.New("roomserver output producer is unavailable after create-only purge")
	}
	if err = r.OutputProducer.ProduceRoomEvents(roomID.String(), []api.OutputEvent{{
		Type: api.OutputTypePurgeRoom,
		PurgeRoom: &api.OutputPurgeRoom{
			RoomID: roomID.String(),
		},
	}}); err != nil {
		return true, err
	}
	return true, nil
}

func validateCreateOnlyPurgeRequest(request *api.PerformAdminPurgeCreateOnlyRoomRequest) (*spec.RoomID, *spec.UserID, error) {
	if request == nil {
		return nil, nil, errors.New("create-only purge request is required")
	}
	roomID, err := spec.NewRoomID(strings.TrimSpace(request.RoomID))
	if err != nil {
		return nil, nil, err
	}
	creatorID, err := spec.NewUserID(strings.TrimSpace(request.CreatorMXID), true)
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(request.CreateEventContentKey) == "" || strings.TrimSpace(request.CreateEventContentValue) == "" {
		return nil, nil, errors.New("create event ownership marker is required")
	}
	return roomID, creatorID, nil
}

// PerformAdminPurgeRoomIfCreateOnly enqueues the condition and purge on the
// room's input stream. The control message cannot interleave with a published
// input batch and is processed after every earlier event for the room.
func (r *Inputer) PerformAdminPurgeRoomIfCreateOnly(ctx context.Context, request *api.PerformAdminPurgeCreateOnlyRoomRequest) (bool, error) {
	roomID, _, err := validateCreateOnlyPurgeRequest(request)
	if err != nil {
		return false, err
	}
	replyTo := nats.NewInbox()
	replySub, err := r.NATSClient.SubscribeSync(replyTo)
	if err != nil {
		return false, fmt.Errorf("r.NATSClient.SubscribeSync: %w", err)
	}
	defer replySub.Drain() // nolint:errcheck

	data, err := json.Marshal(request)
	if err != nil {
		return false, err
	}
	subject := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(roomID.String()))
	msg := &nats.Msg{Subject: subject, Header: nats.Header{}, Data: data}
	msg.Header.Set(jetstream.RoomID, roomID.String())
	msg.Header.Set("sync", replyTo)
	msg.Header.Set(roomInputControlHeader, roomInputControlPurgeCreateOnly)

	unlock := r.lockInputRoomBatches([]string{roomID.String()})
	_, err = r.JetStream.PublishMsg(msg, nats.Context(ctx))
	unlock()
	if err != nil {
		return false, fmt.Errorf("r.JetStream.PublishMsg: %w", err)
	}
	roomserverInputBackpressure.With(prometheus.Labels{"room_id": roomID.String()}).Inc()

	reply, err := replySub.NextMsgWithContext(ctx)
	if err != nil {
		return false, err
	}
	var response roomInputControlResponse
	if err = json.Unmarshal(reply.Data, &response); err != nil {
		return false, fmt.Errorf("decode create-only purge response: %w", err)
	}
	if response.Error != "" {
		return response.Purged, errors.New(response.Error)
	}
	return response.Purged, nil
}

func (r *Inputer) lockInputRoomBatches(roomIDs []string) func() {
	indices := make([]int, 0, len(roomIDs))
	seen := make(map[int]struct{}, len(roomIDs))
	for _, roomID := range roomIDs {
		index := roomInputQueueLockIndex(roomID)
		if _, ok := seen[index]; ok {
			continue
		}
		seen[index] = struct{}{}
		indices = append(indices, index)
	}
	sort.Ints(indices)
	for _, index := range indices {
		r.inputQueueLocks[index].Lock()
	}
	return func() {
		for index := len(indices) - 1; index >= 0; index-- {
			r.inputQueueLocks[indices[index]].Unlock()
		}
	}
}

func roomInputQueueLockIndex(roomID string) int {
	var hash uint32 = 2166136261
	for index := 0; index < len(roomID); index++ {
		hash ^= uint32(roomID[index])
		hash *= 16777619
	}
	return int(hash % roomInputQueueLockCount)
}
