package legacygateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var ErrNoTerminal = errors.New("legacy gateway terminal source has no committed fact")

type RunTerminalFact struct {
	MatrixRoomID      string
	RequestID         string
	RunID             string
	Cursor            string
	Kind              TerminalKind
	ConnectorID       string
	Outcome           string
	ErrorCode         string
	ResultReference   json.RawMessage
	EvidenceReference json.RawMessage
	Digest            [32]byte
}

type RunTerminalSource interface {
	Next(context.Context) (RunTerminalFact, error)
	ACK(context.Context, string) error
}

type MatrixTerminalEvent struct {
	RoomID        string
	EventType     string
	ContentJSON   []byte
	TransactionID string
	EventID       string
}

// MatrixSender must make TransactionID/EventID replay idempotent. A retry with
// the same values and bytes must return the same event ID without a second
// timeline event.
type MatrixSender interface {
	SendTerminal(context.Context, MatrixTerminalEvent) (string, error)
}

type TerminalConsumer struct {
	store  Store
	source RunTerminalSource
	sender MatrixSender
	now    func() time.Time
}

func NewTerminalConsumer(store Store, source RunTerminalSource, sender MatrixSender) (*TerminalConsumer, error) {
	if store == nil || source == nil || sender == nil {
		return nil, errors.New("legacy gateway terminal consumer is disabled without store, source, and Matrix sender")
	}
	return &TerminalConsumer{store: store, source: source, sender: sender, now: time.Now}, nil
}

func (consumer *TerminalConsumer) ProcessOnce(ctx context.Context) error {
	pending, err := consumer.store.PendingTerminals(ctx, 1)
	if err != nil {
		return err
	}
	if len(pending) != 0 {
		return consumer.deliver(ctx, pending[0])
	}
	fact, err := consumer.source.Next(ctx)
	if err != nil {
		return err
	}
	delivery, err := consumer.prepare(ctx, fact)
	if err != nil {
		return err
	}
	reservation, err := consumer.store.ReserveTerminal(ctx, delivery, consumer.now())
	if err != nil {
		return err
	}
	return consumer.deliver(ctx, reservation.Delivery)
}

func (consumer *TerminalConsumer) prepare(ctx context.Context, fact RunTerminalFact) (TerminalDelivery, error) {
	if DigestRunTerminalFact(fact) != fact.Digest {
		return TerminalDelivery{}, errors.New("legacy gateway terminal digest mismatch")
	}
	record, err := consumer.store.LoadAcceptedInvocation(ctx, fact.MatrixRoomID, fact.RequestID, fact.RunID)
	if err != nil {
		return TerminalDelivery{}, err
	}
	var eventType string
	var content []byte
	switch fact.Kind {
	case TerminalResult:
		if strings.TrimSpace(fact.ConnectorID) == "" || strings.TrimSpace(fact.Outcome) == "" ||
			!validJSONReference(fact.ResultReference) || !validJSONReference(fact.EvidenceReference) {
			return TerminalDelivery{}, errors.New("legacy gateway result terminal is invalid")
		}
		eventType = ResultEventType
		content, err = json.Marshal(ResultEventContent{
			RequestID: fact.RequestID, RunID: fact.RunID, InstallationID: record.InstallationID,
			ConnectorID: fact.ConnectorID, Outcome: fact.Outcome, ReplyToEventID: record.MatrixInputEventID,
			ResultReference: fact.ResultReference, EvidenceReference: fact.EvidenceReference,
		})
	case TerminalError:
		if strings.TrimSpace(fact.ErrorCode) == "" {
			return TerminalDelivery{}, errors.New("legacy gateway error terminal is invalid")
		}
		eventType = ErrorEventType
		content, err = json.Marshal(ErrorEventContent{
			RequestID: fact.RequestID, InstallationID: record.InstallationID,
			Code: fact.ErrorCode, ReplyToEventID: record.MatrixInputEventID,
		})
	default:
		return TerminalDelivery{}, errors.New("legacy gateway terminal kind is invalid")
	}
	if err != nil {
		return TerminalDelivery{}, err
	}
	digest := terminalEventDigest(fact.MatrixRoomID, fact.RequestID, fact.RunID, fact.Cursor, eventType, content)
	encoded := base64.RawURLEncoding.EncodeToString(digest[:])
	return TerminalDelivery{
		MatrixRoomID: fact.MatrixRoomID, RequestID: fact.RequestID, RunID: fact.RunID,
		Cursor: fact.Cursor, Kind: fact.Kind, Digest: digest, EventType: eventType, ContentJSON: content,
		MatrixTransactionID: "dtx-agent-terminal-" + hex.EncodeToString(digest[:]),
		MatrixEventID:       "$dtx-agent-terminal-" + encoded, Phase: TerminalSendIntent,
	}, nil
}

func (consumer *TerminalConsumer) deliver(ctx context.Context, delivery TerminalDelivery) error {
	if delivery.Phase == TerminalSourceACK {
		return consumer.source.ACK(ctx, delivery.Cursor)
	}
	if delivery.Phase == TerminalSendIntent {
		eventID, err := consumer.sender.SendTerminal(ctx, MatrixTerminalEvent{
			RoomID: delivery.MatrixRoomID, EventType: delivery.EventType,
			ContentJSON: bytes.Clone(delivery.ContentJSON), TransactionID: delivery.MatrixTransactionID,
			EventID: delivery.MatrixEventID,
		})
		if err != nil {
			return err
		}
		if eventID != delivery.MatrixEventID {
			return errors.New("legacy gateway Matrix sender returned a mismatched event ID")
		}
		delivery, err = consumer.store.AdvanceTerminal(ctx, delivery.MatrixRoomID, delivery.RequestID,
			delivery.Digest, TerminalSendIntent, TerminalSent, consumer.now())
		if err != nil {
			return err
		}
	}
	if delivery.Phase == TerminalSent {
		var err error
		delivery, err = consumer.store.AdvanceTerminal(ctx, delivery.MatrixRoomID, delivery.RequestID,
			delivery.Digest, TerminalSent, TerminalCommitted, consumer.now())
		if err != nil {
			return err
		}
	}
	if delivery.Phase == TerminalCommitted {
		if err := consumer.source.ACK(ctx, delivery.Cursor); err != nil {
			return err
		}
		_, err := consumer.store.AdvanceTerminal(ctx, delivery.MatrixRoomID, delivery.RequestID,
			delivery.Digest, TerminalCommitted, TerminalSourceACK, consumer.now())
		return err
	}
	return nil
}

func DigestRunTerminalFact(fact RunTerminalFact) [32]byte {
	parts := [][]byte{
		[]byte(fact.MatrixRoomID), []byte(fact.RequestID), []byte(fact.RunID), []byte(fact.Cursor),
		[]byte(fact.Kind), []byte(fact.ConnectorID), []byte(fact.Outcome), []byte(fact.ErrorCode),
		fact.ResultReference, fact.EvidenceReference,
	}
	return digestParts("dirextalk.legacy-run-terminal.v1\x00", parts...)
}

func terminalEventDigest(roomID, requestID, runID, cursor, eventType string, content []byte) [32]byte {
	return digestParts("dirextalk.legacy-matrix-terminal.v1\x00", []byte(roomID), []byte(requestID),
		[]byte(runID), []byte(cursor), []byte(eventType), content)
}

func digestParts(domain string, parts ...[]byte) [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	for _, part := range parts {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write(part)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func validJSONReference(value json.RawMessage) bool {
	return len(value) != 0 && len(value) <= MaxAgentGatewayMessageBytes && json.Valid(value)
}
