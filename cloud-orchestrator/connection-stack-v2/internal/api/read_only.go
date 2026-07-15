package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/contract"
	commandstore "github.com/YingSuiAI/dirextalk-message-server/cloud-orchestrator/connection-stack-v2/internal/store"
)

type GatewayRuntime struct {
	DomainName string
	Stage      string
}

type gatewayRuntimeContextKey struct{}

func WithGatewayRuntime(ctx context.Context, runtime GatewayRuntime) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, gatewayRuntimeContextKey{}, runtime)
}

func GatewayRuntimeFromContext(ctx context.Context) (GatewayRuntime, bool) {
	if ctx == nil {
		return GatewayRuntime{}, false
	}
	runtime, ok := ctx.Value(gatewayRuntimeContextKey{}).(GatewayRuntime)
	return runtime, ok && runtime.DomainName != "" && runtime.Stage != ""
}

type RegistrationAttestor interface {
	Attest(ctx context.Context, runtime GatewayRuntime, command contract.Command, request contract.RegistrationRequest) (contract.Registration, error)
}

type QuoteProvider interface {
	Quote(ctx context.Context, command contract.Command, request contract.QuoteRequest, now time.Time) (contract.Quote, error)
}

type Error struct {
	Code       string
	StatusCode int
}

func (e *Error) Error() string {
	if e == nil || e.Code == "" {
		return "connection stack provider error"
	}
	return "connection stack provider error: " + e.Code
}

func NewError(code string, status int) error { return &Error{Code: code, StatusCode: status} }

func (b Broker) executeReadOnly(response http.ResponseWriter, request *http.Request, command contract.Command, now time.Time) {
	requestSHA, err := command.RequestSHA256()
	if err != nil {
		writeError(response, http.StatusNotImplemented, contract.Code(err))
		return
	}
	identity := commandstore.Record{
		ConnectionID: command.ConnectionID, CommandID: command.CommandID, RequestSHA256: requestSHA,
		ExpectedGeneration: command.ExpectedGeneration, NodeCounter: command.NodeCounter, Action: command.Action,
	}
	existing, found, err := b.Store.Lookup(request.Context(), command.ConnectionID, command.CommandID)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if found {
		b.writeReplay(response, command, identity, existing)
		return
	}
	if err := command.ValidateAt(now); err != nil {
		status := http.StatusBadRequest
		if contract.Code(err) == "expired_command" {
			status = http.StatusUnauthorized
		}
		writeError(response, status, contract.Code(err))
		return
	}

	var resultJSON []byte
	var issuedQuote *commandstore.IssuedQuote
	switch command.Action {
	case contract.ActionRegistrationVerify:
		if b.Registration == nil {
			writeError(response, http.StatusServiceUnavailable, "registration_provider_unavailable")
			return
		}
		runtime, ok := GatewayRuntimeFromContext(request.Context())
		if !ok {
			writeError(response, http.StatusInternalServerError, "registration_config_invalid")
			return
		}
		registrationRequest, parseErr := command.RegistrationRequest()
		if parseErr != nil {
			writeError(response, http.StatusBadRequest, contract.Code(parseErr))
			return
		}
		registration, providerErr := b.Registration.Attest(request.Context(), runtime, command, registrationRequest)
		if providerErr != nil {
			writeProviderError(response, providerErr)
			return
		}
		resultJSON, err = contract.MarshalCommittedRegistrationResult(command, registration)
	case contract.ActionQuoteRequest:
		if b.Quote == nil {
			writeError(response, http.StatusServiceUnavailable, "quote_provider_unavailable")
			return
		}
		quoteRequest, parseErr := command.QuoteRequest()
		if parseErr != nil {
			writeError(response, http.StatusBadRequest, contract.Code(parseErr))
			return
		}
		quote, providerErr := b.Quote.Quote(request.Context(), command, quoteRequest, now)
		if providerErr != nil {
			writeProviderError(response, providerErr)
			return
		}
		resultJSON, err = contract.MarshalCommittedQuoteResult(command, quote)
		if err == nil {
			quoteJSON, marshalErr := json.Marshal(quote)
			if marshalErr != nil {
				err = marshalErr
			} else {
				issuedQuote = &commandstore.IssuedQuote{
					ConnectionID: command.ConnectionID, QuoteID: quote.QuoteID, PlanDigest: quote.PlanDigest,
					CommandID: command.CommandID, RequestSHA256: requestSHA, ValidUntil: quote.ValidUntil, QuoteJSON: quoteJSON,
				}
			}
		}
	default:
		writeError(response, http.StatusNotImplemented, "operation_not_enabled")
		return
	}
	if err != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	identity.ResultJSON = append([]byte(nil), resultJSON...)
	stored, created, err := b.Store.Commit(request.Context(), identity, issuedQuote)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	if !created {
		b.writeReplay(response, command, identity, stored)
		return
	}
	if !bytes.Equal(stored.ResultJSON, resultJSON) || contract.ValidateCommittedResult(command, stored.ResultJSON) != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, stored.ResultJSON)
}

func (b Broker) writeReplay(response http.ResponseWriter, command contract.Command, identity, stored commandstore.Record) {
	if !stored.SameIdentity(identity) {
		writeError(response, http.StatusConflict, "command_id_conflict")
		return
	}
	replay, err := contract.IdempotentResult(command, stored.ResultJSON)
	if err != nil {
		writeError(response, http.StatusInternalServerError, "receipt_store_invalid")
		return
	}
	writeRawJSON(response, http.StatusOK, replay)
}

func writeRawJSON(response http.ResponseWriter, status int, raw []byte) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_, _ = response.Write(append(append([]byte(nil), raw...), '\n'))
}

func writeProviderError(response http.ResponseWriter, err error) {
	var providerError *Error
	if errors.As(err, &providerError) && providerError.Code != "" && providerError.StatusCode >= 400 && providerError.StatusCode <= 599 {
		writeError(response, providerError.StatusCode, providerError.Code)
		return
	}
	writeError(response, http.StatusServiceUnavailable, "provider_unavailable")
}

func writeStoreError(response http.ResponseWriter, err error) {
	code := commandstore.Code(err)
	status := http.StatusServiceUnavailable
	if code == "command_id_conflict" || code == "stale_node_counter" || code == "deployment_id_conflict" || code == "approval_already_consumed" || code == "challenge_already_consumed" || code == "deployment_reservation_conflict" {
		status = http.StatusConflict
	}
	writeError(response, status, code)
}
