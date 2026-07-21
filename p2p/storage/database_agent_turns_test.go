package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/YingSuiAI/dirextalk-message-server/internal/sqlutil"
	"github.com/YingSuiAI/dirextalk-message-server/p2p/internal/agentturns"
	"github.com/YingSuiAI/dirextalk-message-server/setup/config"
	"github.com/YingSuiAI/dirextalk-message-server/test"
)

func TestDatabaseAgentTurnsAreOwnerScopedTerminalAndSecretFree(t *testing.T) {
	ctx := context.Background()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer closeDB()
	dbOpts := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
	store, err := NewDatabaseStore(ctx, sqlutil.NewConnectionManager(nil, dbOpts), &dbOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	digest, err := agentturns.RequestDigest("agent.chat.stream", map[string]any{
		"turn_id": "turn", "conversation_id": "conversation", "prompt": "hello",
		"model_profile": map[string]any{"api_key": "request-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := agentturns.Candidate{OwnerID: "owner-a", TurnID: "turn", ConversationID: "conversation", Action: "agent.chat.stream", Digest: digest}
	reservation, err := store.ReserveAgentTurn(ctx, candidate)
	if err != nil || !reservation.Created || reservation.Turn.State != agentturns.StateAccepted {
		t.Fatalf("reserve = (%#v, %v)", reservation, err)
	}
	reservation, err = store.ReserveAgentTurn(ctx, candidate)
	if err != nil || reservation.Created {
		t.Fatalf("reserve replay = (%#v, %v)", reservation, err)
	}
	if _, changed, err := store.MarkAgentTurnRunning(ctx, "owner-a", "turn"); err != nil || !changed {
		t.Fatalf("mark running = (%v, %v)", changed, err)
	}
	if _, err := store.AppendAgentTurnEvent(ctx, "owner-a", "turn", "runtime", "delta", map[string]any{
		"text": "hello", "api_key": "event-secret", "nested": map[string]any{"access_token": "bearer-secret"},
		"function": map[string]any{"arguments": `{"api_key":"json-secret","value":"safe"}`},
		"output":   "Authorization: Bearer raw-bearer-secret",
		"tool_calls": []map[string]any{{
			"arguments": map[string]string{"api_key": "typed-api-secret", "safe": "ok"},
			"trace":     []map[string]string{{"credential": "typed-credential-secret", "text": "Bearer typed-bearer-secret"}},
		}},
		"assistant_text": "Bearer leaf-bearer-secret",
	}); err != nil {
		t.Fatal(err)
	}
	turn, terminal, changed, err := store.FinishAgentTurn(ctx, "owner-a", "turn", agentturns.StateSucceeded, "runtime", "done", map[string]any{"text": "hello"}, "")
	if err != nil || !changed || turn.State != agentturns.StateSucceeded || terminal.Seq != 2 {
		t.Fatalf("finish = (%#v, %#v, %v, %v)", turn, terminal, changed, err)
	}
	turn, _, changed, err = store.StopAgentTurn(ctx, "owner-a", "turn")
	if err != nil || changed || turn.State != agentturns.StateSucceeded {
		t.Fatalf("terminal stop = (%#v, %v, %v)", turn, changed, err)
	}

	other := candidate
	other.OwnerID = "owner-b"
	reservation, err = store.ReserveAgentTurn(ctx, other)
	if err != nil || !reservation.Created {
		t.Fatalf("owner-isolated reserve = (%#v, %v)", reservation, err)
	}
	count, err := store.InterruptAgentTurns(ctx)
	if err != nil || count != 1 {
		t.Fatalf("interrupt = (%d, %v)", count, err)
	}
	turn, ok, err := store.GetAgentTurn(ctx, "owner-b", "turn")
	if err != nil || !ok || turn.State != agentturns.StateInterrupted {
		t.Fatalf("interrupted turn = (%#v, %v, %v)", turn, ok, err)
	}

	var storedEvents string
	if err := store.DB().QueryRowContext(ctx, `
		SELECT COALESCE(string_agg(data_json::text, '' ORDER BY seq), '')
		FROM p2p_native_agent_turn_events WHERE owner_id = $1 AND turn_id = $2
	`, "owner-a", "turn").Scan(&storedEvents); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{
		"event-secret", "bearer-secret", "request-secret", "json-secret", "raw-bearer-secret",
		"typed-api-secret", "typed-credential-secret", "typed-bearer-secret", "leaf-bearer-secret",
	} {
		if strings.Contains(storedEvents, secret) {
			t.Fatalf("stored turn events contain secret %q: %s", secret, storedEvents)
		}
	}
}
