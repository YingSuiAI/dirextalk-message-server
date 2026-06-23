package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/YingSuiAI/direxio-message-server/internal/agentclient"
)

const rootHelp = `direxio - CLI for Direxio P2P and Matrix agent workflows

Credentials:
  DIREXIO_DOMAIN       Site origin, for example https://example.com
  DIREXIO_AGENT_TOKEN  Portal Agent token

Commands:
  auth status
  init
  p2p action <action> --params '{}'
  p2p apis
  p2p sync-bootstrap
  contacts list
  channels list
  channels public-search
  groups list
  matrix session init
  matrix messages send --room-id ROOM --text TEXT
  matrix messages list --room-id ROOM --limit 50
  matrix sync --timeout 30s
  matrix listen

Use "direxio <domain> help" for subcommand examples.
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || isHelp(args[0]) {
		_, _ = fmt.Fprint(stdout, rootHelp)
		return 0
	}
	switch args[0] {
	case "auth", "init", "p2p", "contacts", "channels", "groups", "matrix":
		return runKnown(args, stdout, stderr)
	default:
		agentclient.WriteError(stderr, "direxio: unknown command "+args[0])
		return 2
	}
}

func runKnown(args []string, stdout, stderr io.Writer) int {
	if len(args) > 1 && isHelp(args[1]) {
		_, _ = fmt.Fprint(stdout, helpFor(args[0]))
		return 0
	}
	raw := hasRawFlag(args)
	args = stripRawFlag(args)

	cfg, err := agentclient.ConfigFromEnv()
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 2
	}
	client := agentclient.New(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if args[0] == "matrix" && len(args) >= 2 && args[1] == "listen" {
		return runMatrixListen(ctx, client, args[2:], stdout, stderr)
	}

	var result map[string]any
	switch args[0] {
	case "auth":
		result, err = runAuth(ctx, client, args[1:])
	case "init":
		result, err = client.CallP2PAction(ctx, "portal.status", nil, agentclient.P2PQuery)
	case "p2p":
		result, err = runP2P(ctx, client, args[1:])
	case "contacts":
		result, err = runSimpleDomain(ctx, client, args[1:], "contacts.list", agentclient.P2PQuery)
	case "channels":
		result, err = runChannels(ctx, client, args[1:])
	case "groups":
		result, err = runSimpleDomain(ctx, client, args[1:], "groups.list", agentclient.P2PQuery)
	case "matrix":
		result, err = runMatrix(ctx, client, args[1:])
	}
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	if err := agentclient.WriteJSON(stdout, result, raw); err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	return 0
}

func runAuth(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "status" {
		return client.CallP2PAction(ctx, "portal.status", nil, agentclient.P2PQuery)
	}
	return nil, fmt.Errorf("usage: direxio auth status")
}

func runP2P(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "apis" {
		return client.CallP2PAction(ctx, "apis.list", nil, agentclient.P2PQuery)
	}
	if len(args) == 1 && args[0] == "sync-bootstrap" {
		return client.CallP2PAction(ctx, "sync.bootstrap", nil, agentclient.P2PQuery)
	}
	if len(args) >= 2 && args[0] == "action" {
		fs := flag.NewFlagSet("p2p action", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		paramsJSON := fs.String("params", "{}", "JSON params")
		if err := fs.Parse(args[2:]); err != nil {
			return nil, err
		}
		var params map[string]any
		if err := json.Unmarshal([]byte(*paramsJSON), &params); err != nil {
			return nil, fmt.Errorf("invalid params json: %w", err)
		}
		return client.CallP2PAction(ctx, args[1], params, agentclient.P2PCommand)
	}
	return nil, fmt.Errorf("usage: direxio p2p action <action> --params '{}'")
}

func runSimpleDomain(ctx context.Context, client *agentclient.Client, args []string, action string, route agentclient.P2PRoute) (map[string]any, error) {
	if len(args) == 1 && args[0] == "list" {
		return client.CallP2PAction(ctx, action, nil, route)
	}
	return nil, fmt.Errorf("unsupported command")
}

func runChannels(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) == 1 && args[0] == "list" {
		return client.CallP2PAction(ctx, "channels.list", nil, agentclient.P2PQuery)
	}
	if len(args) == 1 && args[0] == "public-search" {
		return client.CallP2PAction(ctx, "channels.public.search", nil, agentclient.P2PQuery)
	}
	return nil, fmt.Errorf("unsupported channels command")
}

func runMatrix(ctx context.Context, client *agentclient.Client, args []string) (map[string]any, error) {
	if len(args) >= 2 && args[0] == "session" && args[1] == "init" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"device_id":  session.DeviceID,
			"user_id":    session.UserID,
			"homeserver": session.Homeserver,
			"status":     "ok",
		}, nil
	}
	if len(args) >= 2 && args[0] == "messages" {
		switch args[1] {
		case "send":
			fs := flag.NewFlagSet("matrix messages send", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			roomID := fs.String("room-id", "", "Matrix room id")
			text := fs.String("text", "", "message text")
			if err := fs.Parse(args[2:]); err != nil {
				return nil, err
			}
			if strings.TrimSpace(*roomID) == "" {
				return nil, fmt.Errorf("room-id is required")
			}
			if strings.TrimSpace(*text) == "" {
				return nil, fmt.Errorf("text is required")
			}
			session, err := client.CreateMatrixSession(ctx)
			if err != nil {
				return nil, err
			}
			return client.SendTextMessage(ctx, session, *roomID, *text)
		case "list":
			fs := flag.NewFlagSet("matrix messages list", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			roomID := fs.String("room-id", "", "Matrix room id")
			limit := fs.Int("limit", 50, "message limit")
			if err := fs.Parse(args[2:]); err != nil {
				return nil, err
			}
			if strings.TrimSpace(*roomID) == "" {
				return nil, fmt.Errorf("room-id is required")
			}
			session, err := client.CreateMatrixSession(ctx)
			if err != nil {
				return nil, err
			}
			return client.RoomMessages(ctx, session, *roomID, *limit)
		}
	}
	if len(args) >= 1 && args[0] == "sync" {
		timeoutMS, since, err := parseSyncFlags("matrix sync", args[1:])
		if err != nil {
			return nil, err
		}
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		return client.Sync(ctx, session, timeoutMS, since)
	}
	if len(args) >= 1 && args[0] == "listen" {
		session, err := client.CreateMatrixSession(ctx)
		if err != nil {
			return nil, err
		}
		return client.Sync(ctx, session, 30000, "")
	}
	return nil, fmt.Errorf("unsupported matrix command")
}

func runMatrixListen(ctx context.Context, client *agentclient.Client, args []string, stdout, stderr io.Writer) int {
	timeoutMS, since, err := parseSyncFlags("matrix listen", args)
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 2
	}
	session, err := client.CreateMatrixSession(ctx)
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	sync, err := client.Sync(ctx, session, timeoutMS, since)
	if err != nil {
		agentclient.WriteError(stderr, "direxio: "+err.Error())
		return 1
	}
	for _, event := range agentclient.ExtractSyncTimelineEvents(sync) {
		if err := agentclient.WriteNDJSON(stdout, event); err != nil {
			agentclient.WriteError(stderr, "direxio: "+err.Error())
			return 1
		}
	}
	return 0
}

func parseSyncFlags(name string, args []string) (int, string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	timeout := fs.Duration("timeout", 30*time.Second, "sync timeout duration")
	timeoutMS := fs.Int("timeout-ms", 0, "sync timeout in milliseconds")
	since := fs.String("since", "", "sync token")
	if err := fs.Parse(args); err != nil {
		return 0, "", err
	}
	if *timeoutMS > 0 {
		return *timeoutMS, strings.TrimSpace(*since), nil
	}
	return int(timeout.Milliseconds()), strings.TrimSpace(*since), nil
}

func helpFor(group string) string {
	switch group {
	case "channels":
		return `direxio channels - channel workflows

Examples:
  direxio channels list
  direxio channels public-search

Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
	case "p2p":
		return `direxio p2p - raw P2P action fallback

Examples:
  direxio p2p apis
  direxio p2p sync-bootstrap
  direxio p2p action channels.list --params "{}"

Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
	case "contacts":
		return "direxio contacts list\n\nRequires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.\n"
	case "groups":
		return "direxio groups list\n\nRequires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.\n"
	case "auth":
		return "direxio auth status\n\nChecks portal status through the configured Agent token.\n"
	case "matrix":
		return `direxio matrix - Matrix Client-Server workflows

Examples:
  direxio matrix session init
  direxio matrix messages send --room-id "!room:example.com" --text "hello"
  direxio matrix messages list --room-id "!room:example.com" --limit 50
  direxio matrix sync --timeout 30s
  direxio matrix sync --timeout-ms 30000
  direxio matrix listen

The Matrix access token is obtained internally with the Agent token and is not printed.
Requires DIREXIO_DOMAIN and DIREXIO_AGENT_TOKEN.
`
	default:
		return rootHelp
	}
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "--help" || arg == "-h"
}

func hasRawFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--raw" {
			return true
		}
	}
	return false
}

func stripRawFlag(args []string) []string {
	out := args[:0]
	for _, arg := range args {
		if arg != "--raw" {
			out = append(out, arg)
		}
	}
	return out
}
