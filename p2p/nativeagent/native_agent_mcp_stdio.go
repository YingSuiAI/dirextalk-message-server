package nativeagent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func (r *Runtime) mcpStdioListTools(ctx context.Context, server map[string]any) ([]any, error) {
	result, err := r.withMCPStdio(ctx, server, func(client *mcpStdioClient) (any, error) {
		return client.request("tools/list", map[string]any{})
	})
	if err != nil {
		return nil, err
	}
	return mcpToolsFromResult(result), nil
}

func (r *Runtime) mcpStdioCallTool(ctx context.Context, server map[string]any, toolName string, args map[string]any) (any, error) {
	return r.withMCPStdio(ctx, server, func(client *mcpStdioClient) (any, error) {
		return client.request("tools/call", map[string]any{"name": toolName, "arguments": args})
	})
}

func (r *Runtime) withMCPStdio(ctx context.Context, server map[string]any, fn func(*mcpStdioClient) (any, error)) (any, error) {
	command := trimString(server["command"])
	if command == "" {
		return nil, fmt.Errorf("mcp stdio command is required")
	}
	runCtx, cancel := context.WithTimeout(ctx, durationSeconds(server["timeout_seconds"], 30))
	defer cancel()
	cmd := exec.CommandContext(runCtx, command, stringSliceParam(server["args"])...)
	cmd.Dir = filepath.Join(r.dataDir, "mcp", sanitizeNativeID(trimString(server["id"])))
	_ = os.MkdirAll(cmd.Dir, 0o700)
	cmd.Env = append(os.Environ(), envMapToList(server["env"])...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	client := &mcpStdioClient{reader: bufio.NewReader(stdout), writer: stdin}
	defer func() {
		_ = stdin.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()
	}()
	if _, err := client.request("initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "dirextalk-native-agent", "version": "0.1.0"},
	}); err != nil {
		return nil, fmt.Errorf("mcp initialize failed: %w stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	_ = client.notify("notifications/initialized", map[string]any{})
	result, err := fn(client)
	if err != nil {
		return nil, fmt.Errorf("%w stderr=%s", err, strings.TrimSpace(stderr.String()))
	}
	return result, nil
}

type mcpStdioClient struct {
	reader *bufio.Reader
	writer io.Writer
	nextID int
}

func (c *mcpStdioClient) request(method string, params map[string]any) (any, error) {
	c.nextID++
	id := c.nextID
	if err := writeMCPFrame(c.writer, map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	for {
		frame, err := readMCPFrame(c.reader)
		if err != nil {
			return nil, err
		}
		if int(int64Param(frame["id"])) != id {
			continue
		}
		if rawErr, ok := frame["error"]; ok {
			return nil, fmt.Errorf("mcp error: %v", rawErr)
		}
		return frame["result"], nil
	}
}

func (c *mcpStdioClient) notify(method string, params map[string]any) error {
	return writeMCPFrame(c.writer, map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
}

func writeMCPFrame(w io.Writer, payload map[string]any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n\r\n", len(data)); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readMCPFrame(r *bufio.Reader) (map[string]any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "{") {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	}
	length := 0
	for {
		if strings.EqualFold(strings.TrimSpace(strings.SplitN(line, ":", 2)[0]), "Content-Length") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				length, _ = strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
		line, err = r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(line) == "" {
			break
		}
	}
	if length <= 0 {
		return nil, fmt.Errorf("mcp frame missing content length")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var decoded map[string]any
	if err := json.Unmarshal(buf, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
