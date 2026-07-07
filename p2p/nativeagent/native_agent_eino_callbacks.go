package nativeagent

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent"
	"github.com/cloudwego/eino/schema"
	cbtemplate "github.com/cloudwego/eino/utils/callbacks"
)

type einoToolCallRecorder struct {
	mu    sync.Mutex
	calls []map[string]any
}

func (r *einoToolCallRecorder) option() agent.AgentOption {
	handler := cbtemplate.NewHandlerHelper().ChatModel(&cbtemplate.ModelCallbackHandler{
		OnEnd: func(ctx context.Context, info *callbacks.RunInfo, output *model.CallbackOutput) context.Context {
			if output != nil && output.Message != nil {
				r.record(output.Message.ToolCalls)
			}
			return ctx
		},
		OnEndWithStreamOutput: func(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[*model.CallbackOutput]) context.Context {
			defer output.Close()
			for {
				chunk, err := output.Recv()
				if err != nil {
					return ctx
				}
				if chunk != nil && chunk.Message != nil {
					r.record(chunk.Message.ToolCalls)
				}
			}
		},
	}).Handler()
	return agent.WithComposeOptions(compose.WithCallbacks(handler))
}

func (r *einoToolCallRecorder) record(calls []schema.ToolCall) {
	if r == nil || len(calls) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, call := range calls {
		var args map[string]any
		_ = json.Unmarshal([]byte(call.Function.Arguments), &args)
		if args == nil {
			args = map[string]any{}
		}
		r.calls = append(r.calls, map[string]any{
			"id":        call.ID,
			"type":      fallbackString(call.Type, "function"),
			"function":  map[string]any{"name": call.Function.Name, "arguments": call.Function.Arguments},
			"name":      call.Function.Name,
			"arguments": args,
		})
	}
}

func (r *einoToolCallRecorder) snapshot() []map[string]any {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]map[string]any{}, r.calls...)
}
