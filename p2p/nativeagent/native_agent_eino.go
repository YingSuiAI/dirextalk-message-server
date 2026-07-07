package nativeagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	einotool "github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/flow/agent/react"
	"github.com/cloudwego/eino/schema"
)

const nativeAgentGraphName = "DirextalkNativeAgent"

func (r *Runtime) runEinoAgent(ctx context.Context, profile nativeModelProfile, messages []*schema.Message, session einoAgentSession, tools []einotool.BaseTool) (string, []map[string]any, []*schema.Message, error) {
	agentRunner, err := r.newEinoAgent(ctx, profile, tools, session)
	if err != nil {
		return "", nil, nil, err
	}
	recorder := &einoToolCallRecorder{}
	futureOpt, future := react.WithMessageFuture()
	message, err := agentRunner.Generate(ctx, messages, recorder.option(), futureOpt)
	produced, futureErr := collectGeneratedEinoMessages(future)
	if err != nil {
		return "", recorder.snapshot(), produced, err
	}
	if futureErr != nil {
		return "", recorder.snapshot(), produced, futureErr
	}
	if message == nil {
		return "", recorder.snapshot(), produced, fmt.Errorf("model returned empty response")
	}
	if len(produced) == 0 && message != nil {
		produced = append(produced, message)
	}
	return strings.TrimSpace(message.Content), recorder.snapshot(), produced, nil
}

func (r *Runtime) streamEinoAgent(ctx context.Context, profile nativeModelProfile, messages []*schema.Message, session einoAgentSession, tools []einotool.BaseTool, emit func(Event) error) (string, []map[string]any, []*schema.Message, error) {
	agentRunner, err := r.newEinoAgent(ctx, profile, tools, session)
	if err != nil {
		return "", nil, nil, err
	}
	recorder := &einoToolCallRecorder{}
	futureOpt, future := react.WithMessageFuture()
	stream, err := agentRunner.Stream(ctx, messages, recorder.option(), futureOpt)
	if err != nil {
		return "", recorder.snapshot(), nil, err
	}
	defer stream.Close()
	var full strings.Builder
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", recorder.snapshot(), nil, err
		}
		if chunk == nil || chunk.Content == "" {
			continue
		}
		full.WriteString(chunk.Content)
		if err := emit(Event{Event: "delta", Data: map[string]any{"text": chunk.Content}}); err != nil {
			return "", recorder.snapshot(), nil, err
		}
	}
	produced, err := collectStreamedEinoMessages(future)
	if err != nil {
		return "", recorder.snapshot(), produced, err
	}
	return full.String(), recorder.snapshot(), produced, nil
}

func (r *Runtime) newEinoAgent(ctx context.Context, profile nativeModelProfile, tools []einotool.BaseTool, session einoAgentSession) (*react.Agent, error) {
	chatModel, err := r.newEinoChatModel(ctx, profile)
	if err != nil {
		return nil, err
	}
	return react.NewAgent(ctx, &react.AgentConfig{
		ToolCallingModel: chatModel,
		ToolsConfig: compose.ToolsNodeConfig{
			Tools:               tools,
			ExecuteSequentially: true,
			UnknownToolsHandler: func(ctx context.Context, name, input string) (string, error) {
				return jsonValue(map[string]any{"error": fmt.Sprintf("tool %q is not available", name), "arguments": input}), nil
			},
		},
		MaxStep:               nativeAgentToolCallLimit*2 + 4,
		GraphName:             nativeAgentGraphName,
		ModelNodeName:         "NativeAgentModel",
		ToolsNodeName:         "NativeAgentTools",
		MessageRewriter:       session.rewrite,
		MessageModifier:       session.modify,
		StreamToolCallChecker: scanStreamForToolCalls,
	})
}

func scanStreamForToolCalls(_ context.Context, sr *schema.StreamReader[*schema.Message]) (bool, error) {
	defer sr.Close()
	for {
		msg, err := sr.Recv()
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if msg != nil && len(msg.ToolCalls) > 0 {
			return true, nil
		}
	}
}

func collectGeneratedEinoMessages(future react.MessageFuture) ([]*schema.Message, error) {
	iter := future.GetMessages()
	var produced []*schema.Message
	for {
		message, ok, err := iter.Next()
		if err != nil {
			return produced, err
		}
		if !ok {
			return produced, nil
		}
		if message != nil {
			produced = append(produced, message)
		}
	}
}

func collectStreamedEinoMessages(future react.MessageFuture) ([]*schema.Message, error) {
	iter := future.GetMessageStreams()
	var produced []*schema.Message
	for {
		stream, ok, err := iter.Next()
		if err != nil {
			return produced, err
		}
		if !ok {
			return produced, nil
		}
		if stream == nil {
			continue
		}
		message, err := schema.ConcatMessageStream(stream)
		if err != nil {
			return produced, err
		}
		if message != nil {
			produced = append(produced, message)
		}
	}
}
