package nova

import (
	"context"
	"testing"

	"github.com/AutumnsGrove/nova/message"
	"github.com/AutumnsGrove/nova/provider"
	"github.com/housepower/ckman/config"
	ckagent "github.com/housepower/ckman/service/diagnosis/agent"
)

type fakeProvider struct{ request provider.Request }

func (f *fakeProvider) Name() string { return "fake" }
func (f *fakeProvider) Stream(_ context.Context, req provider.Request, emit func(provider.StreamEvent)) (*provider.Response, error) {
	f.request = req
	emit(provider.StreamEvent{Type: provider.StreamReasoningDelta, Text: "hidden chain"})
	emit(provider.StreamEvent{Type: provider.StreamTextDelta, Text: "AI 解释"})
	return &provider.Response{Message: message.Assistant("AI 解释"), Usage: provider.Usage{PromptTokens: 10, CompletionTokens: 2}}, nil
}

func TestExplainStreamsOnlyAnswerAndNoTools(t *testing.T) {
	fake := &fakeProvider{}
	engine := &Engine{cfg: config.LLMConfig{Model: "test", TimeoutSeconds: 1, MaxOutputTokens: 99}, provider: fake}
	var events []ckagent.Event
	err := engine.Explain(context.Background(), ckagent.ExplainRequest{Report: []byte(`{"execution":"partial"}`)}, func(event ckagent.Event) { events = append(events, event) })
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.request.Tools) != 0 || len(fake.request.Messages) != 2 || fake.request.MaxTokens != 99 {
		t.Fatalf("unsafe request: %+v", fake.request)
	}
	if len(events) != 3 || events[0].Delta != "AI 解释" || events[1].InputTokens != 10 || events[2].Type != ckagent.EventDone {
		t.Fatalf("events: %+v", events)
	}
	for _, event := range events {
		if event.Delta == "hidden chain" {
			t.Fatal("reasoning leaked")
		}
	}
}
