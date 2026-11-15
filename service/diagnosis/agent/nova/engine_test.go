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

func TestConfigStatus(t *testing.T) {
	if got := ConfigStatus(config.LLMConfig{}); got.State != StateDisabled {
		t.Fatalf("disabled status = %+v", got)
	}
	t.Setenv("CKMAN_TEST_MISSING_LLM_KEY", "")
	got := ConfigStatus(config.LLMConfig{Enabled: true, BaseURL: "https://example.com/v1", Model: "test", APIKeyEnv: "CKMAN_TEST_MISSING_LLM_KEY"})
	if got.State != StateMisconfigured || got.Message != "AI 凭据未配置，请设置 API key 环境变量或 api_key_file" {
		t.Fatalf("misconfigured status = %+v", got)
	}
}

func TestFriendlyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "deadline", err: context.DeadlineExceeded, want: "AI 服务响应超时，请检查 endpoint 网络连通性或增大 timeout_seconds"},
		{name: "invalid key", err: &provider.Error{Status: 401, Code: "invalid_api_key"}, want: "AI 服务鉴权失败，请检查 API key 是否填写错误或已经过期"},
		{name: "quota", err: &provider.Error{Status: 402}, want: "AI 服务账户余额或可用额度不足，请前往模型服务商检查账户"},
		{name: "model", err: &provider.Error{Status: 400, Message: "model not found"}, want: "AI 模型配置无效或当前账户无权使用，请检查 model"},
		{name: "endpoint", err: &provider.Error{Status: 404}, want: "AI 服务接口或模型不存在，请检查 base_url 和 model"},
		{name: "rate limit", err: &provider.Error{Status: 429}, want: "AI 服务请求受限，请稍后重试并检查调用频率或账户额度"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := FriendlyError(test.err); got != test.want {
				t.Fatalf("FriendlyError() = %q, want %q", got, test.want)
			}
		})
	}
}
