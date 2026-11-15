package agent

import "context"

type EventType string

const (
	EventMessageDelta EventType = "message_delta"
	EventUsage        EventType = "usage"
	EventError        EventType = "error"
	EventDone         EventType = "done"
)

type Event struct {
	Type         EventType `json:"type"`
	Delta        string    `json:"delta,omitempty"`
	InputTokens  int       `json:"input_tokens,omitempty"`
	OutputTokens int       `json:"output_tokens,omitempty"`
	Error        string    `json:"error,omitempty"`
}

type ExplainRequest struct {
	Report []byte
}

type Engine interface {
	Explain(context.Context, ExplainRequest, func(Event)) error
}
