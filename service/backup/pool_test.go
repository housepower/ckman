package backup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestPool_DispatchToWorker(t *testing.T) {
	var ran int32
	exec := func(ctx context.Context, runID string) {
		atomic.AddInt32(&ran, 1)
	}
	p := NewPool(2, exec)
	p.Start(context.Background())
	defer p.Stop()

	if p.Submit("r1") != nil {
		t.Fatal("Submit r1 should succeed")
	}
	deadline := time.After(time.Second)
	for atomic.LoadInt32(&ran) == 0 {
		select {
		case <-deadline:
			t.Fatal("worker did not run")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestPool_UnboundedQueueAcceptsBurst(t *testing.T) {
	// 200 张表同一 crontab 触发的场景：全部入队成功，最终全部执行。
	var ran int32
	exec := func(ctx context.Context, runID string) { atomic.AddInt32(&ran, 1) }
	p := NewPool(8, exec)
	p.Start(context.Background())

	for i := range 200 {
		if p.Submit("r") != nil {
			t.Fatalf("Submit #%d should succeed on unbounded queue", i)
		}
	}
	deadline := time.After(5 * time.Second)
	for atomic.LoadInt32(&ran) < 200 {
		select {
		case <-deadline:
			t.Fatalf("only %d/200 runs executed", atomic.LoadInt32(&ran))
		case <-time.After(time.Millisecond):
		}
	}
	p.Stop()
}

func TestPool_MaxQueueGuardReturnsFalse(t *testing.T) {
	// 兜底上限：超出 maxQueue 时 Submit 返回 false
	block := make(chan struct{})
	exec := func(ctx context.Context, runID string) { <-block }
	p := NewPool(1, exec)
	p.maxQueue = 4
	p.Start(context.Background())
	defer func() { close(block); p.Stop() }()

	// 第 1 个被 worker 取走并阻塞在 exec
	if p.Submit("r") != nil {
		t.Fatal("first Submit should succeed")
	}
	time.Sleep(20 * time.Millisecond)

	// 之后 5 个：4 个进队列，第 5 个超出 maxQueue 被拒绝（ErrQueueFull）
	successCount := 0
	var lastErr error
	for range 5 {
		if err := p.Submit("r"); err == nil {
			successCount++
		} else {
			lastErr = err
		}
	}
	if successCount != 4 {
		t.Fatalf("expected 4 success (maxQueue=4), got %d", successCount)
	}
	if !errors.Is(lastErr, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", lastErr)
	}
}

func TestPool_StopDiscardsQueuedAndRejectsSubmit(t *testing.T) {
	block := make(chan struct{})
	exec := func(ctx context.Context, runID string) { <-block }
	p := NewPool(1, exec)
	p.Start(context.Background())
	time.Sleep(20 * time.Millisecond) // worker 拿走第 1 个
	p.Submit("running")
	p.Submit("queued1")
	p.Submit("queued2")
	go func() { time.Sleep(10 * time.Millisecond); close(block) }()
	p.Stop()
	if p.QueueLen() != 0 {
		t.Fatalf("queued runs should be discarded on Stop, got %d", p.QueueLen())
	}
	if err := p.Submit("after-stop"); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("Submit after Stop should return ErrPoolStopped, got %v", err)
	}
}

func TestPool_CtxCancelDiscardsQueueLikeStop(t *testing.T) {
	// ctx 取消与 Stop 走同一清理：队列归零、worker 退出、Submit 拒绝
	block := make(chan struct{})
	defer close(block)
	exec := func(ctx context.Context, runID string) { <-block }
	ctx, cancel := context.WithCancel(context.Background())
	p := NewPool(1, exec)
	p.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	p.Submit("running")
	p.Submit("queued1")
	cancel()
	deadline := time.After(time.Second)
	for p.QueueLen() != 0 {
		select {
		case <-deadline:
			t.Fatalf("ctx cancel should discard queue, got %d", p.QueueLen())
		case <-time.After(time.Millisecond):
		}
	}
	if err := p.Submit("after-cancel"); !errors.Is(err, ErrPoolStopped) {
		t.Fatalf("Submit after ctx cancel should return ErrPoolStopped, got %v", err)
	}
}

func TestPool_StopDoesNotInterruptRunning(t *testing.T) {
	done := make(chan string, 1)
	exec := func(ctx context.Context, runID string) {
		time.Sleep(50 * time.Millisecond)
		done <- runID
	}
	p := NewPool(1, exec)
	p.Start(context.Background())
	p.Submit("r1")
	time.Sleep(10 * time.Millisecond)
	p.Stop()
	select {
	case got := <-done:
		if got != "r1" {
			t.Fatalf("got %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("running run was interrupted")
	}
}

func TestPool_QueueLen(t *testing.T) {
	block := make(chan struct{})
	exec := func(ctx context.Context, runID string) { <-block }
	p := NewPool(1, exec)
	p.Start(context.Background())
	defer func() { close(block); p.Stop() }()

	time.Sleep(20 * time.Millisecond)
	for i := 0; i < 3; i++ {
		p.Submit("r")
	}
	time.Sleep(20 * time.Millisecond) // 等 worker 消费 1 个
	got := p.QueueLen()
	if got < 2 || got > 3 {
		t.Fatalf("QueueLen got %d, expected 2 or 3", got)
	}
}
