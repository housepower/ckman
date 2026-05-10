package backup

import (
	"context"
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

	if !p.Submit("r1") {
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

func TestPool_QueueFullReturnsFalse(t *testing.T) {
	// 1 worker，队列容量 4。Submit 流程：
	// 第 1 个进 worker（被卡住），第 2-5 个进队列，第 6 个返 false
	block := make(chan struct{})
	exec := func(ctx context.Context, runID string) { <-block }
	p := NewPool(1, exec)
	p.Start(context.Background())
	defer func() { close(block); p.Stop() }()

	// 给 worker 时间消费第一条
	time.Sleep(20 * time.Millisecond)

	successCount := 0
	for i := 0; i < 6; i++ {
		if p.Submit("r") {
			successCount++
		}
	}
	if successCount != 5 {
		t.Fatalf("expected 5 success (1 worker + 4 queue), got %d", successCount)
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
