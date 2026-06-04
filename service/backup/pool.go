package backup

import (
	"context"
	"sync"
)

// ExecFunc is the function signature for executing a backup run.
type ExecFunc func(ctx context.Context, runID string)

// DefaultMaxQueue 是排队 run 数量的兜底上限。正常场景下队列是"无界"的
// （200 张表同一 crontab 触发也全部排队消化），该值仅防御异常情况下
// 内存被无限撑大；超出时 Submit 返回 false，run 记为 skipped(queue_full)。
const DefaultMaxQueue = 10000

// Pool combines an unbounded in-memory FIFO queue with a worker pool.
// 入队顺序即执行顺序；并发度 = workers。
type Pool struct {
	workers  int
	exec     ExecFunc
	maxQueue int

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []string
	closed bool

	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewPool creates a new Pool with the given number of workers and executor function.
func NewPool(workers int, exec ExecFunc) *Pool {
	p := &Pool{
		workers:  workers,
		exec:     exec,
		maxQueue: DefaultMaxQueue,
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Start launches the worker goroutines. ctx cancellation makes idle workers
// exit; it does NOT cancel in-flight exec calls.
func (p *Pool) Start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		p.mu.Lock()
		p.closed = true
		p.cond.Broadcast()
		p.mu.Unlock()
	}()
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			p.mu.Unlock()
			return
		}
		runID := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		// Use an independent context so that Stop does not interrupt
		// a run that has already started executing.
		p.exec(context.Background(), runID)
	}
}

// Submit enqueues a runID. 队列无界（受 DefaultMaxQueue 兜底保护），
// 仅在 Pool 已停止或超出兜底上限时返回 false。
func (p *Pool) Submit(runID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || len(p.queue) >= p.maxQueue {
		return false
	}
	p.queue = append(p.queue, runID)
	p.cond.Signal()
	return true
}

// Stop discards queued-but-not-started runs and waits for all in-flight runs
// to complete. Already-running execs are not interrupted (they use an
// independent context). Discarded runs stay queued in the ledger and are
// marked interrupted by Boot on next startup.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.queue = nil
		p.cond.Broadcast()
		p.mu.Unlock()
		p.wg.Wait()
	})
}

// QueueLen returns the number of runs currently waiting in the queue.
// Useful for metrics and tests.
func (p *Pool) QueueLen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.queue)
}
