package backup

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"

	"github.com/housepower/ckman/log"
)

// ExecFunc is the function signature for executing a backup run.
type ExecFunc func(ctx context.Context, runID string)

// DefaultMaxQueue 是排队 run 数量的兜底上限。正常场景下队列是"无界"的
// （200 张表同一 crontab 触发也全部排队消化），该值仅防御异常情况下
// 内存被无限撑大；超出时 Submit 返回 ErrQueueFull，run 记为 skipped(queue_full)。
const DefaultMaxQueue = 10000

// Submit 的失败原因，调用方据此写台账 status_reason。
var (
	ErrQueueFull   = errors.New("backup queue full")
	ErrPoolStopped = errors.New("backup pool stopped")
)

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

	stopCh   chan struct{} // 通知 Start 里的 ctx 监听 goroutine 退出
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewPool creates a new Pool with the given number of workers and executor function.
func NewPool(workers int, exec ExecFunc) *Pool {
	p := &Pool{
		workers:  workers,
		exec:     exec,
		maxQueue: DefaultMaxQueue,
		stopCh:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	return p
}

// Start launches the worker goroutines. ctx cancellation shuts the pool down
// the same way Stop does (discarding queued runs); it does NOT cancel
// in-flight exec calls.
func (p *Pool) Start(ctx context.Context) {
	go func() {
		select {
		case <-ctx.Done():
			p.shutdown()
		case <-p.stopCh: // Stop() 已清理，本 goroutine 直接退出，不泄漏
		}
	}()
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

// shutdown 丢弃排队项并唤醒所有 worker 退出。ctx 取消与 Stop 共用同一清理，
// 保证两条路径下 QueueLen 一致归零、不残留既不执行也不被丢弃的 run。
func (p *Pool) shutdown() {
	p.mu.Lock()
	p.closed = true
	p.queue = nil
	p.cond.Broadcast()
	p.mu.Unlock()
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
		p.runOne(runID)
	}
}

// runOne 执行单个 run，兜底 recover：ExecFunc panic 只损失这一次执行，
// 不拖垮 worker goroutine 乃至整个进程（run 状态留 queued/running，
// 由 executeExclusive 的 recover 或重启 Boot 收敛）。
func (p *Pool) runOne(runID string) {
	defer func() {
		if rec := recover(); rec != nil {
			log.Logger.Errorf("[backup] worker recovered from panic on run %s: %v\n%s", runID, rec, debug.Stack())
		}
	}()
	// Use an independent context so that Stop does not interrupt
	// a run that has already started executing.
	p.exec(context.Background(), runID)
}

// Submit enqueues a runID. 队列无界（受 DefaultMaxQueue 兜底保护）；
// 返回 nil 表示入队成功，否则为 ErrPoolStopped / ErrQueueFull。
func (p *Pool) Submit(runID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrPoolStopped
	}
	if len(p.queue) >= p.maxQueue {
		return ErrQueueFull
	}
	p.queue = append(p.queue, runID)
	p.cond.Signal()
	return nil
}

// Stop discards queued-but-not-started runs and waits for all in-flight runs
// to complete. Already-running execs are not interrupted (they use an
// independent context). Discarded runs stay queued in the ledger and are
// marked interrupted by Boot on next startup.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
		p.shutdown()
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
