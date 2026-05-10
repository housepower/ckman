package backup

import (
	"context"
	"sync"
)

// ExecFunc is the function signature for executing a backup run.
type ExecFunc func(ctx context.Context, runID string)

// Pool combines an in-memory channel queue with a worker pool.
// Queue capacity = workers * 4 (per spec §5.1).
type Pool struct {
	workers  int
	queue    chan string
	exec     ExecFunc
	wg       sync.WaitGroup
	cancel   context.CancelFunc
	stopOnce sync.Once
}

// NewPool creates a new Pool with the given number of workers and executor function.
func NewPool(workers int, exec ExecFunc) *Pool {
	return &Pool{
		workers: workers,
		queue:   make(chan string, workers*4),
		exec:    exec,
	}
}

// Start launches the worker goroutines. ctx is used only for the workers'
// blocking-on-queue select; it does NOT cancel in-flight exec calls.
func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case runID, ok := <-p.queue:
			if !ok {
				return
			}
			// Use an independent context so that Stop does not interrupt
			// a run that has already started executing.
			p.exec(context.Background(), runID)
		}
	}
}

// Submit enqueues a runID non-blockingly. Returns false if the queue is full.
func (p *Pool) Submit(runID string) bool {
	select {
	case p.queue <- runID:
		return true
	default:
		return false
	}
}

// Stop closes the queue and waits for all in-flight runs to complete.
// Already-running execs are not interrupted (they use an independent context).
// Workers blocked on queue receive will exit via ctx.Done or channel close.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		// Close queue first so workers drain any remaining items then see
		// the channel is closed and return.
		close(p.queue)
		// Cancel ctx as an escape hatch for workers blocked in the select
		// before they could pick up the closed-channel signal.
		if p.cancel != nil {
			p.cancel()
		}
		p.wg.Wait()
	})
}

// QueueLen returns the number of runs currently waiting in the queue.
// Useful for metrics and tests.
func (p *Pool) QueueLen() int { return len(p.queue) }
