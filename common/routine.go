package common

import (
	"runtime"
	"sync"
)

var (
	MaxWorkersDefault = MaxInt(runtime.NumCPU()*2, 10)
)

type GoRoutine struct {
	pool  *WorkerPool
	wg    sync.WaitGroup
	ch    chan error
	delta int
}

func NewGoRoutine(maxWorkers int, queueSize int) *GoRoutine {
	return &GoRoutine{
		pool:  NewWorkerPool(maxWorkers, queueSize),
		wg:    sync.WaitGroup{},
		ch:    make(chan error, queueSize),
		delta: queueSize,
	}
}

func (this *GoRoutine) Init() {
	if this.delta == 0 {
		this.delta = 1
	}
	this.ch = make(chan error, this.delta)
	this.wg.Add(this.delta)
}

func (this *GoRoutine) HoldComplete() {
	this.wg.Add(1)
}

func (this *GoRoutine) SendComplete() {
	this.wg.Done()
}

func (this *GoRoutine) WaitComplete() {
	this.wg.Wait()
}

func (this *GoRoutine) SendTerm() {
	for i := 0; i < this.delta; i++ {
		this.wg.Done()
	}
}

func (this *GoRoutine) SendError(err error) {
	this.ch <- err
}

func (this *GoRoutine) HandleError() error {
	close(this.ch)
	for err := range this.ch {
		return err
	}
	return nil
}

func (this *GoRoutine) Go(fn func()) {
	_ = this.pool.Submit(fn)
}
