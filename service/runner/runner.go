package runner

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

// Default is the process-wide runner instance, set by main.go after construction.
// Exposed so the task controller can reach Cancel() without an injection chain
// through every gin handler. nil-safe: Cancel returns false if Default isn't set
// (e.g. during early startup or in unit tests).
var Default *RunnerService

type RunnerService struct {
	Pool     *common.WorkerPool
	ServerIp string
	Interval int
	Done     chan struct{}
	// cancels maps an in-flight taskId to its cancel func so StopTask can
	// actually interrupt the goroutine instead of just flipping a DB flag.
	// Entries are added when ProcesswithTaskType starts a task and removed
	// when it returns. sync.Map fits the read-mostly access pattern (Cancel
	// is rare; Store/Delete happen once per task).
	cancels sync.Map
}

func NewRunnerService(serverIp string, config config.CKManServerConfig) *RunnerService {
	return &RunnerService{
		ServerIp: serverIp,
		Interval: config.TaskInterval,
		Pool:     common.NewWorkerPool(8, 16), // for tasks, 8 goroutines is enough
		Done:     make(chan struct{}),
	}
}

// Boot recovers tasks that were left in Running status when the previous
// ckman process exited unexpectedly (crash / kill / restart). Without this,
// such tasks would stay Running forever — the polling loop only picks up
// Waiting tasks, so nobody would ever update their status.
//
// Policy: mark orphan Running tasks as Failed rather than re-enqueue. Most
// task types (deploy, addnode, archive, rebalance) are not idempotent; a
// half-done op can leave hosts in an inconsistent state that auto-retry would
// only worsen. Letting the operator see "ckman restarted while task was
// running" and decide manually is the safe default.
//
// Only tasks owned by this server (matching ServerIp) are touched, so in a
// multi-instance deployment each ckman heals its own orphans.
func (runner *RunnerService) Boot() {
	tasks, err := repository.Ps.GetAllTasks()
	if err != nil {
		log.Logger.Errorf("runner boot: list tasks failed: %v", err)
		return
	}
	const reason = "ckman restarted while task was running"
	for _, task := range tasks {
		if task.ServerIp != runner.ServerIp || task.Status != model.TaskStatusRunning {
			continue
		}
		t := task
		if err := deploy.SetTaskStatus(&t, model.TaskStatusFailed, reason); err != nil {
			log.Logger.Errorf("runner boot: mark task %s failed: %v", t.TaskId, err)
			continue
		}
		log.Logger.Warnf("runner boot: task %s (%s/%s) recovered as failed", t.TaskId, t.ClusterName, t.TaskType)
	}
}

// Cancel asks the runner to interrupt the in-flight task with the given id.
// Returns true if the task was found in the in-flight registry (cancellation
// signal delivered). Returns false if the task is not currently running on
// this instance — either it already finished, hasn't been picked up yet, or
// it's owned by a different ckman in a multi-instance deployment.
//
// The actual cancellation is cooperative: handlers must check ctx.Err() at
// phase boundaries. See handle.go and the rebalance package for the wiring.
func (runner *RunnerService) Cancel(taskId string) bool {
	v, ok := runner.cancels.Load(taskId)
	if !ok {
		return false
	}
	v.(context.CancelFunc)()
	return true
}

func (runner *RunnerService) Start() {
	log.Logger.Infof("runner service starting...")
	go runner.Run()
}

func (runner *RunnerService) Run() {
	ticker := time.NewTicker(time.Second * time.Duration(runner.Interval))
	defer ticker.Stop()
	for {
		select {
		case <-runner.Done:
			return
		case <-ticker.C:
			go runner.CheckTaskEvent()
		}
	}
}

func (runner *RunnerService) CheckTaskEvent() {
	tasks, err := repository.Ps.GetPengdingTasks(runner.ServerIp)
	if err != nil {
		return
	}

	for _, task := range tasks {
		_ = runner.Pool.Submit(func() {
			task := task
			if err := runner.ProcesswithTaskType(task); err != nil {
				log.Logger.Errorf("%s failed:%v", task.TaskType, err)
				return
			}
		})
	}
	runner.Pool.Wait()
}

func (runner *RunnerService) ProcesswithTaskType(task model.Task) error {
	log.Logger.Infof("task %s %s %s is triggered", task.TaskId, task.ClusterName, task.TaskType)
	err := deploy.SetTaskStatus(&task, model.TaskStatusRunning, model.TaskStatusMap[model.TaskStatusRunning])
	if err != nil {
		return err
	}

	// Each task gets its own cancellable context. Register in cancels so
	// StopTask can interrupt it; deregister on return so a finished task's
	// id doesn't accumulate.
	ctx, cancel := context.WithCancel(context.Background())
	runner.cancels.Store(task.TaskId, cancel)
	defer func() {
		runner.cancels.Delete(task.TaskId)
		cancel()
	}()

	defer func() {
		if err := recover(); err != nil {
			//update task status failed while task panic
			_ = deploy.SetTaskStatus(&task, model.TaskStatusFailed, "panic")
			log.Logger.Errorf("panic: %v", string(debug.Stack()))
		}
	}()
	if err := TaskHandleFunc[task.TaskType](ctx, &task); err != nil {
		// If the task was cancelled the controller already flipped the DB
		// status to Stopped; respect that and only log here. Otherwise it's
		// a real failure.
		if ctx.Err() != nil {
			deploy.SetNodeStatus(&task, model.NodeStatusFailed, model.ALL_NODES_DEFAULT)
			log.Logger.Infof("task %s cancelled: %v", task.TaskId, err)
			return err
		}
		deploy.SetNodeStatus(&task, model.NodeStatusFailed, model.ALL_NODES_DEFAULT)
		_ = deploy.SetTaskStatus(&task, model.TaskStatusFailed, err.Error())
		return err
	}
	return deploy.SetTaskStatus(&task, model.TaskStatusSuccess, model.TaskStatusMap[model.TaskStatusSuccess])
}

func (runner *RunnerService) Stop() {
	runner.Pool.Close()
	if checkDone() {
		log.Logger.Infof("all task are finished, exit gracefully")
		runner.Shutdown()
		return
	}

	//if have task still running, hold on 60000ms, then force shutdown
	log.Logger.Infof("still have task running, programe will exit after 60000ms")
	ticker := time.NewTicker(time.Second * time.Duration(10))
	timeout := time.NewTicker(time.Minute * time.Duration(1))
	defer ticker.Stop()
	defer timeout.Stop()
	for {
		select {
		case <-ticker.C: //check every 10s
			if checkDone() {
				log.Logger.Infof("all task are finished, exit gracefully")
				runner.Shutdown()
				return
			}
		case <-timeout.C:
			log.Logger.Warnf("time out waiting for task running, ignore and force exit.")
			tasks, _ := repository.Ps.GetAllTasks()
			for _, task := range tasks {
				if task.Status == model.TaskStatusRunning {
					task.Status = model.TaskStatusStopped
					repository.Ps.UpdateTask(task)
				}
			}

			runner.Shutdown()
			return
		}
	}
}

func (runner *RunnerService) Shutdown() {
	var done struct{}
	runner.Done <- done
}

func checkDone() bool {
	done := true
	tasks, err := repository.Ps.GetAllTasks()
	if err != nil {
		return done
	}
	for _, task := range tasks {
		if task.Status == model.TaskStatusRunning {
			done = false
			break
		}
	}
	return done
}
