package runner

import (
	"runtime/debug"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

type RunnerService struct {
	Pool     *common.WorkerPool
	ServerIp string
	Interval int
	Done     chan struct{}
}

func NewRunnerService(serverIp string, config config.CKManServerConfig) *RunnerService {
	return &RunnerService{
		ServerIp: serverIp,
		Interval: config.TaskInterval,
		Pool:     common.NewWorkerPool(8, 16), // for tasks, 8 goroutines is enough
		Done:     make(chan struct{}),
	}
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
	defer func() {
		if err := recover(); err != nil {
			//update task status failed while task panic
			_ = deploy.SetTaskStatus(&task, model.TaskStatusFailed, "panic")
			log.Logger.Errorf("panic: %v", string(debug.Stack()))
		}
	}()
	if err := TaskHandleFunc[task.TaskType](&task); err != nil {
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
