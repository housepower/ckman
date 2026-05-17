package deploy

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-basic/uuid"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
)

func CreateNewTask(clusterName, taskType string, deploy interface{}) (string, error) {
	if HasEffectiveTasks(clusterName) {
		err := errors.Errorf("create task failed, cluster %s has another task already running", clusterName)
		return "", err
	}

	var hosts []string
	var zknodes []string
	switch d := deploy.(type) {
	case *CKDeploy:
		repository.EncodePasswd(deploy.(*CKDeploy).Conf)
		hosts = d.Conf.Hosts
		if d.Conf.KeeperWithStanalone() {
			zknodes = d.Conf.KeeperConf.KeeperNodes
		}
	case *model.ArchiveTableReq:
		conf, _ := repository.Ps.GetClusterbyName(clusterName)
		hosts = conf.Hosts
	case *model.RebalanceTableReq:
		conf, _ := repository.Ps.GetClusterbyName(clusterName)
		hosts = conf.Hosts
	default:
		return "", fmt.Errorf("unknown module")
	}

	var nodeStatus []model.NodeStatus
	for _, host := range hosts {
		node := model.NodeStatus{
			Host:   host,
			Status: model.NodeStatusWating,
		}
		nodeStatus = append(nodeStatus, node)
	}

	var knodeStatus []model.NodeStatus
	if len(zknodes) > 0 {
		for _, host := range zknodes {
			node := model.NodeStatus{
				Host:   host,
				Status: model.NodeStatusWating,
			}
			knodeStatus = append(knodeStatus, node)
		}
	}

	//uuid will generate a global primary key like: 2169dde9-f417-8ddb-a524-0354b3eb4dc2
	taskId := uuid.New()
	task := model.Task{
		TaskId:       taskId,
		ClusterName:  clusterName,
		ServerIp:     common.GetOutboundIP().String(),
		DeployConfig: deploy,
		Status:       model.TaskStatusWaiting,
		Message:      model.TaskStatusMap[model.TaskStatusWaiting],
		TaskType:     taskType,
		ZKNodes:      knodeStatus,
		CKNodes:      nodeStatus,
		NodeStatus:   nodeStatus,
		CreateTime:   time.Now(),
		UpdateTime:   time.Now(),
	}
	err := repository.Ps.CreateTask(task)
	if err != nil {
		return "", err
	}
	return task.TaskId, nil
}

func HasEffectiveTasks(clusterName string) bool {
	tasks, err := repository.Ps.GetAllTasks()
	if err != nil {
		return false
	}
	for _, task := range tasks {
		if task.Status == model.TaskStatusFailed || task.Status == model.TaskStatusSuccess || task.Status == model.TaskStatusStopped {
			continue
		}
		if clusterName == task.ClusterName {
			return true
		}
		logics, err := repository.Ps.GetLogicClusterbyName(clusterName)
		if err != nil {
			continue
		}
		if common.ArraySearch(clusterName, logics) {
			return true
		}
	}
	return false
}

// isTerminalStatus reports whether a task has reached a state that the runner
// must not overwrite. Stopped is set by the controller via the user clicking
// Stop; Success/Failed mean the task has already concluded. Any subsequent
// runner-side update for the same task would clobber the terminal record —
// see Phase 3 review for the lost-update scenario this prevents.
func isTerminalStatus(s int) bool {
	return s == model.TaskStatusStopped || s == model.TaskStatusSuccess || s == model.TaskStatusFailed
}

// SetTaskStatus persists the task's overall status. Skips the write if the
// persisted task already reached a terminal status (typically Stopped from a
// concurrent user Cancel) — the in-memory copy is stale and would otherwise
// trample the terminal state back to Running/Success/Failed.
func SetTaskStatus(task *model.Task, status int, msg string) error {
	fresh, err := repository.Ps.GetTaskbyTaskId(task.TaskId)
	if err == nil && isTerminalStatus(fresh.Status) && fresh.Status != status {
		log.Logger.Infof("[%s] skip status update %d→%d: task already terminal (%d)", task.TaskId, task.Status, status, fresh.Status)
		return nil
	}
	task.Status = status
	task.Message = msg
	return repository.Ps.UpdateTask(*task)
}

// SetTaskStep records a top-level phase transition on the task. Used by task
// types whose progress is naturally a sequence of named phases (rebalance,
// archive) rather than per-host node status. Failures to persist are logged
// but not propagated — losing a step update should never abort the underlying
// operation.
//
// Like SetTaskStatus, this bails out if the persisted task has reached a
// terminal status, to avoid trampling a concurrent Stop/Success/Failed write
// with the stale in-memory Status=Running this caller still holds.
func SetTaskStep(task *model.Task, step model.Internationalization) {
	fresh, err := repository.Ps.GetTaskbyTaskId(task.TaskId)
	if err == nil && isTerminalStatus(fresh.Status) {
		log.Logger.Infof("[%s] skip step %s: task already terminal (status=%d)", task.TaskId, step.EN, fresh.Status)
		return
	}
	task.Step = step
	if err := repository.Ps.UpdateTask(*task); err != nil {
		log.Logger.Errorf("%s %s update task step failed: %v", task.TaskId, task.ClusterName, err)
	}
	log.Logger.Infof("[%s] %s current step: %s", task.ClusterName, task.TaskType, step.EN)
}

func SetNodeStatus(task *model.Task, status model.Internationalization, host string) {
	fresh, err := repository.Ps.GetTaskbyTaskId(task.TaskId)
	if err == nil && isTerminalStatus(fresh.Status) {
		log.Logger.Infof("[%s] skip node status %s: task already terminal (status=%d)", task.TaskId, status.EN, fresh.Status)
		return
	}
	t := strings.Split(task.TaskType, ".")[0]
	switch t {
	case "keeper":
		for idx, node := range task.ZKNodes {
			if host == model.ALL_NODES_DEFAULT {
				task.ZKNodes[idx].Status = status
			} else {
				for _, h := range strings.Split(host, ",") {
					if node.Host == h {
						task.ZKNodes[idx].Status = status
						break
					}
				}
			}
		}
	default:
		for idx, node := range task.CKNodes {
			if host == model.ALL_NODES_DEFAULT {
				task.CKNodes[idx].Status = status
			} else {
				for _, h := range strings.Split(host, ",") {
					if node.Host == h {
						task.CKNodes[idx].Status = status
						break
					}
				}
			}
		}
	}
	if err := repository.Ps.UpdateTask(*task); err != nil {
		log.Logger.Errorf("%s %s update node status failed: %v", task.TaskId, task.ClusterName, err)
	}
	log.Logger.Infof("[%s-%s] %s current step: %s", task.ClusterName, host, task.TaskType, status.EN)
}
