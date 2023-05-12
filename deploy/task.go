package deploy

import (
	"fmt"
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
	switch d := deploy.(type) {
	case *CKDeploy:
		repository.EncodePasswd(deploy.(*CKDeploy).Conf)
		hosts = d.Conf.Hosts
	case *model.ArchiveTableReq:
		conf, _ := repository.Ps.GetClusterbyName(clusterName)
		hosts = conf.Hosts
	case *model.RebalanceTableReq:
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

func SetTaskStatus(task *model.Task, status int, msg string) error {
	task.Status = status
	task.Message = msg
	return repository.Ps.UpdateTask(*task)
}

func SetNodeStatus(task *model.Task, status model.Internationalization, host string) {
	for idx, node := range task.NodeStatus {
		if host == node.Host || host == model.ALL_NODES_DEFAULT {
			task.NodeStatus[idx].Status = status
		}
	}
	err := repository.Ps.UpdateTask(*task)
	if err != nil {
		log.Logger.Errorf("%s %s update node status failed: %v", task.TaskId, task.ClusterName, err)
	}
	log.Logger.Infof("[%s-%s] %s current step: %s", task.ClusterName, host, task.TaskType, status.EN)
}
