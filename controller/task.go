package controller

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/go-errors/errors"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

const (
	TaskIdPath = "taskId"
)

type TaskController struct {
	Controller
}

func NewTaskController(wrapfunc Wrapfunc) *TaskController {
	return &TaskController{
		Controller: Controller{
			wrapfunc: wrapfunc,
		},
	}
}

// @Summary GetTaskById
// @Description Get task by taskId
// @version 1.0
// @Security ApiKeyAuth
// @Param taskId query string true "task id" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":nil}"
// @Failure 200 {string} json "{"retCode":"5100","retMsg":"get task failed","entity":nil}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":{\"TaskId\":\"608e9e83-715e-7448-a149-9bef33f38cfe\",\"ClusterName\":\"usertest\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"升级集群\",\"EN\":\"Upgrade\"},\"NodeStatus\":[{\"Host\":\"192.168.110.10\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}},{\"Host\":\"192.168.110.12\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}},{\"Host\":\"192.168.110.14\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}}]}}"
// @Router /api/v1/task/{taskId} [get]
func (controller *TaskController) GetTaskStatusById(c *gin.Context) {
	taskId := c.Param(TaskIdPath)
	if taskId == "" {
		err := fmt.Errorf("expect taskId but got null")
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	task, err := repository.Ps.GetTaskbyTaskId(taskId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	resp := model.TaskStatusResp{
		TaskId:      task.TaskId,
		ClusterName: task.ClusterName,
		Type:        strings.Split(task.TaskType, ".")[0],
		Option:      model.TaskOptionMap[task.TaskType],
		NodeStatus:  task.NodeStatus,
	}

	controller.wrapfunc(c, model.E_SUCCESS, resp)
}

// @Summary TasksList
// @Description Get all tasklist
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":[{\"TaskId\":\"608e9e83-715e-7448-a149-9bef33f38cfe\",\"ClusterName\":\"usertest\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"升级集群\",\"EN\":\"Upgrade\"},\"Status\":\"Success\",\"Message\":\"Success\",\"CreateTime\":\"2022-08-15T10:38:52.319504494+08:00\",\"UpdateTime\":\"2022-08-15T10:39:22.177215927+08:00\",\"Duration\":\"29s\"},{\"TaskId\":\"c6ee8843-36ba-4c88-94dd-0f226cdf8377\",\"ClusterName\":\"abc\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"设置集群\",\"EN\":\"Setting\"},\"Status\":\"Success\",\"Message\":\"Success\",\"CreateTime\":\"2022-08-09T14:28:00.697211511+08:00\",\"UpdateTime\":\"2022-08-09T14:28:59.887673161+08:00\",\"Duration\":\"59s\"}]}"
// @Router /api/v1/task/lists [get]
func (controller *TaskController) TasksList(c *gin.Context) {
	tasks, err := repository.Ps.GetAllTasks()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	var resps model.TaskResps
	for _, task := range tasks {
		typ := strings.Split(task.TaskType, ".")[0]
		option := model.TaskOptionMap[task.TaskType]
		resp := model.TaskResp{
			TaskId:      task.TaskId,
			ClusterName: task.ClusterName,
			Type:        typ,
			Option:      option,
			Message:     task.Message,
			Status:      model.TaskStatusMap[task.Status],
			CreateTime:  task.CreateTime,
			UpdateTime:  task.UpdateTime,
			Duration:    common.ConvertDuration(task.CreateTime, task.UpdateTime),
		}
		resps = append(resps, resp)
	}
	if len(resps) == 0 {
		resps = model.TaskResps{}
	}

	//sort by updateTime
	sort.Sort(resps)
	controller.wrapfunc(c, model.E_SUCCESS, resps)
}

// @Summary GetRunningTaskCount
// @Description Get running task count
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":3}"
// @Router /api/v1/task/running [get]
func (controller *TaskController) GetRunningTaskCount(c *gin.Context) {
	count := repository.Ps.GetEffectiveTaskCount()
	controller.wrapfunc(c, model.E_SUCCESS, count)
}

// @Summary DeleteTask
// @Description delete task by taskid
// @version 1.0
// @Security ApiKeyAuth
// @Param taskId query string true "taskId" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"retCode":"5101","retMsg":"delete task failed","entity":nil}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/task/{taskId} [delete]
func (controller *TaskController) DeleteTask(c *gin.Context) {
	taskId := c.Param(TaskIdPath)
	if taskId == "" {
		err := fmt.Errorf("expect taskId but got null")
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	task, err := repository.Ps.GetTaskbyTaskId(taskId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if task.Status == model.TaskStatusRunning || task.Status == model.TaskStatusWaiting {
		err := errors.New("can't delete waiting or running task")
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	if err := repository.Ps.DeleteTask(taskId); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary StopTask
// @Description stop task by taskid
// @version 1.0
// @Security ApiKeyAuth
// @Param taskId query string true "taskId" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"retCode":"5102","retMsg":"stop task failed","entity":nil}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/task/{taskId} [put]
func (controller *TaskController) StopTask(c *gin.Context) {
	taskId := c.Param(TaskIdPath)
	if taskId == "" {
		err := fmt.Errorf("expect taskId but got null")
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	task, err := repository.Ps.GetTaskbyTaskId(taskId)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if task.Status != model.TaskStatusRunning && task.Status != model.TaskStatusWaiting {
		err := errors.Errorf("can't stop task while status is %d", task.Status)
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	task.Status = model.TaskStatusStopped
	task.Message = "Manual cancellation, only modifies the task status, does not actually stop the task"
	if err := repository.Ps.UpdateTask(task); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}
