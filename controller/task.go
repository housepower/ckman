package controller

import (
	"fmt"
	"sort"
	"strings"
	"time"

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

// @Summary 获取任务状态
// @Description 根据任务ID获取任务状态
// @version 1.0
// @Security ApiKeyAuth
// @Tags task
// @Accept  json
// @Param taskId query string true "task id" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":nil}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":nil}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":{\"TaskId\":\"608e9e83-715e-7448-a149-9bef33f38cfe\",\"ClusterName\":\"usertest\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"升级集群\",\"EN\":\"Upgrade\"},\"NodeStatus\":[{\"Host\":\"192.168.110.10\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}},{\"Host\":\"192.168.110.12\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}},{\"Host\":\"192.168.110.14\",\"Status\":{\"ZH\":\"上传安装包\",\"EN\":\"Prepare\"}}]}}"
// @Router /api/v2/task/{taskId} [get]
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

// @Summary 获取任务列表
// @Description 获取所有的任务列表
// @version 1.0
// @Security ApiKeyAuth
// @Tags task
// @Accept  json
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":nil}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":[{\"TaskId\":\"608e9e83-715e-7448-a149-9bef33f38cfe\",\"ClusterName\":\"usertest\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"升级集群\",\"EN\":\"Upgrade\"},\"Status\":\"Success\",\"Message\":\"Success\",\"CreateTime\":\"2022-08-15T10:38:52.319504494+08:00\",\"UpdateTime\":\"2022-08-15T10:39:22.177215927+08:00\",\"Duration\":\"29s\"},{\"TaskId\":\"c6ee8843-36ba-4c88-94dd-0f226cdf8377\",\"ClusterName\":\"abc\",\"Type\":\"clickhouse\",\"Option\":{\"ZH\":\"设置集群\",\"EN\":\"Setting\"},\"Status\":\"Success\",\"Message\":\"Success\",\"CreateTime\":\"2022-08-09T14:28:00.697211511+08:00\",\"UpdateTime\":\"2022-08-09T14:28:59.887673161+08:00\",\"Duration\":\"59s\"}]}"
// @Router /api/v2/task/lists [get]
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
		lastTime := common.TernaryExpression(task.Status == model.TaskStatusRunning || task.Status == model.TaskStatusWaiting, time.Now(), task.UpdateTime).(time.Time)
		resp := model.TaskResp{
			TaskId:      task.TaskId,
			ClusterName: task.ClusterName,
			Type:        typ,
			Option:      option,
			Message:     task.Message,
			Status:      model.TaskStatusMap[task.Status],
			CreateTime:  task.CreateTime,
			UpdateTime:  task.UpdateTime,
			Duration:    common.ConvertDuration(task.CreateTime, lastTime),
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

// @Summary 获取正在运行的任务个数
// @Description 获取正在运行的任务个数
// @version 1.0
// @Security ApiKeyAuth
// @Tags task
// @Accept  json
// @Success 200 {string} json "{"code":"0000","msg":"success","data":3}"
// @Router /api/v2/task/running [get]
func (controller *TaskController) GetRunningTaskCount(c *gin.Context) {
	count := repository.Ps.GetEffectiveTaskCount()
	controller.wrapfunc(c, model.E_SUCCESS, count)
}

// @Summary 删除任务
// @Description 根据任务ID删除指定的任务
// @version 1.0
// @Security ApiKeyAuth
// @Tags task
// @Accept  json
// @Param taskId query string true "taskId" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":nil}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":nil}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":nil}"
// @Failure 200 {string} json "{"code":"5803","msg":"数据删除失败","data":nil}"
// @Success 200 {string} json "{"code":"0000","msg":"success","msg":nil}"
// @Router /api/v2/task/{taskId} [delete]
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

// @Summary 停止任务
// @Description 停止指定的任务，注意该动作只是修改了任务状态，实际任务并没有停止
// @version 1.0
// @Security ApiKeyAuth
// @Tags task
// @Accept  json
// @Param taskId query string true "taskId" default(608e9e83-715e-7448-a149-9bef33f38cfe)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":nil}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":nil}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":nil}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":nil}"
// @Success 200 {string} json "{"code":"0000","msg":"success","msg":nil}"
// @Router /api/v2/task/{taskId} [put]
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
