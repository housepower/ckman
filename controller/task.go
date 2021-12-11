package controller

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-errors/errors"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"strings"
)

const (
	TaskIdPath = "taskId"
)

type TaskController struct{}

func NewTaskController() *TaskController {
	return &TaskController{}
}

// @Summary GetTaskById
// @Description Get task by taskId
// @version 1.0
// @Security ApiKeyAuth
// @Param taskId query string true "task id"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":nil}"
// @Failure 200 {string} json "{"retCode":"5010","retMsg":"task not exist","entity":nil}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/task/{taskId} [get]
func (t *TaskController) GetTaskStatusById(c *gin.Context) {
	taskId := c.Param(TaskIdPath)
	if taskId == "" {
		err := fmt.Errorf("expect taskId but got null")
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	task, err := repository.Ps.GetTaskbyTaskId(taskId)
	if err != nil {
		model.WrapMsg(c, model.GET_TASK_FAIL, err)
		return
	}
	resp := model.TaskStatusResp{
		TaskId:      task.TaskId,
		ClusterName: task.ClusterName,
		Type:        strings.Split(task.TaskType, ".")[0],
		Option:      model.TaskOptionMap[task.TaskType],
		NodeStatus:  task.NodeStatus,
	}

	model.WrapMsg(c, model.SUCCESS, resp)
}

// @Summary TasksList
// @Description Get all tasklist
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/task/lists [get]
func (t *TaskController) TasksList(c *gin.Context) {
	tasks, err := repository.Ps.GetAllTasks()
	if err != nil {
		model.WrapMsg(c, model.GET_TASK_FAIL, err)
		return
	}
	var resps []model.TaskResp
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
	model.WrapMsg(c, model.SUCCESS, resps)
}

// @Summary GetRunningTaskCount
// @Description Get running task count
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":3}"
// @Router /api/v1/task/running [get]
func (t *TaskController) GetRunningTaskCount(c *gin.Context) {
	count := repository.Ps.GetEffectiveTaskCount()
	model.WrapMsg(c, model.SUCCESS, count)
}

// @Summary GetRunningTaskCount
// @Description Get running task count
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":3}"
// @Router /api/v1/task/{taskId} [delete]
func (t *TaskController) DeleteTask(c *gin.Context) {
	taskId := c.Param(TaskIdPath)
	if taskId == "" {
		err := fmt.Errorf("expect taskId but got null")
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	task, err := repository.Ps.GetTaskbyTaskId(taskId)
	if err != nil {
		model.WrapMsg(c, model.DELETE_TASK_FAIL, err)
		return
	}
	if task.Status == model.TaskStatusRunning || task.Status == model.TaskStatusWaiting {
		err := errors.New("can't delete waiting or running task")
		model.WrapMsg(c, model.DELETE_TASK_FAIL, err)
		return
	}

	if err := repository.Ps.DeleteTask(taskId); err != nil {
		model.WrapMsg(c, model.DELETE_TASK_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}
