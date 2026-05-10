package controller

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/backup"
)

type DataManageController struct {
	config *config.CKManConfig
	Controller
}

func NewDataManageController(config *config.CKManConfig, wrapfunc Wrapfunc) *DataManageController {
	return &DataManageController{
		config: config,
		Controller: Controller{
			wrapfunc: wrapfunc,
		},
	}
}

// self 返回本实例 host:port 标识，用于立即备份 instance 字段。
func (c *DataManageController) self() string {
	return net.JoinHostPort(c.config.Server.Ip, fmt.Sprint(c.config.Server.Port))
}

// svc 返回包级 backup.Service；若未初始化则通过 wrapfunc 报错并返回 nil。
func (c *DataManageController) svc(ctx *gin.Context) *backup.Service {
	svc := backup.GetService()
	if svc == nil {
		c.wrapfunc(ctx, model.E_DATA_INSERT_FAILED, fmt.Errorf("backup service not initialized"))
	}
	return svc
}

// ============== Backup / Restore 主流程 ==============

// BackupData POST /data_manage/backup/:cluster
// 立即/定时备份；多表展开为多 policy + 多 run。立即备份返回 run_ids；定时备份返回空。
func (c *DataManageController) BackupData(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	var req model.BackupRequest
	if err := model.DecodeRequestBody(ctx.Request, &req); err != nil {
		c.wrapfunc(ctx, model.E_INVALID_PARAMS, err)
		return
	}
	if len(req.Tables) == 0 || len(req.Tables) > 100 {
		c.wrapfunc(ctx, model.E_INVALID_VARIABLE, fmt.Errorf("tables 数量必须在 1-100 之间"))
		return
	}
	// spec #12：立即备份强制绑定到本实例
	if req.ScheduleType == model.BACKUP_IMMEDIATE {
		req.Instance = c.self()
	}
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	runIDs, err := svc.SubmitBackupRequest(cluster, req)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_INSERT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, gin.H{"run_ids": runIDs})
}

// RestoreData POST /data_manage/restore/:cluster
func (c *DataManageController) RestoreData(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	var req backup.RestoreRequest
	if err := model.DecodeRequestBody(ctx.Request, &req); err != nil {
		c.wrapfunc(ctx, model.E_INVALID_PARAMS, err)
		return
	}
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	runID, err := svc.SubmitRestore(cluster, req)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_INSERT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, gin.H{"run_id": runID})
}

// ============== Policy 列表 / 详情 / 编辑 / 删除 ==============

// ListPolicies GET /data_manage/backup/:cluster — 返回 cluster 下 policy 列表
func (c *DataManageController) ListPolicies(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	policies, err := svc.ListPoliciesByCluster(cluster)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, policies)
}

// GetPolicy GET /data_manage/backup/policy/:policy_id
func (c *DataManageController) GetPolicy(ctx *gin.Context) {
	pid := ctx.Param("policy_id")
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	p, err := svc.GetPolicy(pid)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, p)
}

// UpdatePolicy PUT /data_manage/backup/policy/:policy_id
func (c *DataManageController) UpdatePolicy(ctx *gin.Context) {
	pid := ctx.Param("policy_id")
	var p model.BackupPolicy
	if err := model.DecodeRequestBody(ctx.Request, &p); err != nil {
		c.wrapfunc(ctx, model.E_INVALID_PARAMS, err)
		return
	}
	p.PolicyID = pid // path param 优先
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	if err := svc.UpdatePolicy(p); err != nil {
		c.wrapfunc(ctx, model.E_INVALID_VARIABLE, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, nil)
}

// DeletePolicy DELETE /data_manage/backup/policy/:policy_id
func (c *DataManageController) DeletePolicy(ctx *gin.Context) {
	pid := ctx.Param("policy_id")
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	if err := svc.DeletePolicy(pid); err != nil {
		c.wrapfunc(ctx, model.E_DATA_DELETE_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, nil)
}

// TriggerPolicy POST /data_manage/backup/policy/:policy_id/trigger
// 手动触发一次立即执行，不论 schedule_type。
func (c *DataManageController) TriggerPolicy(ctx *gin.Context) {
	pid := ctx.Param("policy_id")
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	runID, err := svc.TriggerPolicy(pid)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_INSERT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, gin.H{"run_id": runID})
}

// ============== Run 详情 + 台账 ==============

// GetRun GET /data_manage/backup/run/:run_id
func (c *DataManageController) GetRun(ctx *gin.Context) {
	rid := ctx.Param("run_id")
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	r, err := svc.GetRun(rid)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, r)
}

// ListRunsByPolicy GET /data_manage/backup/policy/:policy_id/runs?limit=&before=
func (c *DataManageController) ListRunsByPolicy(ctx *gin.Context) {
	pid := ctx.Param("policy_id")
	limit, _ := strconv.Atoi(ctx.DefaultQuery("limit", "30"))
	var before time.Time
	if b := ctx.Query("before"); b != "" {
		if t, err := time.Parse(time.RFC3339, b); err == nil {
			before = t
		}
	}
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	runs, err := svc.ListRunsByPolicy(pid, limit, before)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, runs)
}

// ListRunsByTable GET /data_manage/backup/table/:cluster/:database/:table/runs?days=
func (c *DataManageController) ListRunsByTable(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	database := ctx.Param("database")
	table := ctx.Param("table")
	days, _ := strconv.Atoi(ctx.DefaultQuery("days", "30"))
	if days <= 0 {
		days = 30
	}
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	runs, err := svc.ListRunsByTable(cluster, database, table, days)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, runs)
}

// GetTablePartitionSummary GET /data_manage/tables/:cluster/:database/summary
// 返回 cluster.database 下所有 MergeTree 系列表的分区信息与大小，供前端选表时 batch 缓存。
func (c *DataManageController) GetTablePartitionSummary(ctx *gin.Context) {
	cluster := ctx.Param(ClickHouseClusterPath)
	database := ctx.Param("database")
	svc := c.svc(ctx)
	if svc == nil {
		return
	}
	infos, err := svc.GetTablePartitionSummary(cluster, database)
	if err != nil {
		c.wrapfunc(ctx, model.E_DATA_SELECT_FAILED, err)
		return
	}
	c.wrapfunc(ctx, model.E_SUCCESS, infos)
}
