package controller

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	_ "gitlab.eoitek.net/EOI/ckman/docs"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
)

const (
	ClickHouseDefaultEngine string = "ReplacingMergeTree"
	ClickHouseClusterPath   string = "clusterName"
)

type ClickHouseController struct {
}

func NewClickHouseController() *ClickHouseController {
	ck := &ClickHouseController{}
	return ck
}

// @Summary 导入ClickHouse集群
// @Description 导入ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param req body config.CKManClickHouseConfig true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5042,"msg":"导入ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/cluster [post]
func (ck *ClickHouseController) ImportCk(c *gin.Context) {
	var req config.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	_, ok := clickhouse.CkClusters.Load(req.Cluster)
	if ok {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, model.GetMsg(model.IMPORT_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s already exist", req.Cluster))
		return
	}

	clickhouse.CkConfigFillDefault(&req)
	clickhouse.CkClusters.Store(req.Cluster, req)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 更新ClickHouse集群
// @Description 更新ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param req body config.CKManClickHouseConfig true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5043,"msg":"更新ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/cluster [put]
func (ck *ClickHouseController) UpdateCk(c *gin.Context) {
	var req config.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	_, ok := clickhouse.CkClusters.Load(req.Cluster)
	if !ok {
		model.WrapMsg(c, model.UPDATE_CK_CLUSTER_FAIL, model.GetMsg(model.UPDATE_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", req.Cluster))
		return
	}

	clickhouse.CkConfigFillDefault(&req)
	clickhouse.CkClusters.Store(req.Cluster, req)
	clickhouse.CkServices.Delete(req.Cluster)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 删除ClickHouse集群
// @Description 删除ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/cluster/{clusterName} [delete]
func (ck *ClickHouseController) DeleteCk(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	clickhouse.CkClusters.Delete(clusterName)
	clickhouse.CkServices.Delete(clusterName)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 获取ClickHouse集群
// @Description 获取ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"code":200,"msg":"ok","data":{"test":{"hosts":["192.168.101.105"],"port":9000,"user":"eoi","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.101.102"],"zkPort":2181,"isReplica":false}}}"
// @Router /api/v1/ck/cluster [get]
func (ck *ClickHouseController) GetCk(c *gin.Context) {
	clustersMap := make(map[string]config.CKManClickHouseConfig)
	clickhouse.CkClusters.Range(func(k, v interface{}) bool {
		clustersMap[k.(string)] = v.(config.CKManClickHouseConfig)
		return true
	})

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), clustersMap)
}

// @Summary 创建表
// @Description 创建表
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.CreateCkTableReq true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5001,"msg":"创建ClickHouse表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/table/{clusterName} [post]
func (ck *ClickHouseController) CreateTable(c *gin.Context) {
	var req model.CreateCkTableReq
	var params model.CreateCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, model.GetMsg(model.CREAT_CK_TABLE_FAIL), err.Error())
		return
	}

	params.Name = req.Name
	params.DB = req.DB
	params.Cluster = ckService.Config.Cluster
	params.Engine = ClickHouseDefaultEngine
	params.Fields = req.Fields
	params.Order = req.Order
	params.Partition = req.Partition
	if params.DB == "" {
		params.DB = ckService.Config.DB
	}

	if err := ckService.CreateTable(&params); err != nil {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, model.GetMsg(model.CREAT_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 修改表
// @Description 修改表
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5003,"msg":"更改ClickHouse表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/table/{clusterName} [put]
func (ck *ClickHouseController) AlterTable(c *gin.Context) {
	var req model.AlterCkTableReq
	var params model.AlterCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, model.GetMsg(model.ALTER_CK_TABLE_FAIL), err.Error())
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = req.Name
	params.DB = req.DB
	params.Add = req.Add
	params.Drop = req.Drop
	params.Modify = req.Modify
	if params.DB == "" {
		params.DB = ckService.Config.DB
	}

	if err := ckService.AlterTable(&params); err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, model.GetMsg(model.ALTER_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 删除表
// @Description 删除表
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"code":5002,"msg":"删除ClickHouse表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/table/{clusterName} [delete]
func (ck *ClickHouseController) DeleteTable(c *gin.Context) {
	var params model.DeleteCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, model.GetMsg(model.DELETE_CK_TABLE_FAIL), err.Error())
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = ckService.Config.DB
	}

	if err := ckService.DeleteTable(&params); err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, model.GetMsg(model.DELETE_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 描述表
// @Description 描述表
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"code":5040,"msg":"描述ClickHouse表失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":[{"name":"_timestamp","type":"DateTime","defaultType":"","defaultExpression":"","comment":"","codecExpression":"","ttlExpression":""}]}"
// @Router /api/v1/ck/table/{clusterName} [get]
func (ck *ClickHouseController) DescTable(c *gin.Context) {
	var params model.DescCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, model.GetMsg(model.DESC_CK_TABLE_FAIL), err.Error())
		return
	}

	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = ckService.Config.DB
	}

	atts, err := ckService.DescTable(&params)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, model.GetMsg(model.DESC_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), atts)
}

// @Summary 执行query命令
// @Description 执行query命令
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param query query string true "sql" default(show databases)
// @Failure 200 {string} json "{"code":5042,"msg":"查询ClickHouse失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":[["name"],["default"],["system"]]}"
// @Router /api/v1/ck/query/{clusterName} [get]
func (ck *ClickHouseController) QueryInfo(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	query := c.Query("query")

	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, model.GetMsg(model.QUERY_CK_FAIL), err.Error())
		return
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, model.GetMsg(model.QUERY_CK_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), data)
}
