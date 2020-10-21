package controller

import (
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/config"
	_ "gitlab.eoitek.net/EOI/ckman/docs"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
)

const (
	ClickHouseDefaultEngine  string = "ReplacingMergeTree"
	ClickHouseDefaultColumun string = "_timestamp"
)

type ClickHouseController struct {
	config    *config.CKManConfig
	ckService *clickhouse.CkService
}

func NewClickHouseController(config *config.CKManConfig, ckService *clickhouse.CkService) *ClickHouseController {
	ck := &ClickHouseController{}
	ck.config = config
	ck.ckService = ckService
	return ck
}

// @Summary 创建表
// @Description 创建表
// @version 1.0
// @Param req body model.CreateCkTableReq true "request body"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5001,"msg":"创建ClickHouse表失败","data":""}"
// @Router /api/v1/ck/table [post]
func (ck *ClickHouseController) CreateTable(c *gin.Context) {
	var req model.CreateCkTableReq
	var params model.CreateCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	params.Name = req.Name
	params.DB = ck.config.ClickHouse.DB
	params.Cluster = ck.config.ClickHouse.Cluster
	params.Engine = ClickHouseDefaultEngine
	params.Fields = req.Fields
	params.Order = req.Order
	params.Partition = req.Partition
	if err := ck.ckService.CreateTable(&params); err != nil {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, model.GetMsg(model.CREAT_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 修改表
// @Description 修改表
// @version 1.0
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5003,"msg":"更改ClickHouse表失败","data":""}"
// @Router /api/v1/ck/table [put]
func (ck *ClickHouseController) AlterTable(c *gin.Context) {
	var req model.AlterCkTableReq
	var params model.AlterCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	params.Name = req.Name
	params.DB = ck.config.ClickHouse.DB
	params.Cluster = ck.config.ClickHouse.Cluster
	params.Add = req.Add
	params.Drop = req.Drop
	params.Modify = req.Modify
	if err := ck.ckService.AlterTable(&params); err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, model.GetMsg(model.ALTER_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 删除表
// @Description 删除表
// @version 1.0
// @Param tableName query string true "table name"
// @Success 200 {string} json "{"code":200,"msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":5002,"msg":"删除ClickHouse表失败","data":""}"
// @Router /api/v1/ck/table [delete]
func (ck *ClickHouseController) DeleteTable(c *gin.Context) {
	var params model.DeleteCkTableParams

	params.Name = c.Query("tableName")
	params.Cluster = ck.config.ClickHouse.Cluster
	if err := ck.ckService.DeleteTable(&params); err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, model.GetMsg(model.DELETE_CK_TABLE_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}
