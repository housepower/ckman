package controller

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"gitlab.eoitek.net/EOI/ckman/deploy"
	_ "gitlab.eoitek.net/EOI/ckman/docs"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"gitlab.eoitek.net/EOI/ckman/service/clickhouse"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

const (
	ClickHouseClusterPath string = "clusterName"
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
// @Param req body model.CkImportConfig true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5042,"msg":"导入ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/cluster [post]
func (ck *ClickHouseController) ImportCk(c *gin.Context) {
	var req model.CkImportConfig
	var conf model.CKManClickHouseConfig

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

	err := clickhouse.GetCkClusterConfig(req, &conf)
	if err != nil {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, model.GetMsg(model.IMPORT_CK_CLUSTER_FAIL), err.Error())
		return
	}

	conf.Mode = model.CkClusterImport
	clickhouse.CkConfigFillDefault(&conf)
	clickhouse.CkClusters.Store(req.Cluster, conf)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 更新ClickHouse集群
// @Description 更新ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.CkImportConfig true "request body"
// @Failure 200 {string} json "{"code":400,"msg":"请求参数错误","data":""}"
// @Failure 200 {string} json "{"code":5043,"msg":"更新ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"ok","data":null}"
// @Router /api/v1/ck/cluster [put]
func (ck *ClickHouseController) UpdateCk(c *gin.Context) {
	var req model.CkImportConfig
	var conf model.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	con, ok := clickhouse.CkClusters.Load(req.Cluster)
	if !ok {
		model.WrapMsg(c, model.UPDATE_CK_CLUSTER_FAIL, model.GetMsg(model.UPDATE_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", req.Cluster))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err := clickhouse.GetCkClusterConfig(req, &conf)
	if err != nil {
		model.WrapMsg(c, model.UPDATE_CK_CLUSTER_FAIL, model.GetMsg(model.UPDATE_CK_CLUSTER_FAIL), err.Error())
		return
	}

	clickhouse.CkConfigFillDefault(&conf)
	clickhouse.CkClusters.Store(req.Cluster, conf)
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
	clustersMap := make(map[string]model.CKManClickHouseConfig)
	clickhouse.CkClusters.Range(func(k, v interface{}) bool {
		clustersMap[k.(string)] = v.(model.CKManClickHouseConfig)
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
	params.Fields = req.Fields
	params.Order = req.Order
	params.Partition = req.Partition
	if ckService.Config.IsReplica {
		params.Engine = model.ClickHouseReplicaDefaultEngine
	} else {
		params.Engine = model.ClickHouseDefaultEngine
	}
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

// @Summary 升级ClickHouse集群
// @Description 升级ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param packageVersion query string true "package version" default(20.8.5.45)
// @Failure 200 {string} json "{"code":5060,"msg":"升级ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/upgrade/{clusterName} [put]
func (ck *ClickHouseController) UpgradeCk(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	packageVersion := c.Query("packageVersion")

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, model.GetMsg(model.UPGRADE_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, model.GetMsg(model.UPGRADE_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.UpgradeCkCluster(&conf, packageVersion)
	if err != nil {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, model.GetMsg(model.UPGRADE_CK_CLUSTER_FAIL), err.Error())
		return
	}

	conf.Version = packageVersion
	clickhouse.CkClusters.Store(clusterName, conf)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 启动ClickHouse集群
// @Description 启动ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":5061,"msg":"启动ClickHouse服务失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/start/{clusterName} [put]
func (ck *ClickHouseController) StartCk(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, model.GetMsg(model.START_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, model.GetMsg(model.START_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.StartCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, model.GetMsg(model.START_CK_CLUSTER_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 停止ClickHouse集群
// @Description 停止ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":5062,"msg":"停止ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/stop/{clusterName} [put]
func (ck *ClickHouseController) StopCk(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(model.STOP_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(model.STOP_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.StopCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(model.STOP_CK_CLUSTER_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 销毁ClickHouse集群
// @Description 销毁ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":5063,"msg":"销毁ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/destroy/{clusterName} [put]
func (ck *ClickHouseController) DestroyCk(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, model.GetMsg(model.DESTROY_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, model.GetMsg(model.DESTROY_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.DestroyCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, model.GetMsg(model.DESTROY_CK_CLUSTER_FAIL), err.Error())
		return
	}
	clickhouse.CkClusters.Delete(clusterName)

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 均衡ClickHouse集群
// @Description 均衡ClickHouse集群
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":5064,"msg":"均衡ClickHouse集群失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/rebalance/{clusterName} [put]
func (ck *ClickHouseController) RebalanceCk(c *gin.Context) {
	args := make([]string, 0)
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(model.REBALANCE_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	var conf model.CKManClickHouseConfig
	conf = con.(model.CKManClickHouseConfig)

	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(model.REBALANCE_CK_CLUSTER_FAIL), err.Error())
		return
	}
	infos, err := ckService.QueryInfo(fmt.Sprintf("SELECT DISTINCT name FROM system.tables WHERE (database = '%s') AND (engine LIKE '%%MergeTree%%')", conf.DB))
	if err != nil {
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(model.REBALANCE_CK_CLUSTER_FAIL), err.Error())
		return
	}

	hosts := make([]string, len(conf.Shards))
	for index, shard := range conf.Shards {
		hosts[index] = shard.Replicas[0].Ip
	}
	num := len(infos)-1
	tables := make([]string, num)
	for i := 0; i < num; i++ {
		tables[i] = infos[i+1][0].(string)
	}

	args = append(args, path.Join(filepath.Dir(os.Args[0]), "rebalancer"))
	args = append(args, fmt.Sprintf("-ch-hosts=%s", strings.Join(hosts, ",")))
	args = append(args, fmt.Sprintf("-ch-all-hosts=%s", strings.Join(conf.Hosts, ",")))
	args = append(args, fmt.Sprintf("-ch-port=%d", conf.Port))
	args = append(args, fmt.Sprintf("-ch-user=%s", conf.User))
	args = append(args, fmt.Sprintf("-ch-password=%s", conf.Password))
	args = append(args, fmt.Sprintf("-ch-tables=%s", strings.Join(tables, ",")))
	args = append(args, fmt.Sprintf("-ch-data-dir=%s", conf.Path))
	args = append(args, fmt.Sprintf("-os-user=%s", conf.SshUser))
	args = append(args, fmt.Sprintf("-os-password=%s", conf.SshPassword))

	cmd := strings.Join(args, " ")
	log.Logger.Infof("run %s", cmd)
	exe := exec.Command("/bin/sh", "-c", cmd)
	if err := exe.Start(); err != nil {
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(model.REBALANCE_CK_CLUSTER_FAIL), err.Error())
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 获取ClickHouse集群信息
// @Description 获取ClickHouse集群信息
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":5065,"msg":"获取ClickHouse集群信息失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":{"status":"green","version":"20.8.5.45","nodes":[{"ip":"192.168.101.105","hostname":"vm101105","status":"green"}]}}"
// @Router /api/v1/ck/get/{clusterName} [get]
func (ck *ClickHouseController) GetCkCluster(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.GET_CK_CLUSTER_INFO_FAIL, model.GetMsg(model.GET_CK_CLUSTER_INFO_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	statusList := clickhouse.GetCkClusterStatus(&conf)

	globalStatus := model.CkStatusGreen
	for index, status := range statusList {
		if status != model.CkStatusGreen {
			for _, shard := range conf.Shards {
				if conf.Hosts[index] == shard.Replicas[0].Ip {
					globalStatus = model.CkStatusRed
					break
				} else {
					globalStatus = model.CkStatusYellow
				}
			}
			if globalStatus == model.CkStatusRed {
				break
			}
		}
	}

	nodes := make([]model.CkClusterNode, len(statusList))
	for index, status := range statusList {
		nodes[index] = model.CkClusterNode{
			Ip:       conf.Hosts[index],
			HostName: conf.Names[index],
			Status:   status,
		}
	}
	info := model.CkClusterInfoRsp{
		Status:  globalStatus,
		Version: conf.Version,
		Nodes:   nodes,
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), info)
}

// @Summary 添加ClickHouse集群节点
// @Description 添加ClickHouse集群节点
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AddNodeReq true "request body"
// @Failure 200 {string} json "{"code":5066,"msg":"添加ClickHouse集群节点失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/node/{clusterName} [post]
func (ck *ClickHouseController) AddNode(c *gin.Context) {
	var req model.AddNodeReq
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(model.INVALID_PARAMS), err.Error())
		return
	}

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.ADD_CK_CLUSTER_NODE_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err := deploy.AddCkClusterNode(&conf, &req)
	if err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.ADD_CK_CLUSTER_NODE_FAIL), err.Error())
		return
	}

	tmp := &model.CKManClickHouseConfig{
		Hosts:    []string{req.Ip},
		Port:     conf.Port,
		Cluster:  conf.Cluster,
		User:     conf.User,
		Password: conf.Password,
	}

	service := clickhouse.NewCkService(tmp)
	if err := service.InitCkService(); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.ADD_CK_CLUSTER_NODE_FAIL), err.Error())
		return
	}
	defer service.Stop()
	if err := service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.ADD_CK_CLUSTER_NODE_FAIL), err.Error())
		return
	}

	clickhouse.CkClusters.Store(clusterName, conf)
	clickhouse.CkServices.Delete(clusterName)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}

// @Summary 删除ClickHouse集群节点
// @Description 删除ClickHouse集群节点
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param ip query string true "node ip address" default(192.168.101.105)
// @Failure 200 {string} json "{"code":5067,"msg":"删除ClickHouse集群节点失败","data":""}"
// @Success 200 {string} json "{"code":200,"msg":"success","data":null}"
// @Router /api/v1/ck/node/{clusterName} [delete]
func (ck *ClickHouseController) DeleteNode(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.DELETE_CK_CLUSTER_NODE_FAIL),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err := deploy.DeleteCkClusterNode(&conf, ip)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL, model.GetMsg(model.DELETE_CK_CLUSTER_NODE_FAIL), err.Error())
		return
	}

	clickhouse.CkClusters.Store(clusterName, conf)
	clickhouse.CkServices.Delete(clusterName)
	clickhouse.MarshalClusters()

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(model.SUCCESS), nil)
}
