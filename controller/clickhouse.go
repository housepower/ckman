package controller

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/deploy"
	_ "github.com/housepower/ckman/docs"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
)

const (
	ClickHouseClusterPath  string = "clusterName"
	ClickHouseSessionLimit int    = 10
)

type ClickHouseController struct {
	Controller
}

func NewClickHouseController(wrapfunc Wrapfunc) *ClickHouseController {
	controller := &ClickHouseController{}
	controller.wrapfunc = wrapfunc
	return controller
}

// @Summary 导入集群
// @Description 将一个已经存在的集群导入到ckman
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.CkImportConfig true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5005","msg":"集群已存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5805","msg":"开启事务失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/cluster [post]
func (controller *ClickHouseController) ImportCluster(c *gin.Context) {
	var req model.CkImportConfig
	var conf model.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if repository.Ps.ClusterExists(req.Cluster) {
		controller.wrapfunc(c, model.E_DATA_DUPLICATED, fmt.Sprintf("cluster %s already exist", req.Cluster))
		return
	}

	conf.Hosts = req.Hosts
	conf.Port = req.Port
	conf.HttpPort = req.HttpPort
	conf.Protocol = req.Protocol
	conf.Secure = req.Secure
	conf.Cluster = req.Cluster
	conf.LogicCluster = &req.LogicCluster
	conf.User = req.User
	conf.Password = req.Password
	conf.ZkNodes = req.ZkNodes
	conf.ZkPort = req.ZkPort
	conf.PromHost = req.PromHost
	conf.PromPort = req.PromPort
	conf.AuthenticateType = model.SshPasswordNotSave
	conf.Mode = model.CkClusterImport
	conf.Normalize()

	if req.LogicCluster != "" {
		logics, err := repository.Ps.GetLogicClusterbyName(req.LogicCluster)
		if err == nil {
			for _, cn := range logics {
				clus, err := repository.Ps.GetClusterbyName(cn)
				if err != nil {
					controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
					return
				}
				if clus.Mode == model.CkClusterDeploy {
					controller.wrapfunc(c, model.E_DATA_DUPLICATED, fmt.Sprintf("logic %s has cluster %s which is depolyed", req.LogicCluster, cn))
					return
				}
			}
		}
	}
	code, err := clickhouse.GetCkClusterConfig(&conf)
	if err != nil {
		controller.wrapfunc(c, code, err)
		return
	}

	if err = repository.Ps.Begin(); err != nil {
		controller.wrapfunc(c, model.E_TRANSACTION_DEGIN_FAILED, err)
		return
	}
	if err = repository.Ps.CreateCluster(conf); err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		_ = repository.Ps.Rollback()
		return
	}

	//logic cluster
	if req.LogicCluster != "" {
		physics, err := repository.Ps.GetLogicClusterbyName(req.LogicCluster)
		if err != nil {
			if errors.Is(err, repository.ErrRecordNotFound) {
				physics = []string{req.Cluster}
				if err1 := repository.Ps.CreateLogicCluster(req.LogicCluster, physics); err1 != nil {
					controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err1)
					_ = repository.Ps.Rollback()
					return
				}
			} else {
				controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
				_ = repository.Ps.Rollback()
				return
			}
		} else {
			physics = append(physics, req.Cluster)
			if err2 := repository.Ps.UpdateLogicCluster(req.LogicCluster, physics); err2 != nil {
				controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err2)
				_ = repository.Ps.Rollback()
				return
			}
		}
	}
	_ = repository.Ps.Commit()

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除集群
// @Description 删除一个集群，但是并没有销毁该集群
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Failure 200 {string} json "{"code":"5800","msg":"数据查询失败","data":""}"
// @Router /api/v2/ck/cluster/{clusterName} [delete]
func (controller *ClickHouseController) DeleteCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	common.CloseConns(conf.Hosts)

	if err = repository.Ps.Begin(); err != nil {
		controller.wrapfunc(c, model.E_TRANSACTION_DEGIN_FAILED, err)
		return
	}
	if err = repository.Ps.DeleteCluster(clusterName); err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		_ = repository.Ps.Rollback()
		return
	}

	if conf.LogicCluster != nil {
		if err = deploy.ClearLogicCluster(conf.Cluster, *conf.LogicCluster, false); err != nil {
			controller.wrapfunc(c, model.E_CONFIG_FAILED, err)
			_ = repository.Ps.Rollback()
			return
		}
	}
	_ = repository.Ps.Commit()

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 获取指定的集群信息
// @Description 根据集群名获取集群的信息
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5804","msg":"查询数据失败","data":null}"
// @Success 200 {string} json "{"code":"0000","msg":"ok", "data":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}"
// @Router /api/v2/ck/cluster/{clusterName} [get]
func (controller *ClickHouseController) GetCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	var cluster model.CKManClickHouseConfig
	cluster, err = repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if cluster.Mode == model.CkClusterImport {
		_, _ = clickhouse.GetCkClusterConfig(&cluster)
	}
	cluster.Normalize()
	cluster.Pack()
	controller.wrapfunc(c, model.E_SUCCESS, cluster)
}

// @Summary 获取所有集群信息
// @Description 获取所有集群的信息
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Success 200 {string} json "{"code":"0000","msg":"ok", "data":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":null}"
// @Router /api/v2/ck/cluster [get]
func (controller *ClickHouseController) GetClusters(c *gin.Context) {
	var err error

	clusters, err := repository.Ps.GetAllClusters()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	for key, cluster := range clusters {
		if cluster.Mode == model.CkClusterImport {
			if _, err = clickhouse.GetCkClusterConfig(&cluster); err != nil {
				log.Logger.Warnf("get import cluster failed:%v", err)
				// delete(clusters, key)
				// continue
			}
		}
		cluster.Normalize()
		cluster.Pack()
		clusters[key] = cluster
	}

	controller.wrapfunc(c, model.E_SUCCESS, clusters)
}

// @Summary 创建表
// @Description 在集群上创建本地表和分布式表
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.CreateCkTableReq true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":null}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":null}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":null}"
// @Failure 200 {string} json "{"code":"5808","msg":"创建表失败","data":null}"
// @Failure 200 {string} json "{"code":"5810","msg":"删除表失败","data":null}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":null}"
// @Failure 200 {string} json "{"code":"5120","msg":"同步zookeeper失败","data":null}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":null}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/table/{clusterName} [post]
func (controller *ClickHouseController) CreateTable(c *gin.Context) {
	var req model.CreateCkTableReq
	var params model.CreateCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	params.Name = req.Name
	params.DistName = req.DistName
	params.DB = req.DB
	params.Cluster = ckService.Config.Cluster
	params.Fields = req.Fields
	params.Order = req.Order
	params.Partition = req.Partition
	if ckService.Config.IsReplica {
		if req.Distinct {
			params.Engine = model.ClickHouseReplicaReplacingEngine
		} else {
			params.Engine = model.ClickHouseDefaultReplicaEngine
		}
	} else {
		if req.Distinct {
			params.Engine = model.ClickHouseReplacingEngine
		} else {
			params.Engine = model.ClickHouseDefaultEngine
		}
	}
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if len(req.TTL) > 0 {
		express, err := genTTLExpress(req.TTL, conf.Storage)
		if err != nil {
			controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
			return
		}
		params.TTLExpr = strings.Join(express, ",")
	}

	if req.StoragePolicy != "" {
		if conf.Storage == nil {
			controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Sprintf("cluster %s can't find storage_policy %s", clusterName, req.StoragePolicy))
			return
		}
		found := false
		for _, policy := range conf.Storage.Policies {
			if policy.Name == req.StoragePolicy {
				found = true
				break
			}
		}
		if !found {
			controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, fmt.Sprintf("cluster %s can't find storage_policy %s", clusterName, req.StoragePolicy))
			return
		}
		params.StoragePolicy = req.StoragePolicy
	}

	if len(req.Indexes) > 0 {
		for _, index := range req.Indexes {
			idx := fmt.Sprintf("INDEX %s %s TYPE %s GRANULARITY %d", index.Name, index.Field, index.Type, index.Granularity)
			params.IndexExpr += "," + idx
		}
	}

	if common.CompareClickHouseVersion(conf.Version, "21.8") > 0 {
		params.Projections = req.Projections
	}
	local, dist, err := common.GetTableNames(ckService.Conn, params.DB, params.Name, params.DistName, params.Cluster, false)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}
	params.Name = local
	params.DistName = dist

	if req.ForceCreate {
		err := clickhouse.DropTableIfExists(params, ckService)
		if err != nil {
			controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
			return
		}
	}
	statements, err := ckService.CreateTable(&params, req.DryRun)
	if err != nil {
		controller.wrapfunc(c, model.E_TBL_CREATE_FAILED, err)
		return
	}

	if req.DryRun {
		controller.wrapfunc(c, model.E_SUCCESS, statements)
	} else {
		controller.wrapfunc(c, model.E_SUCCESS, nil)
	}
}

// @Summary 创建逻辑表
// @Description 创建逻辑表，如果某个物理集群上没有本地表，会同步创建
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(logic_test)
// @Param req body model.DistLogicTableReq true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":null}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5808","msg":"创建表失败","data":null}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/dist-logic-table/{clusterName} [post]
func (controller *ClickHouseController) CreateDistTableOnLogic(c *gin.Context) {
	var req model.DistLogicTableReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, clusterName)
		return
	}
	if conf.LogicCluster == nil {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Sprintf("cluster %s not belong any logic cluster", clusterName))
		return
	}
	physics, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("logic cluster %s is not exist", *conf.LogicCluster))
		return
	}

	for _, cluster := range physics {
		ckService, err := clickhouse.GetCkService(cluster)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
		params := model.DistLogicTblParams{
			Database:     req.Database,
			TableName:    req.LocalTable,
			DistName:     req.DistTable,
			ClusterName:  cluster,
			LogicCluster: *conf.LogicCluster,
		}
		if err = ckService.CreateDistTblOnLogic(&params); err != nil {
			if common.ExceptionAS(err, common.UNKNOWN_TABLE) && cluster != conf.Cluster {
				log.Logger.Warnf("table %s.%s not exist on cluster %s, should create it", params.Database, params.TableName, cluster)
				//means local table is not exist, will auto sync schema
				con, err := repository.Ps.GetClusterbyName(cluster)
				if err == nil {
					// conf is current cluster, we believe that local table must be exist
					clickhouse.SyncLogicTable(conf, con, req.Database, req.LocalTable)
					continue
				}
			}
			controller.wrapfunc(c, model.E_TBL_CREATE_FAILED, err)
			return
		}
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除逻辑表
// @Description 删除逻辑表
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(logic_test)
// @Param req body model.DistLogicTableReq true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5810","msg":"删除表失败","data":null}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/dist-logic-table/{clusterName} [delete]
func (controller *ClickHouseController) DeleteDistTableOnLogic(c *gin.Context) {
	var req model.DistLogicTableReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, clusterName)
		return
	}
	if conf.LogicCluster == nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Sprintf("cluster %s not belong any logic cluster", clusterName))
		return
	}
	physics, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("logic cluster %s is not exist", *conf.LogicCluster))
		return
	}

	for _, cluster := range physics {
		ckService, err := clickhouse.GetCkService(cluster)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
		params := model.DistLogicTblParams{
			Database:     req.Database,
			TableName:    req.LocalTable,
			DistName:     req.DistTable,
			ClusterName:  cluster,
			LogicCluster: *conf.LogicCluster,
		}
		if err = ckService.DeleteDistTblOnLogic(&params); err != nil {
			controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
			return
		}
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 清空表
// @Description 清空表数据
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5803","msg":"数据删除失败","data":null}"
// @Router /api/v2/ck/truncate-table/{clusterName} [delete]
func (controller *ClickHouseController) TruncateTable(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, err)
		return
	}
	hosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	database := c.Query("database")
	table := c.Query("tableName")
	query := fmt.Sprintf("TRUNCATE TABLE IF EXISTS `%s`.`%s` SETTINGS alter_sync = 0", database, table)
	var lastErr error
	var wg sync.WaitGroup
	for _, host := range hosts {
		host := host
		wg.Add(1)
		common.Pool.Submit(func() {
			defer wg.Done()
			conn := common.GetConnection(host)
			if conn == nil {
				log.Logger.Errorf("[%s] get connection failed", host)
				return
			}
			log.Logger.Debugf("[%s]%s", host, query)
			err = conn.Exec(query)
			if err != nil {
				lastErr = err
				return
			}
		})
	}
	wg.Wait()
	if lastErr != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, lastErr)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 修改表
// @Description 修改表schema
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5809","msg":"修改表失败","data":null}"
// @Router /api/v2/ck/table/{clusterName} [put]
func (controller *ClickHouseController) AlterTable(c *gin.Context) {
	var req model.AlterCkTableReq
	var params model.AlterCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = req.Name
	params.DistName = req.DistName
	params.DB = req.DB
	params.Add = req.Add
	params.Drop = req.Drop
	params.Modify = req.Modify
	params.Rename = req.Rename
	params.AddIndex = req.AddIndex
	params.DropIndex = req.DropIndex
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	if common.CompareClickHouseVersion(conf.Version, "21.8") > 0 {
		params.Projections = req.Projections
	}

	if err := ckService.AlterTable(&params); err != nil {
		controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 更新/删除表中的数据
// @Description 更新/删除表中的数据
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.DMLOnLogicReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5809","msg":"修改表失败","data":null}"
// @Router /api/v2/ck/table/dml/{clusterName} [post]
func (controller *ClickHouseController) DMLOnLogic(c *gin.Context) {
	var req model.DMLOnLogicReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	clusterName := c.Param(ClickHouseClusterPath)
	cluster, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, err)
		return
	}

	logics, err := repository.Ps.GetLogicClusterbyName(*cluster.LogicCluster)

	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, err)
		return
	}

	if req.Manipulation != model.DML_DELETE && req.Manipulation != model.DML_UPDATE {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, errors.New("manipulation is invalid"))
		return
	}

	if req.Manipulation == model.DML_UPDATE && len(req.KV) == 0 {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, errors.New("kv is empty"))
		return
	}

	if err := clickhouse.DMLOnLogic(logics, req); err != nil {
		controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 修改表TTL
// @Description 修改表TTL
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterTblsTTLReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":null}"
// @Failure 200 {string} json "{"code":"5809","msg":"修改表失败","data":null}"
// @Router /api/v2/ck/table/ttl/{clusterName} [put]
func (controller *ClickHouseController) AlterTableTTL(c *gin.Context) {
	var req model.AlterTblsTTLReq

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	if req.TTLType != model.TTLTypeModify && req.TTLType != model.TTLTypeRemove {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Sprintf("unsupported type:%s", req.TTLType))
		return
	}
	if len(req.TTL) > 0 {
		express, err := genTTLExpress(req.TTL, conf.Storage)
		if err != nil {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
			return
		}
		req.TTLExpr = strings.Join(express, ",")
	}

	if err := ckService.AlterTableTTL(&req); err != nil {
		controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 修复表只读状态
// @Description 恢复表只读状态
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5814","msg":"恢复表失败","data":""}"
// @Router /api/v2/ck/table/readonly/{clusterName} [put]
func (controller *ClickHouseController) RestoreReplica(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	table := c.Query("table")
	tbls := strings.SplitN(table, ".", 2)
	if len(tbls) != 2 {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Sprintf("table %s is invalid", table))
		return
	}
	database := tbls[0]
	tblName := tbls[1]

	for _, host := range conf.Hosts {
		if err := clickhouse.RestoreReplicaTable(&conf, host, database, tblName); err != nil {
			controller.wrapfunc(c, model.E_TBL_RESTORE_FAILED, err)
			return
		}
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 修改表order by字段
// @Description 修改表的order by字段，顺序
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.OrderbyReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5809","msg":"修改表失败","data":""}"
// @Router /api/v2/ck/table/orderby/{clusterName} [put]
func (controller *ClickHouseController) SetOrderby(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var req model.OrderbyReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if len(req.Orderby) == 0 && reflect.DeepEqual(req.Partitionby, model.PartitionInfo{}) {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Errorf("both order by and partition by are empty"))
		return
	}

	err = clickhouse.SetTableOrderBy(&conf, req)
	if err != nil {
		controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 创建/修改物化视图
// @Description 创建/修改物化视图
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.MaterializedViewReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5809","msg":"修改表失败","data":""}"
// @Router /api/v2/ck/table/view/{clusterName} [put]
func (controller *ClickHouseController) MaterializedView(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var req model.MaterializedViewReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if req.Operate != model.OperateCreate && req.Operate != model.OperateDelete {
		err := fmt.Errorf("operate only supports create and delete")
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}
	statement, err := clickhouse.MaterializedView(&conf, req)
	if err != nil {
		controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, statement)
}

// @Summary 使用groupUniqArray创建本地聚合表，本地物化视图，分布式聚合表，分布式视图
// @Description 使用groupUniqArray创建本地聚合表，本地物化视图，分布式聚合表，分布式视图
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.GroupUniqArrayReq true "request body"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5808","msg":"创建表失败","data":""}"
// @Router /api/v2/ck/table/group-uniq-array/{clusterName} [post]
func (controller *ClickHouseController) GroupUniqArray(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var req model.GroupUniqArrayReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	if err := clickhouse.GroupUniqArray(&conf, req); err != nil {
		controller.wrapfunc(c, model.E_TBL_CREATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 查询groupUniqArray聚合视图
// @Description 查询groupUniqArray聚合视图，拿到指定
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Router /api/v2/ck/table/group-uniq-array/{clusterName} [get]
func (controller *ClickHouseController) GetGroupUniqArray(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	table := c.Query("table")
	tbls := strings.SplitN(table, ".", 2)
	if len(tbls) != 2 {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Sprintf("table %s is invalid", table))
		return
	}
	database := tbls[0]
	tblName := tbls[1]
	result, err := clickhouse.GetGroupUniqArray(&conf, database, tblName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, result)
}

// @Summary DelGroupUniqArray
// @Description Delete Materialized View with groupUniqArray
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5810","msg":"删除表失败","data":""}"
// @Router /api/v2/ck/table/group-uniq-array/{clusterName} [delete]
func (controller *ClickHouseController) DelGroupUniqArray(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	table := c.Query("table")
	tbls := strings.SplitN(table, ".", 2)
	if len(tbls) != 2 {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Sprintf("table %s is invalid", table))
		return
	}
	database := tbls[0]
	tblName := tbls[1]
	err = clickhouse.DelGroupUniqArray(&conf, database, tblName)
	if err != nil {
		controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除指定表及其所有关联表，包括本地表，分布式表，逻辑表，物化视图， 本地聚合表，分布式聚合表，逻辑聚合表，聚合物化视图
// @Description 删除指定表及其所有关联表，包括本地表，分布式表，逻辑表，物化视图，本地聚合表，分布式聚合表，逻辑聚合表，聚合物化视图
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"code":"5110","msg":"连接clickhouse失败","data":""}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5810","msg":"删除表失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/table-all/{clusterName} [delete]
func (controller *ClickHouseController) DeleteTableAll(c *gin.Context) {
	var params model.DeleteCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	var conf model.CKManClickHouseConfig
	conf, err = repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	if err := ckService.DeleteTable(&conf, &params); err != nil {
		controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
		return
	}

	physics, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, fmt.Sprintf("logic cluster %s is not exist", *conf.LogicCluster))
		return
	}

	for _, cluster := range physics {
		ckService, err := clickhouse.GetCkService(cluster)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
		params := model.DistLogicTblParams{
			Database:     params.DB,
			TableName:    params.Name,
			ClusterName:  cluster,
			LogicCluster: *conf.LogicCluster,
		}
		if err = ckService.DeleteDistTblOnLogic(&params); err != nil {
			controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
			return
		}
	}

	if err := clickhouse.DelGroupUniqArray(&conf, params.DB, params.Name); err != nil {
		controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
		return
	}

	//sync zookeeper path
	err = repository.Ps.UpdateCluster(conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除表
// @Description 删除本地表以及分布式表
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"code":"5110","msg":"连接clickhouse失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5810","msg":"删除表失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":null}"
// @Router /api/v2/ck/table/{clusterName} [delete]
func (controller *ClickHouseController) DeleteTable(c *gin.Context) {
	var params model.DeleteCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	var conf model.CKManClickHouseConfig
	conf, err = repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	if err := ckService.DeleteTable(&conf, &params); err != nil {
		controller.wrapfunc(c, model.E_TBL_DROP_FAILED, err)
		return
	}

	//sync zookeeper path
	err = repository.Ps.UpdateCluster(conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 描述表
// @Description 描述表的schema
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"code":"5110","msg":"连接clickhouse失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"name":"_timestamp","type":"DateTime","defaultType":"","defaultExpression":"","comment":"","codecExpression":"","ttlExpression":""}]}"
// @Router /api/v2/ck/table/{clusterName} [get]
func (controller *ClickHouseController) DescTable(c *gin.Context) {
	var params model.DescCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	atts, err := ckService.DescTable(&params)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, atts)
}

// @Summary 查询SQL
// @Description 查询SQL
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param query query string true "sql" default(show databases)
// @Failure 200 {string} json "{"code":"5110","msg":"连接clickhouse失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[["name"],["default"],["system"]]}"
// @Router /api/v2/ck/query/{clusterName} [get]
func (controller *ClickHouseController) QueryInfo(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	host := c.Query("host")
	query := c.Query("query")
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	var ckService *clickhouse.CkService
	var err error
	if host == "" {
		ckService, err = clickhouse.GetCkService(clusterName)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
	} else {
		ckService, err = clickhouse.GetCkNodeService(clusterName, host)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	//only save success query
	key := fmt.Sprintf("[%s]%s", clusterName, query)
	history := model.QueryHistory{
		CheckSum: common.Md5CheckSum(key),
		Cluster:  clusterName,
		QuerySql: query,
	}
	if _, err := repository.Ps.GetQueryHistoryByCheckSum(history.CheckSum); err != nil {
		//not found, create record
		_ = repository.Ps.CreateQueryHistory(history)
	} else {
		_ = repository.Ps.UpdateQueryHistory(history)
	}

	if repository.Ps.GetQueryHistoryCount() > 100 {
		earliest, err := repository.Ps.GetEarliestQuery()
		if err == nil && earliest.CheckSum != "" {
			_ = repository.Ps.DeleteQueryHistory(earliest.CheckSum)
		}
	}

	controller.wrapfunc(c, model.E_SUCCESS, data)
}

// @Summary 查询SQL
// @Description 查询SQL
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param query query string true "sql" default(show databases)
// @Failure 200 {string} json "{"code":"5110","msg":"连接clickhouse失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[["name"],["default"],["system"]]}"
// @Router /api/v2/ck/query_export/{clusterName} [get]
func (controller *ClickHouseController) QueryExport(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	host := c.Query("host")
	query := c.Query("query")
	query = strings.TrimRight(strings.TrimSpace(query), ";")

	var ckService *clickhouse.CkService
	var err error
	if host == "" {
		ckService, err = clickhouse.GetCkService(clusterName)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
	} else {
		ckService, err = clickhouse.GetCkNodeService(clusterName, host)
		if err != nil {
			controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
			return
		}
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	fileName := fmt.Sprintf("ckman_query_%s_%s.csv", clusterName, time.Now().Format("2006-01-02T15:04:05"))

	buf := &bytes.Buffer{}
	buf.WriteString("\xEF\xBB\xBE")
	writer := csv.NewWriter(buf)
	for _, row := range data {
		var cells []string
		for _, cell := range row {
			cells = append(cells, fmt.Sprintf("%v", cell))
		}
		writer.Write(cells)
	}
	writer.Flush()
	c.Writer.Header().Set("Content-Disposition", "attachment;filename="+fileName)
	c.Data(http.StatusOK, "application/octet-stream", buf.Bytes())
}

// @Summary 升级集群
// @Description 升级集群，支持全量升级和滚动升级
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param req body model.CkUpgradeCkReq true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/upgrade/{clusterName} [put]
func (controller *ClickHouseController) UpgradeCluster(c *gin.Context) {
	var req model.CkUpgradeCkReq
	clusterName := c.Param(ClickHouseClusterPath)

	req.SkipSameVersion = true    // skip the same version default
	req.Policy = model.PolicyFull // use full policy default
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	conf.Version = req.PackageVersion
	var chHosts []string
	if req.SkipSameVersion {
		for _, host := range conf.Hosts {
			version, err := clickhouse.GetCKVersion(&conf, host)
			if err == nil && version == req.PackageVersion {
				continue
			}
			chHosts = append(chHosts, host)
		}
	} else {
		chHosts = conf.Hosts
	}

	if len(chHosts) == 0 {
		err := errors.New("there is nothing to be upgrade")
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}

	d := deploy.NewCkDeploy(conf)
	d.Packages = deploy.BuildPackages(req.PackageVersion, conf.PkgType, conf.Cwd)
	d.Ext.Policy = req.Policy
	d.Ext.CurClusterOnly = true
	d.Conf.Hosts = chHosts

	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKUpgrade, d)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary 启动集群
// @Description 启动集群里所有节点的ck服务，会跳过已经启动的节点
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程执行命令失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/start/{clusterName} [put]
func (controller *ClickHouseController) StartCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	err = deploy.StartCkCluster(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	conf.Watch(model.ALL_NODES_DEFAULT)
	err = repository.Ps.UpdateCluster(conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 停止集群
// @Description 停止集群内所有节点，会跳过已经停止的节点
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程执行命令失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Failure 200 {string} json "{"code":"5120","msg":"同步zookeeper失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/stop/{clusterName} [put]
func (controller *ClickHouseController) StopCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	common.CloseConns(conf.Hosts)
	err = deploy.StopCkCluster(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	conf.UnWatch(model.ALL_NODES_DEFAULT)
	if err = repository.Ps.UpdateCluster(conf); err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 销毁集群
// @Description 销毁集群，删除所有数据，并卸载服务
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/destroy/{clusterName} [put]
func (controller *ClickHouseController) DestroyCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	d := deploy.NewCkDeploy(conf)
	d.Packages = deploy.BuildPackages(conf.Version, conf.PkgType, conf.Cwd)
	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKDestory, d)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary 均衡集群
// @Description 均衡集群，可以按照partition和shardingkey均衡，如果没有指定shardingkey，默认按照
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.RebalanceTableReq true "request body"
// @Param all query string false "if all = true, rebalance all tables, else only rebalance tables witch in requests" default(true)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5809","msg":"均衡集群失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/rebalance/{clusterName} [put]
func (controller *ClickHouseController) RebalanceCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if deploy.HasEffectiveTasks(clusterName) {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, fmt.Sprintf("cluster %s has effective tasks, can't do rebalance", clusterName))
		return
	}

	// if shard == 1, there is no need to rebalance
	if len(conf.Shards) > 1 {
		var req model.RebalanceTableReq
		if c.Request.Body != http.NoBody {
			params, ok := SchemaUIMapping[GET_SCHEMA_UI_REBALANCE]
			if !ok {
				controller.wrapfunc(c, model.E_INVALID_PARAMS, "")
				return
			}
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
				return
			}
			err = params.UnmarshalConfig(string(body), &req)
			if err != nil {
				controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
				return
			}
		}
		allTable := common.TernaryExpression(c.Query("all") == "false", false, true).(bool)
		if req.ExceptMaxShard {
			allTable = true
		}
		err = clickhouse.RebalanceCluster(&conf, req.Keys, allTable, req.ExceptMaxShard)
		if err != nil {
			controller.wrapfunc(c, model.E_TBL_ALTER_FAILED, err)
			return
		}
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 获取集群状态
// @Description 获取集群各个节点的状态
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5005","msg":"集群已存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}}"
// @Router /api/v2/ck/get/{clusterName} [get]
func (controller *ClickHouseController) GetClusterStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if conf.Mode == model.CkClusterImport {
		_, _ = clickhouse.GetCkClusterConfig(&conf)
	}
	conf.Normalize()
	statusList := clickhouse.GetCkClusterStatus(&conf)

	globalStatus := model.CkStatusGreen
	for index, status := range statusList {
		if status.Status != model.CkStatusGreen {
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

	needPassword := false
	if conf.AuthenticateType == model.SshPasswordNotSave {
		needPassword = true
	}

	info := model.CkClusterInfoRsp{
		PkgType:      conf.PkgType,
		Status:       globalStatus,
		Version:      conf.Version,
		Nodes:        statusList,
		Mode:         conf.Mode,
		NeedPassword: needPassword,
		HttpPort:     conf.HttpPort,
	}

	controller.wrapfunc(c, model.E_SUCCESS, info)
}

// @Summary 增加集群节点
// @Description 增加集群节点，可对已有shard增加副本，也可直接增加新的shard
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param req body model.AddNodeReq true "request body"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5005","msg":"节点重复","data":""}"
// @Failure 200 {string} json "{"code":"5002","msg":"数据不匹配","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/node/{clusterName} [post]
func (controller *ClickHouseController) AddNode(c *gin.Context) {
	var req model.AddNodeReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	force := common.TernaryExpression(c.Query("force") == "true", true, false).(bool)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	maxShardNum := len(conf.Shards)
	if !conf.IsReplica && req.Shard != maxShardNum+1 {
		err := errors.Errorf("It's not allow to add replica node for shard%d while IsReplica is false", req.Shard)
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}

	if !conf.IsReplica && len(req.Ips) > 1 {
		err := errors.Errorf("import mode can only add 1 node once")
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}

	// add the node to conf struct
	for _, ip := range req.Ips {
		for _, host := range conf.Hosts {
			if host == ip {
				err := errors.Errorf("node ip %s is duplicate", ip)
				controller.wrapfunc(c, model.E_DATA_DUPLICATED, err)
				return
			}
		}
	}

	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	if len(shards) >= req.Shard {
		for _, ip := range req.Ips {
			replica := model.CkReplica{
				Ip: ip,
			}
			shards[req.Shard-1].Replicas = append(shards[req.Shard-1].Replicas, replica)
		}
	} else if len(shards)+1 == req.Shard {
		var replicas []model.CkReplica
		for _, ip := range req.Ips {
			replica := model.CkReplica{
				Ip: ip,
			}
			replicas = append(replicas, replica)
		}
		shard := model.CkShard{
			Replicas: replicas,
		}
		shards = append(shards, shard)
	} else {
		err := errors.Errorf("shard number %d is incorrect", req.Shard)
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
		return
	}

	// install clickhouse and start service on the new node
	d := deploy.NewCkDeploy(conf)
	d.Conf.Hosts = req.Ips

	d.Packages = deploy.BuildPackages(conf.Version, conf.PkgType, conf.Cwd)
	if reflect.DeepEqual(d.Packages, deploy.Packages{}) {
		err := errors.Errorf("package %s %s not found in localpath", conf.Version, conf.PkgType)
		controller.wrapfunc(c, model.E_DATA_MISMATCHED, err)
		return
	}
	d.Conf.Shards = shards

	if !force {
		if err := common.CheckCkInstance(d.Conf); err != nil {
			controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
			return
		}
	}

	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKAddNode, d)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary 删除节点
// @Description 删除节点
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param ip query string true "node ip address" default(192.168.101.105)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/node/{clusterName} [delete]
func (controller *ClickHouseController) DeleteNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")

	force := common.TernaryExpression(c.Query("force") == "true", true, false).(bool)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	available := false
	deleteShard := false
	for i, shard := range conf.Shards {
		if available {
			break
		}
		for _, replica := range shard.Replicas {
			if replica.Ip == ip {
				available = true
				if len(shard.Replicas) == 1 {
					if i+1 != len(conf.Shards) {
						err = fmt.Errorf("can't delete node which only 1 replica in shard")
					} else {
						deleteShard = true
					}
				}
				break
			}
		}
	}

	if deleteShard && !force {
		query := `SELECT sum(total_bytes)
FROM system.tables
WHERE match(engine, 'MergeTree') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))
SETTINGS skip_unavailable_shards = 1`
		log.Logger.Debugf("[%s]%s", ip, query)
		conn, _ := common.ConnectClickHouse(ip, model.ClickHouseDefaultDB, conf.GetConnOption())
		if conn != nil {
			rows, _ := conn.Query(query)
			for rows.Next() {
				var data uint64
				rows.Scan(&data)
				if data > 0 {
					controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, "The current node still has data, please use rebalance to move the data to another shard at first")
					return
				}
			}
		}
	}

	if !available {
		err = fmt.Errorf("can't find this ip in cluster")
	}

	if err != nil {
		log.Logger.Errorf("can't delete this node: %v", err)
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	d := deploy.NewCkDeploy(conf)
	d.Packages = deploy.BuildPackages(conf.Version, conf.PkgType, conf.Cwd)
	d.Conf.Hosts = []string{ip}

	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKDeleteNode, d)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary 上线节点
// @Description 将集群内的某一个节点上线
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程命令执行失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/node/start/{clusterName} [put]
func (controller *ClickHouseController) StartNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Errorf("node ip does not exist"))
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}
	con := conf
	//copy the original hosts
	var host string
	for _, h := range conf.Hosts {
		if h != ip {
			host = h
			break
		}
	}
	con.Hosts = []string{ip}

	err = deploy.StartCkCluster(&con)
	if err != nil {
		controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	conf.Watch(ip)
	if host != "" {
		ckService := clickhouse.NewCkService(&conf)
		ckService.InitCkService()
		err := ckService.FetchSchemerFromOtherNode(host)
		if err != nil {
			if common.ExceptionAS(err, common.REPLICA_ALREADY_EXISTS) {
				//Code: 253: Replica /clickhouse/tables/XXX/XXX/replicas/{replica} already exists, clean the znode and  retry
				service, err := zookeeper.GetZkService(conf.Cluster)
				if err == nil {
					err = service.CleanZoopath(conf, conf.Cluster, ip, false)
					if err == nil {
						if err = ckService.FetchSchemerFromOtherNode(host); err != nil {
							log.Logger.Errorf("fetch schema from other node failed again")

						}
					} else {
						log.Logger.Errorf("can't create zookeeper instance:%v", err)
					}
				}
			}
		}
		if conf.IsReplica {
			// https://clickhouse.com/docs/en/sql-reference/statements/system/#sync-replica
			// Provides possibility to start background fetches for inserted parts for tables in the ReplicatedMergeTree family: Always returns Ok. regardless of the table engine and even if table or database does not exist.
			query := "SYSTEM START FETCHES"
			ckService.Conn.Exec(query)
		}
	}
	err = repository.Ps.UpdateCluster(conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 下线节点
// @Description 将集群内的某一个节点下线
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程命令执行失败","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/node/stop/{clusterName} [put]
func (controller *ClickHouseController) StopNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Errorf("node ip does not exist"))
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	con := conf
	con.Hosts = []string{ip}

	err = deploy.StopCkCluster(&con)
	if err != nil {
		controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
		return
	}
	conf.UnWatch(ip)
	err = repository.Ps.UpdateCluster(conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 获取clickhouse日志
// @Description 获取clickhouse日志
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.GetLogReq true "request body"
// @Param password query string false "password"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程命令执行失败","data":""}"
// @Failure 200 {string} json "{"code":"5601","msg":"反序列化失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":null}"
// @Router /api/v2/ck/node/log/{clusterName} [post]
func (controller *ClickHouseController) GetLog(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, fmt.Errorf("node ip does not exist"))
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	var req model.GetLogReq
	req.Tail = true
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_UNMARSHAL_FAILED, err)
		return
	}

	var logPath, logName string
	if strings.Contains(conf.PkgType, "tgz") {
		logPath = path.Join(conf.Cwd, "log", "clickhouse-server")
	} else {
		logPath = "/var/log/clickhouse-server"
	}
	if req.LogType == "" || req.LogType == "normal" {
		logName = path.Join(logPath, "clickhouse-server.log")
	} else if req.LogType == "error" {
		logName = path.Join(logPath, "clickhouse-server.err.log")
	}
	var cmd string
	if req.Lines == 0 || req.Lines > 1000 {
		req.Lines = 1000
	}
	if req.Tail {
		cmd = fmt.Sprintf("tail -%d %s", req.Lines, logName)
	} else {
		cmd = fmt.Sprintf("head -%d %s", req.Lines, logName)
	}
	opts := common.SshOptions{
		User:             conf.SshUser,
		Password:         conf.SshPassword,
		Port:             conf.SshPort,
		Host:             ip,
		NeedSudo:         conf.NeedSudo,
		AuthenticateType: conf.AuthenticateType,
	}
	result, err := common.RemoteExecute(opts, cmd)
	if err != nil {
		controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
	}
	controller.wrapfunc(c, model.E_SUCCESS, result)
}

// @Summary 获取表指标
// @Description 获取标指标
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string false "filtered by database, if database is empty, select all databases" default(default)
// @Param columns query string false "return columns, if columns is empty, return all columns" default(columns,partitions,parts,compressed,uncompressed,is_readonly,queries,cost)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"sensor_dt_result_online":{"columns":22,"rows":1381742496,"parts":192,"space":54967700946,"completedQueries":5,"failedQueries":0,"queryCost":{"middle":130,"secondaryMax":160.76,"max":162}}}}"
// @Router /api/v2/ck/table-metric/{clusterName} [get]
func (controller *ClickHouseController) GetTableMetric(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	database := c.Query("database")
	columns := c.Query("columns")
	var cols []string
	if columns != "" {
		cols = strings.Split(columns, ",")
	}

	var gotError bool
	metrics, err := clickhouse.GetCkTableMetrics(&conf, database, cols)
	if err != nil {
		// we do not return error when system.query_log is not exist
		gotError = !common.ExceptionAS(err, common.UNKNOWN_TABLE)
	}

	if gotError {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, metrics)
}

// @Summary 获取表merge指标
// @Description 获取表merge指标
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{"sensor_dt_result_online":{"columns":22,"rows":1381742496,"parts":192,"space":54967700946,"completedQueries":5,"failedQueries":0,"queryCost":{"middle":130,"secondaryMax":160.76,"max":162}}}}"
// @Router /api/v2/ck/table-merges/{clusterName} [get]
func (controller *ClickHouseController) GetTableMerges(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	merges, err := clickhouse.GetCKMerges(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, merges)
}

// @Summary 查询正在进行的会话
// @Description 查询正在进行的会话
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"startTime":1609997894,"queryDuration":1,"query":"SELECT DISTINCT name FROM system.tables","user":"eoi","queryId":"62dce71d-9294-4e47-9d9b-cf298f73233d","address":"192.168.21.73","threads":2}]}"
// @Router /api/v2/ck/open-sessions/{clusterName} [get]
func (controller *ClickHouseController) GetOpenSessions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	limit := ClickHouseSessionLimit
	limitStr := c.Query("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	sessions, err := clickhouse.GetCkOpenSessions(&conf, limit)
	if err != nil {
		// we do not return error when system.query_log is not exist
		gotError = !common.ExceptionAS(err, common.UNKNOWN_TABLE)
	}
	if gotError {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, sessions)
}

// @Summary 终止正在进行的会话
// @Description 终止正在进行的会话
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param host query string false "host"
// @Param query_id query string false "query_id"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5802","msg":"数据更新失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"startTime":1609997894,"queryDuration":1,"query":"SELECT DISTINCT name FROM system.tables","user":"eoi","queryId":"62dce71d-9294-4e47-9d9b-cf298f73233d","address":"192.168.21.73","threads":2}]}"
// @Router /api/v2/ck/open-sessions/{clusterName} [put]
func (controller *ClickHouseController) KillOpenSessions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	host := c.Query("host")
	queryId := c.Query("queryId")
	typ := c.Query("type")

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	err = clickhouse.KillCkOpenSessions(&conf, host, queryId, typ)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_UPDATE_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 查询慢SQL
// @Description 查询慢SQL
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Param start query string false "sessions limit"
// @Param end query string false "sessions limit"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"startTime":1609986493,"queryDuration":145,"query":"select * from dist_sensor_dt_result_online limit 10000","user":"default","queryId":"8aa3de08-92c4-4102-a83d-2f5d88569dab","address":"::1","threads":2}]}"
// @Router /api/v2/ck/slow-sessions/{clusterName} [get]
func (controller *ClickHouseController) GetSlowSessions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	now := time.Now().Unix() // second
	cond := model.SessionCond{
		StartTime: now - 7*24*3600, // 7 days before
		EndTime:   now,
		Limit:     ClickHouseSessionLimit,
	}
	limit := c.Query("limit")
	if limit != "" {
		cond.Limit, _ = strconv.Atoi(limit)
	}
	startTime := c.Query("start")
	if startTime != "" {
		cond.StartTime, _ = strconv.ParseInt(startTime, 10, 64)
	}
	endTime := c.Query("end")
	if endTime != "" {
		cond.EndTime, _ = strconv.ParseInt(endTime, 10, 64)
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	sessions, err := clickhouse.GetCkSlowSessions(&conf, cond)
	if err != nil {
		// we do not return error when system.query_log is not exist
		gotError = !common.ExceptionAS(err, common.UNKNOWN_TABLE)
	}
	if gotError {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, sessions)
}

// @Summary 查询分布式DDL
// @Description 查询分布式DDL
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":[{"startTime":1609986493,"queryDuration":145,"query":"select * from dist_sensor_dt_result_online limit 10000","user":"default","queryId":"8aa3de08-92c4-4102-a83d-2f5d88569dab","address":"::1","threads":2}]}"
// @Router /api/v2/ck/ddl_queue/{clusterName} [get]
func (controller *ClickHouseController) GetDistDDLQueue(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	ddlQueue, err := clickhouse.GetDistibutedDDLQueue(&conf)
	if err != nil {
		// we do not return error when system.query_log is not exist
		gotError = !common.ExceptionAS(err, common.UNKNOWN_TABLE)
	}
	if gotError {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, ddlQueue)
}

// @Summary Ping集群是否健康
// @Description 探测集群是否可以正常对外提供服务
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.PingClusterReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":nil}"
// @Router /api/v2/ck/ping/{clusterName} [post]
func (controller *ClickHouseController) PingCluster(c *gin.Context) {
	var req model.PingClusterReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, "can't find any host")
		return
	}

	shardAvailable := true
	conf.Password = req.Password
	conf.User = req.User
	for _, shard := range conf.Shards {
		failNum := 0
		for _, replica := range shard.Replicas {
			host := replica.Ip
			_, err = common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
			if err != nil {
				log.Logger.Error("err: %v", err)
				failNum++
				continue
			}
		}
		if failNum == len(shard.Replicas) {
			shardAvailable = false
		}
	}

	if !shardAvailable {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 删除表某个时间范围内的数据
// @Description 删除表某个时间范围内的数据
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.PurgerTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5803","msg":"数据删除失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/purge-tables/{clusterName} [post]
func (controller *ClickHouseController) PurgeTables(c *gin.Context) {
	var req model.PurgerTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, errors.Errorf("can't find any host"))
		return
	}

	chHosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	p := clickhouse.NewPurgerRange(chHosts, req.Database, req.Begin, req.End, conf.GetConnOption())
	err = p.InitConns()
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	for _, table := range req.Tables {
		err := p.PurgeTable(table)
		if err != nil {
			controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
			return
		}
	}
	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

// @Summary 获取分区信息
// @Description 获取分区信息
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/partition/{clusterName} [get]
func (controller *ClickHouseController) GetPartitions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	table := c.Query("table")
	if table == "" {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, "table must not be empty")
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	partInfo, err := clickhouse.GetPartitions(&conf, table)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, partInfo)
}

// @Summary 备份表
// @Description 按时间段备份表数据
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5101","msg":"SSH远程命令执行失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/archive/{clusterName} [post]
func (controller *ClickHouseController) ArchiveTable(c *gin.Context) {
	var req model.ArchiveTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, errors.Errorf("can't find any host"))
		return
	}

	ckService := clickhouse.NewCkService(&conf)
	if err := ckService.InitCkService(); err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	for _, table := range req.Tables {
		query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database='%s' AND table='%s' AND is_in_partition_key=1 AND type like 'Date%%'", req.Database, table)
		log.Logger.Debugf("query: %s", query)
		data, err := ckService.QueryInfo(query)
		if err != nil {
			controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
			return
		}
		if len(data) == 1 {
			err := fmt.Errorf("table %s doesn't has any Date/DateTime columns in partition by options", table)
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
			return
		} else if len(data) > 2 {
			err := fmt.Errorf("table %s has multiple Date/DateTime columns in partition by options", table)
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
			return
		}
	}
	if req.Target == model.ArchiveTargetLocal {
		if err := common.EnsurePathNonPrefix([]string{
			path.Join(conf.Path, "clickhouse"),
			req.Local.Path,
		}); err != nil {
			controller.wrapfunc(c, model.E_INVALID_VARIABLE, err)
			return
		}

		if err := checkAccess(req.Local.Path, &conf); err != nil {
			controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
			return
		}

		cmd := `which rsync >/dev/null 2>&1 ;echo $?`
		for _, host := range conf.Hosts {
			output, err := common.RemoteExecute(common.SshOptions{
				User:             conf.SshUser,
				Password:         conf.SshPassword,
				Port:             conf.SshPort,
				Host:             host,
				NeedSudo:         conf.NeedSudo,
				AuthenticateType: conf.AuthenticateType,
			}, cmd)
			if err != nil {
				controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
				return
			}
			if strings.TrimSuffix(output, "\n") != "0" {
				err := errors.Errorf("excute cmd:[%s] on %s failed", cmd, host)
				controller.wrapfunc(c, model.E_SSH_EXECUTE_FAILED, err)
				return
			}
		}
	}

	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKArchive, &req)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary 查看建表语句
// @Description 查看建表语句
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5006","msg":"数据不允许为空","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":"{\"create_table_query\": \"CREATE TABLE default.apache_access_log (`@collectiontime` DateTime, `@hostname` LowCardinality(String), `@ip` LowCardinality(String), `@path` String, `@lineno` Int64, `@message` String, `agent` String, `auth` String, `bytes` Int64, `clientIp` String, `device_family` LowCardinality(String), `httpversion` LowCardinality(String), `ident` String, `os_family` LowCardinality(String), `os_major` LowCardinality(String), `os_minor` LowCardinality(String), `referrer` String, `request` String, `requesttime` Float64, `response` LowCardinality(String), `timestamp` DateTime64(3), `userAgent_family` LowCardinality(String), `userAgent_major` LowCardinality(String), `userAgent_minor` LowCardinality(String), `verb` LowCardinality(String), `xforwardfor` LowCardinality(String)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/default/apache_access_log', '{replica}') PARTITION BY toYYYYMMDD(timestamp) ORDER BY (timestamp, `@hostname`, `@path`, `@lineno`) SETTINGS index_granularity = 8192 │ ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/default/apache_access_log', '{replica}') PARTITION BY toYYYYMMDD(timestamp) ORDER BY (timestamp, `@hostname`, `@path`, `@lineno`) SETTINGS index_granularity = 8192\"}"
// @Router /api/v2/ck/table-schema/{clusterName} [get]
func (controller *ClickHouseController) ShowSchema(c *gin.Context) {
	var schema model.ShowSchemaRsp
	clusterName := c.Param(ClickHouseClusterPath)
	database := c.Query("database")
	tableName := c.Query("tableName")
	if database == "" {
		database = model.ClickHouseDefaultDB
	}
	if tableName == "" {
		controller.wrapfunc(c, model.E_DATA_EMPTY, fmt.Errorf("table name must not be nil"))
		return
	}
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	schema.CreateTableQuery, err = ckService.ShowCreateTable(tableName, database)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, schema)
}

func verifySshPassword(c *gin.Context, conf *model.CKManClickHouseConfig, sshUser, sshPassword string) error {
	if conf.Mode == model.CkClusterImport {
		return fmt.Errorf("not support this operate with import mode")
	}

	if sshUser == "" {
		return fmt.Errorf("sshUser must not be null")
	}

	if conf.AuthenticateType == model.SshPasswordNotSave {
		password := c.Query("password")
		conf.SshPassword = password
	}
	return nil
}

// @Summary  修改集群配置
// @Description 修改集群配置
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5000","msg":"参数不合法","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":nil}"
// @Router /api/v2/ck/config/{clusterName} [post]
func (controller *ClickHouseController) ClusterSetting(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	conf.Cluster = clusterName
	err := DecodeRequestBody(c.Request, &conf, GET_SCHEMA_UI_CONFIG)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	force := common.TernaryExpression(c.Query("force") == "true", true, false).(bool)
	policy := common.TernaryExpression(c.Query("policy") == model.PolicyFull, model.PolicyFull, model.PolicyRolling).(string)
	if err := checkConfigParams(&conf); err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	restart, cc, err := mergeClickhouseConfig(&conf, force)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	d := deploy.NewCkDeploy(conf)
	d.Ext.Restart = restart
	d.Ext.Policy = policy
	d.Ext.ChangeCk = cc
	d.Ext.CurClusterOnly = true
	taskId, err := deploy.CreateNewTask(clusterName, model.TaskTypeCKSetting, d)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

// @Summary  获取集群配置
// @Description 获取集群配置
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5001","msg":"变量不合法","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Failure 200 {string} json "{"code":"5600","msg":"序列化失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":nil}"
// @Router /api/v2/ck/config/{clusterName} [get]
func (controller *ClickHouseController) GetConfig(c *gin.Context) {
	var err error
	var resp model.GetConfigRsp
	params, ok := SchemaUIMapping[GET_SCHEMA_UI_CONFIG]
	if !ok {
		controller.wrapfunc(c, model.E_INVALID_VARIABLE, errors.Errorf("type %s does not registered", GET_SCHEMA_UI_CONFIG))
	}
	clusterName := c.Param(ClickHouseClusterPath)

	cluster, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, nil)
		return
	}
	cluster.Normalize()
	cluster.Pack()
	data, err := params.MarshalConfig(cluster)
	if err != nil {
		controller.wrapfunc(c, model.E_MARSHAL_FAILED, nil)
		return
	}
	resp.Mode = cluster.Mode
	resp.Config = data
	controller.wrapfunc(c, model.E_SUCCESS, resp)
}

// @Summary  获取分布式表List
// @Description 获取所有的分布式表和逻辑表
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5800","msg":"集群不存在","data":""}"
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":{\"default\":{\"dist_centers\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_centers111\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_ckcenters\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_ckcenters2\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_logic_centers\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_logic_centers111\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_logic_ckcenters\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"],\"dist_logic_ckcenters2\":[\"@message\",\"@topic\",\"@@id\",\"@rownumber\",\"@ip\",\"@collectiontime\",\"@hostname\",\"@path\",\"@timestamp\",\"@storageTime\"]}}}"
// @Router /api/v2/ck/table-lists/{clusterName} [get]
func (controller *ClickHouseController) GetTableLists(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_RECORD_NOT_FOUND, clusterName)
		return
	}
	service := clickhouse.NewCkService(&conf)
	if err := service.InitCkService(); err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}
	result, err := service.GetTblLists()
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, result)
}

// @Summary  执行计划
// @Description 查看执行计划
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5110","msg":"clickhouse连接失败","data":""}"
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/query-explain/{clusterName} [get]
func (controller *ClickHouseController) QueryExplain(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	query := c.Query("query")
	query = fmt.Sprintf("EXPLAIN PLAN description = 1, actions = 1 %s", query)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_CH_CONNECT_FAILED, err)
		return
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, data)
}

// @Summary  SQL查询历史
// @Description SQL查询历史
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5804","msg":"数据查询失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/query-history/{clusterName} [get]
func (controller *ClickHouseController) QueryHistory(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	historys, err := repository.Ps.GetQueryHistoryByCluster(clusterName)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_SELECT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, historys)
}

// @Summary  删除查询历史
// @Description 删除查询历史
// @version 1.0
// @Security ApiKeyAuth
// @Tags clickhouse
// @Accept  json
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"code":"5803","msg":"数据删除失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"ok","data":""}"
// @Router /api/v2/ck/query-history/{clusterName} [delete]
func (controller *ClickHouseController) DeleteQuery(c *gin.Context) {
	checksum := c.Query("checksum")
	err := repository.Ps.DeleteQueryHistory(checksum)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_DELETE_FAILED, err)
		return
	}

	controller.wrapfunc(c, model.E_SUCCESS, nil)
}

func checkConfigParams(conf *model.CKManClickHouseConfig) error {
	con, err := repository.Ps.GetClusterbyName(conf.Cluster)
	conf.UnPack(con)
	if err != nil {
		return errors.Errorf("cluster %s is not exist", conf.Cluster)
	}

	if conf.SshUser == "" {
		return errors.Errorf("ssh user must not be empty")
	}

	if conf.LogicCluster != nil {
		if con.LogicCluster != nil {
			if *conf.LogicCluster != *con.LogicCluster {
				return errors.Errorf("not support change logic cluster from one to another")
			}
		}

		logics, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
		if err == nil {
			for _, logic := range logics {
				clus, err1 := repository.Ps.GetClusterbyName(logic)
				if err1 == nil {
					if clus.Password != conf.Password {
						return errors.Errorf("default password %s is diffrent from other logic cluster: cluster %s password %s", conf.Password, logic, clus.Password)
					}
					if clus.Mode == model.CkClusterImport {
						return errors.Errorf("logic cluster %s contains cluster which import, import cluster: %s ", *conf.LogicCluster, clus.Cluster)
					}
				}
			}
		}
	} else {
		if con.LogicCluster != nil {
			return errors.Errorf("can't remove cluster from a logic cluster")
		}
	}

	disks := make([]string, 0)
	disks = append(disks, "default")
	if conf.Storage != nil {
		for _, disk := range conf.Storage.Disks {
			disks = append(disks, disk.Name)
			switch disk.Type {
			case "local":
				if !strings.HasSuffix(disk.DiskLocal.Path, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
				}
				if err := checkAccess(disk.DiskLocal.Path, conf); err != nil {
					return err
				}
			case "hdfs":
				if !strings.HasSuffix(disk.DiskHdfs.Endpoint, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskHdfs.Endpoint))
				}
				if common.CompareClickHouseVersion(con.Version, "21.9") < 0 {
					return errors.Errorf("clickhouse do not support hdfs storage policy while version < 21.9 ")
				}
			case "s3":
				if !strings.HasSuffix(disk.DiskS3.Endpoint, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskS3.Endpoint))
				}
			default:
				return errors.Errorf("unsupport disk type %s", disk.Type)
			}
		}
		for _, policy := range conf.Storage.Policies {
			for _, vol := range policy.Volumns {
				for _, disk := range vol.Disks {
					if !common.ArraySearch(disk, disks) {
						return errors.Errorf("invalid disk in policy %s", disk)
					}
				}
			}
		}
	}

	var profiles []string
	profiles = append(profiles, model.ClickHouseUserProfileDefault)
	if len(conf.UsersConf.Profiles) > 0 {
		for _, profile := range conf.UsersConf.Profiles {
			if common.ArraySearch(profile.Name, profiles) {
				return errors.Errorf("profile %s is duplicate", profile.Name)
			}
			if profile.Name == model.ClickHouseUserProfileDefault {
				return errors.Errorf("profile can't be default")
			}
			profiles = append(profiles, profile.Name)
		}
	}

	var quotas []string
	quotas = append(quotas, model.ClickHouseUserQuotaDefault)
	if len(conf.UsersConf.Quotas) > 0 {
		for _, quota := range conf.UsersConf.Quotas {
			if common.ArraySearch(quota.Name, quotas) {
				return errors.Errorf("quota %s is duplicate", quota.Name)
			}
			if quota.Name == model.ClickHouseUserQuotaDefault {
				return errors.Errorf("quota can't be default")
			}
			quotas = append(quotas, quota.Name)
		}
	}

	var usernames []string
	if len(conf.UsersConf.Users) > 0 {
		for _, user := range conf.UsersConf.Users {
			if common.ArraySearch(user.Name, usernames) {
				return errors.Errorf("username %s is duplicate", user.Name)
			}
			if user.Name == model.ClickHouseDefaultUser {
				return errors.Errorf("username can't be default")
			}
			if user.Name == "" || user.Password == "" {
				return errors.Errorf("username or password can't be empty")
			}
			user.Profile = common.GetStringwithDefault(user.Profile, model.ClickHouseUserProfileDefault)
			if !common.ArraySearch(user.Profile, profiles) {
				return errors.Errorf("profile %s is invalid", user.Profile)
			}
			user.Quota = common.GetStringwithDefault(user.Quota, model.ClickHouseUserQuotaDefault)
			if !common.ArraySearch(user.Quota, quotas) {
				return errors.Errorf("quota %s is invalid", user.Quota)
			}
			usernames = append(usernames, user.Name)
		}
	}

	return nil
}

func mergeClickhouseConfig(conf *model.CKManClickHouseConfig, force bool) (bool, bool, error) {
	restart := false
	config := false
	cluster, err := repository.Ps.GetClusterbyName(conf.Cluster)
	if err != nil {
		return false, false, errors.Errorf("cluster %s is not exist", conf.Cluster)
	}
	conf.UnPack(cluster)
	storageChanged := !reflect.DeepEqual(cluster.Storage, conf.Storage)
	expertChanged := !reflect.DeepEqual(cluster.Expert, conf.Expert)
	userconfChanged := !reflect.DeepEqual(cluster.UsersConf, conf.UsersConf)
	logicChaned := !reflect.DeepEqual(cluster.LogicCluster, conf.LogicCluster)
	zkChanged := !reflect.DeepEqual(cluster.ZkNodes, conf.ZkNodes)
	keeperChanged := !reflect.DeepEqual(cluster.KeeperConf, conf.KeeperConf)

	noChangedFn := func() bool {
		return cluster.Port == conf.Port &&
			cluster.Comment == conf.Comment &&
			cluster.AuthenticateType == conf.AuthenticateType &&
			cluster.SshUser == conf.SshUser &&
			cluster.SshPassword == conf.SshPassword &&
			cluster.SshPort == conf.SshPort &&
			cluster.Password == conf.Password && !storageChanged && !expertChanged &&
			cluster.PromHost == conf.PromHost && cluster.PromPort == conf.PromPort &&
			cluster.ZkPort == conf.ZkPort &&
			!userconfChanged && !logicChaned && !zkChanged && !keeperChanged
	}

	if !force {
		if noChangedFn() {
			return false, false, errors.Errorf("all config are the same, it's no need to update")
		}
	}

	if storageChanged {
		srcDisks := make(common.Map)
		dstDisks := make(common.Map)
		if cluster.Storage != nil {
			for _, disk := range cluster.Storage.Disks {
				srcDisks[disk.Name] = disk
			}
		}
		if conf.Storage != nil {
			for _, disk := range conf.Storage.Disks {
				dstDisks[disk.Name] = disk
			}
		}
		delDisks := srcDisks.Difference(dstDisks).(common.Map)
		if len(delDisks) > 0 {
			// delDisks contains disks which will be deleted
			svr := clickhouse.NewCkService(&cluster)
			if err := svr.InitCkService(); err == nil {
				var disks []string
				for k := range delDisks {
					disks = append(disks, k)
				}
				query := fmt.Sprintf(`SELECT policy_name
				FROM system.storage_policies
				WHERE (policy_name != 'default') AND hasAny(disks, ['%s'])`, strings.Join(disks, "','"))
				log.Logger.Debug(query)
				rows, err := svr.Conn.Query(query)
				if err != nil {
					return false, false, err
				}
				for rows.Next() {
					var policy string
					if err = rows.Scan(&policy); err == nil {
						rows.Close()
						return false, false, fmt.Errorf("disk %v was refrenced by storage_policy %s, can't delete", disks, policy)
					} else {
						rows.Close()
						return false, false, err
					}
				}
				rows.Close()
			} else if !noChangedFn() {
				return false, false, err
			}
		}

		srcPolicy := make(common.Map)
		dstPolicy := make(common.Map)
		if cluster.Storage != nil {
			for _, policy := range cluster.Storage.Policies {
				srcPolicy[policy.Name] = policy
			}
		}
		if conf.Storage != nil {
			for _, policy := range conf.Storage.Policies {
				dstPolicy[policy.Name] = policy
			}
		}
		delPolicy := srcPolicy.Difference(dstPolicy).(common.Map)
		if len(delPolicy) > 0 {
			// delPolicy contains disks which will be deleted
			svr := clickhouse.NewCkService(&cluster)
			if err := svr.InitCkService(); err == nil {
				var policies []string
				for k := range delPolicy {
					policies = append(policies, k)
				}
				query := fmt.Sprintf(`SELECT database, table, storage_policy 
				FROM system.tables
				WHERE storage_policy in ('%s')`, strings.Join(policies, "','"))
				log.Logger.Debug(query)
				rows, err := svr.Conn.Query(query)
				if err != nil {
					return false, false, err
				}
				for rows.Next() {
					var database, table, policy_name string
					if err = rows.Scan(&database, &table, &policy_name); err == nil {
						rows.Close()
						return false, false, fmt.Errorf("storage policy %s was refrenced by table %s.%s, can't delete", policy_name, database, table)
					} else {
						rows.Close()
						return false, false, err
					}
				}
				rows.Close()
			} else if !noChangedFn() {
				return false, false, err
			}
		}

	}

	if userconfChanged {
		if !reflect.DeepEqual(cluster.UsersConf.Profiles, conf.UsersConf.Profiles) ||
			!reflect.DeepEqual(cluster.UsersConf.Quotas, conf.UsersConf.Quotas) {
			restart = true
		}
	}

	if force || cluster.Port != conf.Port ||
		cluster.Password != conf.Password || storageChanged || expertChanged ||
		cluster.ZkPort != conf.ZkPort ||
		userconfChanged || logicChaned || zkChanged || keeperChanged {
		config = true
	}

	// need restart
	if force || cluster.Port != conf.Port || storageChanged || expertChanged || keeperChanged {
		restart = true
	}

	// merge conf
	cluster.Comment = conf.Comment
	cluster.Port = conf.Port
	cluster.AuthenticateType = conf.AuthenticateType
	cluster.SshUser = conf.SshUser
	cluster.SshPassword = conf.SshPassword
	cluster.SshPort = conf.SshPort
	cluster.Password = conf.Password
	cluster.Storage = conf.Storage
	cluster.PromHost = conf.PromHost
	cluster.PromPort = conf.PromPort
	cluster.Expert = conf.Expert
	cluster.UsersConf = conf.UsersConf
	cluster.LogicCluster = conf.LogicCluster
	cluster.ZkPort = conf.ZkPort
	cluster.ZkNodes = conf.ZkNodes
	cluster.KeeperConf = conf.KeeperConf
	if err = common.DeepCopyByGob(conf, cluster); err != nil {
		return false, false, err
	}
	return restart, config, nil
}

func genTTLExpress(ttls []model.CkTableTTL, storage *model.Storage) ([]string, error) {
	var express []string
	for _, ttl := range ttls {
		expr := fmt.Sprintf("toDateTime(`%s`) + toInterval%s(%d) ", ttl.TimeCloumn, strings.Title(strings.ToLower(ttl.Unit)), ttl.Interval)
		if ttl.Action == model.TTLActionDelete {
			expr += ttl.Action
		} else if ttl.Action == model.TTLActionToVolume {
			if storage == nil {
				return express, fmt.Errorf("can't find volume %s in storage policy", ttl.Target)
			}
			found := false
			for _, policy := range storage.Policies {
				for _, vol := range policy.Volumns {
					if vol.Name == ttl.Target {
						found = true
						break
					}
				}
			}
			if !found {
				return express, fmt.Errorf("can't find volume %s in storage policy", ttl.Target)
			}
			expr += " TO VOLUME '" + ttl.Target + "'"
		} else if ttl.Action == model.TTLActionToDisk {
			if storage == nil {
				return express, fmt.Errorf("can't find disk %s in storage policy", ttl.Target)
			}
			found := false
			for _, disk := range storage.Disks {
				if disk.Name == ttl.Target {
					found = true
				}
			}
			if !found {
				return express, fmt.Errorf("can't find disk %s in storage policy", ttl.Target)
			}
			expr += " TO DISK '" + ttl.Target + "'"
		}
		express = append(express, expr)
	}
	return express, nil
}
