package controller

import (
	"database/sql"
	"fmt"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"strconv"

	"github.com/housepower/ckman/service/nacos"

	client "github.com/ClickHouse/clickhouse-go"
	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/deploy"
	_ "github.com/housepower/ckman/docs"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
)

const (
	ClickHouseClusterPath  string = "clusterName"
	ClickHouseSessionLimit int    = 10
)

type ClickHouseController struct {
	nacosClient *nacos.NacosClient
}

func NewClickHouseController(nacosClient *nacos.NacosClient) *ClickHouseController {
	ck := &ClickHouseController{
		nacosClient,
	}
	return ck
}

func (ck *ClickHouseController) syncDownClusters(c *gin.Context) (err error) {
	var data string
	data, err = ck.nacosClient.GetConfig()
	if err != nil {
		model.WrapMsg(c, model.GET_NACOS_CONFIG_FAIL, model.GetMsg(c, model.GET_NACOS_CONFIG_FAIL), err)
		return
	}
	if data != "" {
		var updated bool
		if updated, err = clickhouse.UpdateLocalCkClusterConfig([]byte(data)); err == nil && updated {
			buf, _ := clickhouse.MarshalClusters()
			_ = clickhouse.WriteClusterConfigFile(buf)
		}
	}
	return
}

func (ck *ClickHouseController) syncUpClusters(c *gin.Context) (err error) {
	clickhouse.AddCkClusterConfigVersion()
	buf, _ := clickhouse.MarshalClusters()
	_ = clickhouse.WriteClusterConfigFile(buf)
	err = ck.nacosClient.PublishConfig(string(buf))
	if err != nil {
		model.WrapMsg(c, model.PUB_NACOS_CONFIG_FAIL, model.GetMsg(c, model.PUB_NACOS_CONFIG_FAIL), err)
		return
	}
	return
}

// @Summary Import a ClickHouse cluster
// @Description Import a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.CkImportConfig true "request body"
// @Failure 200 {string} json "{"retCode":5000,"retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":5042,"retMsg":"import ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":null}"
// @Router /api/v1/ck/cluster [post]
func (ck *ClickHouseController) ImportCluster(c *gin.Context) {
	var req model.CkImportConfig
	var conf model.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	_, ok := clickhouse.CkClusters.Load(req.Cluster)
	if ok {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, model.GetMsg(c, model.IMPORT_CK_CLUSTER_FAIL),
			fmt.Sprintf("cluster %s already exist", req.Cluster))
		return
	}

	err := clickhouse.GetCkClusterConfig(req, &conf)
	if err != nil {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, model.GetMsg(c, model.IMPORT_CK_CLUSTER_FAIL), err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(req.Cluster, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Delete a ClickHouse cluster
// @Description Delete a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":null}"
// @Router /api/v1/ck/cluster/{clusterName} [delete]
func (ck *ClickHouseController) DeleteCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Delete(clusterName)
	clickhouse.CkServices.Delete(clusterName)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Get config of a ClickHouse cluster
// @Description Get config of a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5065,"retMsg":"get ClickHouse cluster information failed","entity":null}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok", "entity":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}"
// @Router /api/v1/ck/cluster/{clusterName} [get]
func (ck *ClickHouseController) GetCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	var cluster *model.CKManClickHouseConfig
	clickhouse.CkClusters.Range(func(k, v interface{}) bool {
		switch clus := v.(type) {
		case model.CKManClickHouseConfig:
			if k.(string) == clusterName {
				cluster = &clus
			}
		}
		return true
	})
	if cluster != nil {
		model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), cluster)
	} else {
		model.WrapMsg(c, model.GET_CK_CLUSTER_INFO_FAIL, model.GetMsg(c, model.GET_CK_CLUSTER_INFO_FAIL), nil)
	}
}

// @Summary Get config of all ClickHouse cluster
// @Description Get ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok", "entity":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}"
// @Router /api/v1/ck/cluster [get]
func (ck *ClickHouseController) GetClusters(c *gin.Context) {
	var err error
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clustersMap := make(map[string]model.CKManClickHouseConfig)
	clickhouse.CkClusters.Range(func(k, v interface{}) bool {
		switch clus := v.(type) {
		case model.CKManClickHouseConfig:
			clustersMap[k.(string)] = clus
		}
		return true
	})

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), clustersMap)
}

// @Summary Create Table
// @Description Create Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.CreateCkTableReq true "request body"
// @Failure 200 {string} json "{"retCode":5000,"retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":5001,"retMsg":"create ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":null}"
// @Router /api/v1/ck/table/{clusterName} [post]
func (ck *ClickHouseController) CreateTable(c *gin.Context) {
	var req model.CreateCkTableReq
	var params model.CreateCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, model.GetMsg(c, model.CREAT_CK_TABLE_FAIL), err)
		return
	}

	params.Name = req.Name
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

	if err := ckService.CreateTable(&params); err != nil {
		clickhouse.DropTableIfExists(params, ckService)
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, model.GetMsg(c, model.CREAT_CK_TABLE_FAIL), err)
		return
	}

	//sync zookeeper path
	var conf model.CKManClickHouseConfig
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err = clickhouse.GetReplicaZkPath(&conf)
	if err != nil {
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Alter Table
// @Description Alter Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":nil}"
// @Failure 200 {string} json "{"retCode":5000,"retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":5003,"retMsg":"alter ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":null}"
// @Router /api/v1/ck/table/{clusterName} [put]
func (ck *ClickHouseController) AlterTable(c *gin.Context) {
	var req model.AlterCkTableReq
	var params model.AlterCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, model.GetMsg(c, model.ALTER_CK_TABLE_FAIL), err)
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = req.Name
	params.DB = req.DB
	params.Add = req.Add
	params.Drop = req.Drop
	params.Modify = req.Modify
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	if err := ckService.AlterTable(&params); err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, model.GetMsg(c, model.ALTER_CK_TABLE_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Delete Table
// @Description Delete Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"retCode":5002,"retMsg":"delete ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":null}"
// @Router /api/v1/ck/table/{clusterName} [delete]
func (ck *ClickHouseController) DeleteTable(c *gin.Context) {
	var params model.DeleteCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, model.GetMsg(c, model.DELETE_CK_TABLE_FAIL), err)
		return
	}

	var conf model.CKManClickHouseConfig
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)

	params.Cluster = ckService.Config.Cluster
	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	if err := ckService.DeleteTable(&conf, &params); err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, model.GetMsg(c, model.DELETE_CK_TABLE_FAIL), err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Describe Table
// @Description Describe Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"retCode":5040,"retMsg":"describe ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":[{"name":"_timestamp","type":"DateTime","defaultType":"","defaultExpression":"","comment":"","codecExpression":"","ttlExpression":""}]}"
// @Router /api/v1/ck/table/{clusterName} [get]
func (ck *ClickHouseController) DescTable(c *gin.Context) {
	var params model.DescCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, model.GetMsg(c, model.DESC_CK_TABLE_FAIL), err)
		return
	}

	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	atts, err := ckService.DescTable(&params)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, model.GetMsg(c, model.DESC_CK_TABLE_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), atts)
}

// @Summary Query Info
// @Description Query Info
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param query query string true "sql" default(show databases)
// @Failure 200 {string} json "{"retCode":5042,"retMsg":"query ClickHouse failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":[["name"],["default"],["system"]]}"
// @Router /api/v1/ck/query/{clusterName} [get]
func (ck *ClickHouseController) QueryInfo(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	query := c.Query("query")

	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, model.GetMsg(c, model.QUERY_CK_FAIL), err)
		return
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, model.GetMsg(c, model.QUERY_CK_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), data)
}

// @Summary Upgrade ClickHouse cluster
// @Description Upgrade ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param packageVersion query string true "package version" default(20.8.5.45)
// @Failure 200 {string} json "{"retCode":5060,"retMsg":"upgrade ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/upgrade/{clusterName} [put]
func (ck *ClickHouseController) UpgradeCluster(c *gin.Context) {
	var req model.CkUpgradeCk
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	packageVersion := req.PackageVersion

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, model.GetMsg(c, model.UPGRADE_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.UpgradeCkCluster(&conf, packageVersion)
	if err != nil {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, model.GetMsg(c, model.UPGRADE_CK_CLUSTER_FAIL), err)
		return
	}

	conf.Version = packageVersion
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Start ClickHouse cluster
// @Description Start ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5061,"retMsg":"start ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/start/{clusterName} [put]
func (ck *ClickHouseController) StartCluster(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, model.GetMsg(c, model.START_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err := deploy.StartCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, model.GetMsg(c, model.START_CK_CLUSTER_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Stop ClickHouse cluster
// @Description Stop ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5062,"retMsg":"stop ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/stop/{clusterName} [put]
func (ck *ClickHouseController) StopCluster(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(c, model.STOP_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	//before stop, we need sync zoopath
	/*
		Since when destory cluster, the cluster must be stopped,
		we cant't get zookeeper path by querying ck,
		so need to save the ZooKeeper path before stopping the cluster.
	*/
	err := clickhouse.GetReplicaZkPath(&conf)
	if err != nil {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(c, model.STOP_CK_CLUSTER_FAIL), err)
		return
	}

	clickhouse.CkServices.Delete(clusterName)
	err = deploy.StopCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, model.GetMsg(c, model.STOP_CK_CLUSTER_FAIL), err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Destroy ClickHouse cluster
// @Description Destroy ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5063,"retMsg":"destroy ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/destroy/{clusterName} [put]
func (ck *ClickHouseController) DestroyCluster(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	var err error
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	if conf.SshUser == "" || conf.SshPassword == "" {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, model.GetMsg(c, model.DESTROY_CK_CLUSTER_FAIL),
			fmt.Sprintf("can't find ssh username/passowrd for cluster %s", clusterName))
		return
	}

	if err = deploy.DestroyCkCluster(&conf); err != nil {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, model.GetMsg(c, model.DESTROY_CK_CLUSTER_FAIL), err)
		return
	}
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Delete(clusterName)
	clickhouse.CkServices.Delete(clusterName)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}
	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Rebanlance a ClickHouse cluster
// @Description Rebanlance a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5064,"retMsg":"rebanlance ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/rebalance/{clusterName} [put]
func (ck *ClickHouseController) RebalanceCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf := con.(model.CKManClickHouseConfig)
	hosts := make([]string, len(conf.Shards))
	for index, shard := range conf.Shards {
		hosts[index] = shard.Replicas[0].Ip
	}

	rebalancer := &business.CKRebalance{
		Hosts:      hosts,
		Port:       conf.Port,
		User:       conf.User,
		Password:   conf.Password,
		DataDir:    conf.Path,
		OsUser:     conf.SshUser,
		OsPassword: conf.SshPassword,
		OsPort:     conf.SshPort,
		DBTables:   make(map[string][]string),
		SshConns:   make(map[string]*ssh.Client),
		CKConns:    make(map[string]*sql.DB),
		RepTables:  make(map[string]map[string]string),
	}

	if err = rebalancer.InitCKConns(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(c, model.REBALANCE_CK_CLUSTER_FAIL), err)
		return
	}
	if err = rebalancer.GetTables(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(c, model.REBALANCE_CK_CLUSTER_FAIL), err)
		return
	}
	if err = rebalancer.GetRepTables(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(c, model.REBALANCE_CK_CLUSTER_FAIL), err)
		return
	}

	if err = rebalancer.DoRebalance(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, model.GetMsg(c, model.REBALANCE_CK_CLUSTER_FAIL), err)
		return
	}
	log.Logger.Infof("rebalance done")

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Get ClickHouse cluster status
// @Description Get ClickHouse cluster status
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5065,"retMsg":"get ClickHouse cluster information failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}}"
// @Router /api/v1/ck/get/{clusterName} [get]
func (ck *ClickHouseController) GetClusterStatus(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
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

	info := model.CkClusterInfoRsp{
		Status:  globalStatus,
		Version: conf.Version,
		Nodes:   statusList,
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), info)
}

// @Summary Add ClickHouse node
// @Description Add ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AddNodeReq true "request body"
// @Failure 200 {string} json "{"retCode":5066,"retMsg":"add ClickHouse node failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/{clusterName} [post]
func (ck *ClickHouseController) AddNode(c *gin.Context) {
	var req model.AddNodeReq
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err := deploy.AddCkClusterNode(&conf, &req)
	if err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL), err)
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
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL), err)
		return
	}
	defer service.Stop()
	if err := service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, model.GetMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL), err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	clickhouse.CkServices.Delete(clusterName)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Delete ClickHouse node
// @Description Delete ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param ip query string true "node ip address" default(192.168.101.105)
// @Failure 200 {string} json "{"retCode":5067,"retMsg":"delete ClickHouse node failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/{clusterName} [delete]
func (ck *ClickHouseController) DeleteNode(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	err := deploy.DeleteCkClusterNode(&conf, ip)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL, model.GetMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL), err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(clusterName, conf)
	clickhouse.CkServices.Delete(clusterName)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Get metrics of MergeTree in ClickHouse
// @Description Get metrics of MergeTree in ClickHouse
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":{"sensor_dt_result_online":{"columns":22,"rows":1381742496,"parts":192,"space":54967700946,"completedQueries":5,"failedQueries":0,"queryCost":{"middle":130,"secondaryMax":160.76,"max":162}}}}"
// @Router /api/v1/ck/table_metric/{clusterName} [get]
func (ck *ClickHouseController) GetTableMetric(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	conf = con.(model.CKManClickHouseConfig)
	metrics, err := clickhouse.GetCkTableMetrics(&conf)
	if err != nil {
		model.WrapMsg(c, model.GET_CK_TABLE_METRIC_FAIL, model.GetMsg(c, model.GET_CK_TABLE_METRIC_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), metrics)
}

// @Summary Get open sessions
// @Description Get open sessions
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":[{"startTime":1609997894,"queryDuration":1,"query":"SELECT DISTINCT name FROM system.tables","user":"eoi","queryId":"62dce71d-9294-4e47-9d9b-cf298f73233d","address":"192.168.21.73","threads":2}]}"
// @Router /api/v1/ck/open_sessions/{clusterName} [get]
func (ck *ClickHouseController) GetOpenSessions(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	limit := ClickHouseSessionLimit
	limitStr := c.Query("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	conf = con.(model.CKManClickHouseConfig)
	sessions, err := clickhouse.GetCkOpenSessions(&conf, limit)
	if err != nil {
		gotError = true
		var exception *client.Exception
		if errors.As(err, &exception) {
			if exception.Code == 60 {
				// we do not return error when system.query_log is not exist
				gotError = false
			}
		}
	}
	if gotError {
		model.WrapMsg(c, model.GET_CK_OPEN_SESSIONS_FAIL, model.GetMsg(c, model.GET_CK_OPEN_SESSIONS_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), sessions)
}

// @Summary Get slow sessions
// @Description Get slow sessions
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":[{"startTime":1609986493,"queryDuration":145,"query":"select * from dist_sensor_dt_result_online limit 10000","user":"default","queryId":"8aa3de08-92c4-4102-a83d-2f5d88569dab","address":"::1","threads":2}]}"
// @Router /api/v1/ck/slow_sessions/{clusterName} [get]
func (ck *ClickHouseController) GetSlowSessions(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	clusterName := c.Param(ClickHouseClusterPath)
	limit := ClickHouseSessionLimit
	limitStr := c.Query("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	conf = con.(model.CKManClickHouseConfig)
	sessions, err := clickhouse.GetCkSlowSessions(&conf, limit)
	if err != nil {
		gotError = true
		var exception *client.Exception
		if errors.As(err, &exception) {
			if exception.Code == 60 {
				// we do not return error when system.query_log is not exist
				gotError = false
			}
		}
	}
	if gotError {
		model.WrapMsg(c, model.GET_CK_SLOW_SESSIONS_FAIL, model.GetMsg(c, model.GET_CK_SLOW_SESSIONS_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), sessions)
}

// @Summary Ping cluster
// @Description check clickhousr server in cluster wether useful
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.PingClusterReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5201, "retMsg":"ClickHouse cluster can't ping all nodes successfully", "entity":[]}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":""}"
// @Router /api/v1/ck/ping/{clusterName} [post]
func (ck *ClickHouseController) PingCluster(c *gin.Context) {
	var req model.PingClusterReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	var conf model.CKManClickHouseConfig
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST), fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	conf = con.(model.CKManClickHouseConfig)

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.PING_CK_CLUSTER_FAIL, model.GetMsg(c, model.PING_CK_CLUSTER_FAIL), "can't find any host")
		return
	}

	var err error
	var db *sql.DB
	shardAvailable := true
	for _, shard := range conf.Shards {
		failNum := 0
		for _, replica := range shard.Replicas {
			host := replica.Ip
			db, err = common.ConnectClickHouse(host, conf.Port, req.Database, req.User, req.Password)
			if err != nil {
				log.Logger.Error("err: %+v", err)
				failNum++
				continue
			}
			if err = db.Ping(); err != nil {
				log.Logger.Error("err: %+v", err)
				failNum++
				continue
			}
		}
		if failNum == len(shard.Replicas) {
			shardAvailable = false
		}
	}

	if !shardAvailable {
		model.WrapMsg(c, model.PING_CK_CLUSTER_FAIL, model.GetMsg(c, model.PING_CK_CLUSTER_FAIL), err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Purger Tables Range
// @Description purger table
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.PurgerTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5203, "retMsg":"purger tables range failed", "entity":"error"}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":""}"
// @Router /api/v1/ck/purge_tables/{clusterName} [post]
func (ck *ClickHouseController) PurgeTables(c *gin.Context) {
	var req model.PurgerTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	var conf model.CKManClickHouseConfig
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	conf = con.(model.CKManClickHouseConfig)

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.PURGER_TABLES_FAIL, model.GetMsg(c, model.PURGER_TABLES_FAIL),
			errors.Errorf("can't find any host"))
		return
	}

	var chHosts []string
	for _, shard := range conf.Shards {
		chHosts = append(chHosts, shard.Replicas[0].Ip)
	}
	p := business.NewPurgerRange(chHosts, conf.Port, conf.User, conf.Password, req.Database, req.Begin, req.End)
	err := p.InitConns()
	if err != nil {
		model.WrapMsg(c, model.PURGER_TABLES_FAIL, model.GetMsg(c, model.PURGER_TABLES_FAIL), err)
		return
	}
	for _, table := range req.Tables {
		err := p.PurgeTable(table)
		if err != nil {
			model.WrapMsg(c, model.PURGER_TABLES_FAIL, model.GetMsg(c, model.PURGER_TABLES_FAIL), err)
			return
		}
	}
	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

// @Summary Archive Tables to HDFS
// @Description archive tables to hdfs
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":5204, "retMsg":"archive to hdfs failed", "entity":"error"}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"ok","entity":""}"
// @Router /api/v1/ck/archive/{clusterName} [post]
func (ck *ClickHouseController) ArchiveToHDFS(c *gin.Context) {
	var req model.ArchiveTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	var conf model.CKManClickHouseConfig
	con, ok := clickhouse.CkClusters.Load(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, model.GetMsg(c, model.CLUSTER_NOT_EXIST),
			fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}
	conf = con.(model.CKManClickHouseConfig)

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, model.GetMsg(c, model.ARCHIVE_TO_HDFS_FAIL),
			errors.Errorf("can't find any host"))
		return
	}

	var chHosts []string
	for _, shard := range conf.Shards {
		chHosts = append(chHosts, shard.Replicas[0].Ip)
	}
	archive := &business.ArchiveHDFS{
		Hosts:       chHosts,
		Port:        conf.Port,
		User:        conf.User,
		Password:    conf.Password,
		Database:    req.Database,
		Tables:      req.Tables,
		Begin:       req.Begin,
		End:         req.End,
		MaxFileSize: req.MaxFileSize,
		HdfsAddr:    req.HdfsAddr,
		HdfsUser:    req.HdfsUser,
		HdfsDir:     req.HdfsDir,
		Parallelism: req.Parallelism,
		Conns:       make(map[string]*sql.DB),
	}

	archive.FillArchiveDefault()
	if err := archive.InitConns(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, model.GetMsg(c, model.ARCHIVE_TO_HDFS_FAIL), err)
	}

	if err := archive.GetSortingInfo(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, model.GetMsg(c, model.ARCHIVE_TO_HDFS_FAIL), err)
	}

	if err := archive.ClearHDFS(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, model.GetMsg(c, model.ARCHIVE_TO_HDFS_FAIL), err)
	}

	if err := archive.ExportToHDFS(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, model.GetMsg(c, model.PING_CK_CLUSTER_FAIL), err)
	}
	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}
