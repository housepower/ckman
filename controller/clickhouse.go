package controller

import (
	"fmt"
	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/pkg/errors"
	"golang.org/x/crypto/ssh"
	"strconv"
	"time"

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
		model.WrapMsg(c, model.GET_NACOS_CONFIG_FAIL, err)
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
		model.WrapMsg(c, model.PUB_NACOS_CONFIG_FAIL, err)
		return
	}
	return
}

// @Summary Import a ClickHouse cluster
// @Description Import a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.CkImportConfig true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5042","retMsg":"import ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":null}"
// @Router /api/v1/ck/cluster [post]
func (ck *ClickHouseController) ImportCluster(c *gin.Context) {
	var req model.CkImportConfig
	var conf model.CKManClickHouseConfig

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	_, ok := clickhouse.CkClusters.GetClusterByName(req.Cluster)
	if ok {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, fmt.Sprintf("cluster %s already exist", req.Cluster))
		return
	}

	conf.Hosts = req.Hosts
	conf.Port = req.Port
	conf.HttpPort = req.HttpPort
	conf.Cluster = req.Cluster
	conf.User = req.User
	conf.Password = req.Password
	conf.ZkNodes = req.ZkNodes
	conf.ZkPort = req.ZkPort
	conf.ZkStatusPort = req.ZkStatusPort
	conf.SshPasswordFlag = model.SshPasswordNotSave
	conf.Mode = model.CkClusterImport
	conf.Normalize()
	err := clickhouse.GetCkClusterConfig(&conf)
	if err != nil {
		model.WrapMsg(c, model.IMPORT_CK_CLUSTER_FAIL, err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(req.Cluster, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Delete a ClickHouse cluster
// @Description Delete a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":null}"
// @Router /api/v1/ck/cluster/{clusterName} [delete]
func (ck *ClickHouseController) DeleteCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if ok {
		common.CloseConns(conf.Hosts)
	}

	clickhouse.CkClusters.DeleteClusterByName(clusterName)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS,  nil)
}

// @Summary Get config of a ClickHouse cluster
// @Description Get config of a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5065","retMsg":"get ClickHouse cluster information failed","entity":null}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok", "entity":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}"
// @Router /api/v1/ck/cluster/{clusterName} [get]
func (ck *ClickHouseController) GetCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	var cluster model.CKManClickHouseConfig
	cluster, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.GET_CK_CLUSTER_INFO_FAIL, nil)
	}
	if cluster.Mode == model.CkClusterImport {
		_ = clickhouse.GetCkClusterConfig(&cluster)
	}
	cluster.Password = common.DesEncrypt(cluster.Password)
	if cluster.SshPassword != "" {
		cluster.SshPassword = common.DesEncrypt(cluster.SshPassword)
	}
	model.WrapMsg(c, model.SUCCESS, cluster)
}

// @Summary Get config of all ClickHouse cluster
// @Description Get ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok", "entity":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}"
// @Router /api/v1/ck/cluster [get]
func (ck *ClickHouseController) GetClusters(c *gin.Context) {
	var err error
	if err = ck.syncDownClusters(c); err != nil {
		return
	}

	clusters := clickhouse.CkClusters.GetClusters()
	for key, cluster := range clusters {
		if cluster.Mode == model.CkClusterImport {
			if err = clickhouse.GetCkClusterConfig(&cluster); err != nil {
				log.Logger.Warnf("get import cluster failed:%v", err)
				delete(clusters, key)
				continue
			}
		}
		cluster.Password = common.DesEncrypt(cluster.Password)
		if cluster.SshPassword != "" {
			cluster.SshPassword = common.DesEncrypt(cluster.SshPassword)
		}
		clusters[key] = cluster
	}

	model.WrapMsg(c, model.SUCCESS, clusters)
}

// @Summary Create Table
// @Description Create Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.CreateCkTableReq true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5001","retMsg":"create ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":null}"
// @Router /api/v1/ck/table/{clusterName} [post]
func (ck *ClickHouseController) CreateTable(c *gin.Context) {
	var req model.CreateCkTableReq
	var params model.CreateCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, err)
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
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, err)
		return
	}

	//sync zookeeper path
	var conf model.CKManClickHouseConfig
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	err = clickhouse.GetReplicaZkPath(&conf)
	if err != nil {
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Create Distribute Table on logic cluster
// @Description Create Distribute Table on logic cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(logic_test)
// @Param req body model.CreateDistTableReq true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5001","retMsg":"create ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":null}"
// @Router /api/v1/ck/dist_table [post]
func (ck *ClickHouseController) CreateDistTableOnLogic(c *gin.Context) {
	var req model.CreateDistTableReq
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	logics, ok := clickhouse.CkClusters.GetLogicClusterByName(req.LogicName)
	if !ok {
		model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, fmt.Sprintf("logic cluster %s is not exist", req.LogicName))
		return
	}

	for _, cluster := range logics {
		ckService, err := clickhouse.GetCkService(cluster)
		if err != nil {
			model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, err)
			return
		}
		params := model.CreateDistTblParams{
			Database:    req.Database,
			TableName:   req.LocalTable,
			ClusterName: cluster,
			LogicName:   req.LogicName,
		}
		if err = ckService.CreateDistTblOnLogic(&params); err != nil {
			model.WrapMsg(c, model.CREAT_CK_TABLE_FAIL, err)
			return
		}
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Alter Table
// @Description Alter Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param req body model.AlterCkTableReq true "request body"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5003","retMsg":"alter ClickHouse table failed","entity":""}"
// @Router /api/v1/ck/table/{clusterName} [put]
func (ck *ClickHouseController) AlterTable(c *gin.Context) {
	var req model.AlterCkTableReq
	var params model.AlterCkTableParams

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, err)
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
		model.WrapMsg(c, model.ALTER_CK_TABLE_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Delete Table
// @Description Delete Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"retCode":"5002","retMsg":"delete ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":null}"
// @Router /api/v1/ck/table/{clusterName} [delete]
func (ck *ClickHouseController) DeleteTable(c *gin.Context) {
	var params model.DeleteCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, err)
		return
	}

	var conf model.CKManClickHouseConfig
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	params.Cluster = ckService.Config.Cluster
	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	if err := ckService.DeleteTable(&conf, &params); err != nil {
		model.WrapMsg(c, model.DELETE_CK_TABLE_FAIL, err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Describe Table
// @Description Describe Table
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param database query string true "database name" default(default)
// @Param tableName query string true "table name" default(test_table)
// @Failure 200 {string} json "{"retCode":"5040","retMsg":"describe ClickHouse table failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"name":"_timestamp","type":"DateTime","defaultType":"","defaultExpression":"","comment":"","codecExpression":"","ttlExpression":""}]}"
// @Router /api/v1/ck/table/{clusterName} [get]
func (ck *ClickHouseController) DescTable(c *gin.Context) {
	var params model.DescCkTableParams

	clusterName := c.Param(ClickHouseClusterPath)
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, err)
		return
	}

	params.Name = c.Query("tableName")
	params.DB = c.Query("database")
	if params.DB == "" {
		params.DB = model.ClickHouseDefaultDB
	}

	atts, err := ckService.DescTable(&params)
	if err != nil {
		model.WrapMsg(c, model.DESC_CK_TABLE_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, atts)
}

// @Summary Query Info
// @Description Query Info
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param query query string true "sql" default(show databases)
// @Failure 200 {string} json "{"retCode":"5042","retMsg":"query ClickHouse failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[["name"],["default"],["system"]]}"
// @Router /api/v1/ck/query/{clusterName} [get]
func (ck *ClickHouseController) QueryInfo(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	query := c.Query("query")

	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, err)
		return
	}

	data, err := ckService.QueryInfo(query)
	if err != nil {
		model.WrapMsg(c, model.QUERY_CK_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, data)
}

// @Summary Upgrade ClickHouse cluster
// @Description Upgrade ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param req body model.CkUpgradeCkReq true "request body"
// @Failure 200 {string} json "{"retCode":"5060","retMsg":"upgrade ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/upgrade/{clusterName} [put]
func (ck *ClickHouseController) UpgradeCluster(c *gin.Context) {
	var req model.CkUpgradeCkReq
	clusterName := c.Param(ClickHouseClusterPath)

	req.SkipSameVersion = true // skip the same version default
	req.Policy = model.UpgradePolicyFull // use full policy default
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, err)
		return
	}

	err := deploy.UpgradeCkCluster(&conf, req)
	if err != nil {
		model.WrapMsg(c, model.UPGRADE_CK_CLUSTER_FAIL, err)
		return
	}

	conf.Version = req.PackageVersion
	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Start ClickHouse cluster
// @Description Start ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"retCode":"5061","retMsg":"start ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/start/{clusterName} [put]
func (ck *ClickHouseController) StartCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, err)
		return
	}

	err := deploy.StartCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.START_CK_CLUSTER_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Stop ClickHouse cluster
// @Description Stop ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"retCode":"5062","retMsg":"stop ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/stop/{clusterName} [put]
func (ck *ClickHouseController) StopCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, err)
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
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, err)
		return
	}

	common.CloseConns(conf.Hosts)
	err = deploy.StopCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.STOP_CK_CLUSTER_FAIL, err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Destroy ClickHouse cluster
// @Description Destroy ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"retCode":"5063","retMsg":"destroy ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/destroy/{clusterName} [put]
func (ck *ClickHouseController) DestroyCluster(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	var err error
	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, err)
		return
	}

	if err = deploy.DestroyCkCluster(&conf); err != nil {
		model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, err)
		return
	}
	if err = ck.syncDownClusters(c); err != nil {
		return
	}

	if conf.LogicName != "" {
		var newLogics []string
		logics, ok := clickhouse.CkClusters.GetLogicClusterByName(conf.LogicName)
		if ok {
			//need delete logic cluster and reconf other cluster
			for _, logic := range logics {
				if logic == clusterName {
					continue
				}
				newLogics = append(newLogics, logic)
			}
		}
		if len(newLogics) == 0 {
			clickhouse.CkClusters.DeleteLogicClusterByName(conf.LogicName)
		} else {
			clickhouse.CkClusters.SetLogicClusterByName(conf.LogicName, newLogics)
			for _, newLogic := range newLogics {
				if err = deploy.ConfigLogicOtherCluster(newLogic); err != nil {
					model.WrapMsg(c, model.DESTROY_CK_CLUSTER_FAIL, err)
					return
				}
			}
		}
	}

	clickhouse.CkClusters.DeleteClusterByName(clusterName)
	common.CloseConns(conf.Hosts)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Rebanlance a ClickHouse cluster
// @Description Rebanlance a ClickHouse cluster
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5064","retMsg":"rebanlance ClickHouse cluster failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/rebalance/{clusterName} [put]
func (ck *ClickHouseController) RebalanceCluster(c *gin.Context) {
	var err error
	clusterName := c.Param(ClickHouseClusterPath)

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	hosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, err)
		return
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
		RepTables:  make(map[string]map[string]string),
	}

	if err = rebalancer.InitCKConns(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, err)
		return
	}

	if err = rebalancer.GetTables(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, err)
		return
	}
	if err = rebalancer.GetRepTables(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, err)
		return
	}

	if err = rebalancer.DoRebalance(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		model.WrapMsg(c, model.REBALANCE_CK_CLUSTER_FAIL, err)
		return
	}
	log.Logger.Infof("rebalance done")

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Get ClickHouse cluster status
// @Description Get ClickHouse cluster status
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5065","retMsg":"get ClickHouse cluster information failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":{"test":{"mode":"import","hosts":["192.168.0.1","192.168.0.2","192.168.0.3","192.168.0.4"],"names":["node1","node2","node3","node4"],"port":9000,"httpPort":8123,"user":"ck","password":"123456","database":"default","cluster":"test","zkNodes":["192.168.0.1","192.168.0.2","192.168.0.3"],"zkPort":2181,"zkStatusPort":8080,"isReplica":true,"version":"20.8.5.45","sshUser":"","sshPassword":"","shards":[{"replicas":[{"ip":"192.168.0.1","hostname":"node1"},{"ip":"192.168.0.2","hostname":"node2"}]},{"replicas":[{"ip":"192.168.0.3","hostname":"node3"},{"ip":"192.168.0.4","hostname":"node4"}]}],"path":""}}}}"
// @Router /api/v1/ck/get/{clusterName} [get]
func (ck *ClickHouseController) GetClusterStatus(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if conf.Mode == model.CkClusterImport {
		err := clickhouse.GetCkClusterConfig(&conf)
		if err != nil {
			model.WrapMsg(c, model.CLUSTER_NOT_EXIST, err)
			return
		}
	}
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
	if conf.SshPasswordFlag == model.SshPasswordNotSave {
		needPassword = true
	}

	info := model.CkClusterInfoRsp{
		Status:       globalStatus,
		Version:      conf.Version,
		Nodes:        statusList,
		Mode:         conf.Mode,
		NeedPassword: needPassword,
	}

	model.WrapMsg(c, model.SUCCESS, info)
}

// @Summary Add ClickHouse node
// @Description Add ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param req body model.AddNodeReq true "request body"
// @Failure 200 {string} json "{"retCode":"5066","retMsg":"add ClickHouse node failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/{clusterName} [post]
func (ck *ClickHouseController) AddNode(c *gin.Context) {
	var req model.AddNodeReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, err)
		return
	}

	err := deploy.AddCkClusterNode(&conf, &req)
	if err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, err)
		return
	}

	tmp := &model.CKManClickHouseConfig{
		Hosts:    req.Ips,
		Port:     conf.Port,
		Cluster:  conf.Cluster,
		User:     conf.User,
		Password: conf.Password,
	}

	service := clickhouse.NewCkService(tmp)
	if err := service.InitCkService(); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, err)
		return
	}
	if err := service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
		model.WrapMsg(c, model.ADD_CK_CLUSTER_NODE_FAIL, err)
		return
	}

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Delete ClickHouse node
// @Description Delete ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Param ip query string true "node ip address" default(192.168.101.105)
// @Failure 200 {string} json "{"retCode":"5067","retMsg":"delete ClickHouse node failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/{clusterName} [delete]
func (ck *ClickHouseController) DeleteNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL, err)
		return
	}

	err := deploy.DeleteCkClusterNode(&conf, ip)
	if err != nil {
		model.WrapMsg(c, model.DELETE_CK_CLUSTER_NODE_FAIL, err)
		return
	}
	common.CloseConns([]string{ip})

	if err = ck.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(clusterName, conf)
	if err = ck.syncUpClusters(c); err != nil {
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Start ClickHouse node
// @Description Start ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"retCode":"5052","retMsg":"start node failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/start/{clusterName} [put]
func (ck *ClickHouseController) StartNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		model.WrapMsg(c, model.START_CK_NODE_FAIL, fmt.Errorf("node ip does not exist"))
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.START_CK_NODE_FAIL, err)
		return
	}

	conf.Hosts = []string{ip}

	err := deploy.StartCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.START_CK_NODE_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Stop ClickHouse node
// @Description Stop ClickHouse node
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param password query string false "password"
// @Failure 200 {string} json "{"retCode":"5053","retMsg":"stop node failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":null}"
// @Router /api/v1/ck/node/stop/{clusterName} [put]
func (ck *ClickHouseController) StopNode(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	ip := c.Query("ip")
	if ip == "" {
		model.WrapMsg(c, model.STOP_CK_NODE_FAIL, fmt.Errorf("node ip does not exist"))
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if err := verifySshPassword(c, &conf, conf.SshUser, conf.SshPassword); err != nil {
		model.WrapMsg(c, model.STOP_CK_NODE_FAIL, err)
		return
	}

	conf.Hosts = []string{ip}

	err := deploy.StopCkCluster(&conf)
	if err != nil {
		model.WrapMsg(c, model.STOP_CK_NODE_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Get metrics of MergeTree in ClickHouse
// @Description Get metrics of MergeTree in ClickHouse
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":{"sensor_dt_result_online":{"columns":22,"rows":1381742496,"parts":192,"space":54967700946,"completedQueries":5,"failedQueries":0,"queryCost":{"middle":130,"secondaryMax":160.76,"max":162}}}}"
// @Router /api/v1/ck/table_metric/{clusterName} [get]
func (ck *ClickHouseController) GetTableMetric(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	metrics, err := clickhouse.GetCkTableMetrics(&conf)
	if err != nil {
		model.WrapMsg(c, model.GET_CK_TABLE_METRIC_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, metrics)
}

// @Summary Get open sessions
// @Description Get open sessions
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"startTime":1609997894,"queryDuration":1,"query":"SELECT DISTINCT name FROM system.tables","user":"eoi","queryId":"62dce71d-9294-4e47-9d9b-cf298f73233d","address":"192.168.21.73","threads":2}]}"
// @Router /api/v1/ck/open_sessions/{clusterName} [get]
func (ck *ClickHouseController) GetOpenSessions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	limit := ClickHouseSessionLimit
	limitStr := c.Query("limit")
	if limitStr != "" {
		limit, _ = strconv.Atoi(limitStr)
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
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
		model.WrapMsg(c, model.GET_CK_OPEN_SESSIONS_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, sessions)
}

// @Summary Get slow sessions
// @Description Get slow sessions
// @version 1.0
// @Security ApiKeyAuth
// @Param clusterName path string true "cluster name" default(test)
// @Param limit query string false "sessions limit" default(10)
// @Param start query string false "sessions limit"
// @Param end query string false "sessions limit"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":[{"startTime":1609986493,"queryDuration":145,"query":"select * from dist_sensor_dt_result_online limit 10000","user":"default","queryId":"8aa3de08-92c4-4102-a83d-2f5d88569dab","address":"::1","threads":2}]}"
// @Router /api/v1/ck/slow_sessions/{clusterName} [get]
func (ck *ClickHouseController) GetSlowSessions(c *gin.Context) {
	clusterName := c.Param(ClickHouseClusterPath)
	now := time.Now().Unix() //second
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

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	var gotError bool
	sessions, err := clickhouse.GetCkSlowSessions(&conf, cond)
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
		model.WrapMsg(c, model.GET_CK_SLOW_SESSIONS_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, sessions)
}

// @Summary Ping cluster
// @Description check clickhousr server in cluster wether useful
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.PingClusterReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5201", "retMsg":"ClickHouse cluster can't ping all nodes successfully:DB::NetException: Connection refused", "entity":nil}"
// @Failure 200 {string} json "{"retCode":"0081", "retMsg":"ClickHouse cluster can't ping all nodes successfully:DB::Exception: Database kkkk doesn't exist.", "entity":nil}"
// @Failure 200 {string} json "{"retCode":"0516", "retMsg":"ClickHouse cluster can't ping all nodes successfully: Authentication failed: password is incorrect or there is no user with such name. ", "entity":nil}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":nil}"
// @Router /api/v1/ck/ping/{clusterName} [post]
func (ck *ClickHouseController) PingCluster(c *gin.Context) {
	var req model.PingClusterReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.PING_CK_CLUSTER_FAIL, "can't find any host")
		return
	}

	var err error
	shardAvailable := true
	for _, shard := range conf.Shards {
		failNum := 0
		for _, replica := range shard.Replicas {
			host := replica.Ip
			_, err = common.ConnectClickHouse(host, conf.Port, req.Database, req.User, req.Password)
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
		model.WrapMsg(c, model.PING_CK_CLUSTER_FAIL, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Purger Tables Range
// @Description purger table
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.PurgerTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5203", "retMsg":"purger tables range failed", "entity":"error"}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":""}"
// @Router /api/v1/ck/purge_tables/{clusterName} [post]
func (ck *ClickHouseController) PurgeTables(c *gin.Context) {
	var req model.PurgerTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.PURGER_TABLES_FAIL, errors.Errorf("can't find any host"))
		return
	}

	chHosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		model.WrapMsg(c, model.PURGER_TABLES_FAIL, err)
		return
	}
	p := business.NewPurgerRange(chHosts, conf.Port, conf.User, conf.Password, req.Database, req.Begin, req.End)
	err = p.InitConns()
	if err != nil {
		model.WrapMsg(c, model.PURGER_TABLES_FAIL, err)
		return
	}
	for _, table := range req.Tables {
		err := p.PurgeTable(table)
		if err != nil {
			model.WrapMsg(c, model.PURGER_TABLES_FAIL, err)
			return
		}
	}
	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary Archive Tables to HDFS
// @Description archive tables to hdfs
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5204", "retMsg":"archive to hdfs failed", "entity":"error"}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":""}"
// @Router /api/v1/ck/archive/{clusterName} [post]
func (ck *ClickHouseController) ArchiveToHDFS(c *gin.Context) {
	var req model.ArchiveTableReq
	clusterName := c.Param(ClickHouseClusterPath)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	conf, ok := clickhouse.CkClusters.GetClusterByName(clusterName)
	if !ok {
		model.WrapMsg(c, model.CLUSTER_NOT_EXIST, fmt.Sprintf("cluster %s does not exist", clusterName))
		return
	}

	if len(conf.Hosts) == 0 {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, errors.Errorf("can't find any host"))
		return
	}

	chHosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, err)
		return
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
	}

	archive.FillArchiveDefault()
	if err := archive.InitConns(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, err)
		return
	}

	if err := archive.GetSortingInfo(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, err)
		return
	}

	if err := archive.ClearHDFS(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, err)
		return
	}

	if err := archive.ExportToHDFS(); err != nil {
		model.WrapMsg(c, model.ARCHIVE_TO_HDFS_FAIL, err)
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
}

// @Summary show create table
// @Description show create table
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.ArchiveTableReq true "request body"
// @Param clusterName path string true "cluster name" default(test)
// @Failure 200 {string} json "{"retCode":"5205", "retMsg":"show create table schemer failed", "entity":"error"}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"ok","entity":"{\"create_table_query\": \"CREATE TABLE default.apache_access_log (`@collectiontime` DateTime, `@hostname` LowCardinality(String), `@ip` LowCardinality(String), `@path` String, `@lineno` Int64, `@message` String, `agent` String, `auth` String, `bytes` Int64, `clientIp` String, `device_family` LowCardinality(String), `httpversion` LowCardinality(String), `ident` String, `os_family` LowCardinality(String), `os_major` LowCardinality(String), `os_minor` LowCardinality(String), `referrer` String, `request` String, `requesttime` Float64, `response` LowCardinality(String), `timestamp` DateTime64(3), `userAgent_family` LowCardinality(String), `userAgent_major` LowCardinality(String), `userAgent_minor` LowCardinality(String), `verb` LowCardinality(String), `xforwardfor` LowCardinality(String)) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/default/apache_access_log', '{replica}') PARTITION BY toYYYYMMDD(timestamp) ORDER BY (timestamp, `@hostname`, `@path`, `@lineno`) SETTINGS index_granularity = 8192 │ ReplicatedMergeTree('/clickhouse/tables/{cluster}/{shard}/default/apache_access_log', '{replica}') PARTITION BY toYYYYMMDD(timestamp) ORDER BY (timestamp, `@hostname`, `@path`, `@lineno`) SETTINGS index_granularity = 8192\"}"
// @Router /api/v1/ck/table_schema/{clusterName} [get]
func (ck *ClickHouseController) ShowSchema(c *gin.Context) {
	var schema model.ShowSchemaRsp
	clusterName := c.Param(ClickHouseClusterPath)
	database := c.Query("database")
	tableName := c.Query("tableName")
	if database == "" {
		database = model.ClickHouseDefaultDB
	}
	if tableName == "" {
		model.WrapMsg(c, model.SHOW_SCHEMA_ERROR, fmt.Errorf("table name must not be nil"))
		return
	}
	ckService, err := clickhouse.GetCkService(clusterName)
	if err != nil {
		model.WrapMsg(c, model.SHOW_SCHEMA_ERROR, err)
		return
	}
	schema.CreateTableQuery, err = ckService.ShowCreateTable(tableName, database)
	if err != nil {
		model.WrapMsg(c, model.SHOW_SCHEMA_ERROR, err)
		return
	}

	model.WrapMsg(c, model.SUCCESS, schema)
}

func verifySshPassword(c *gin.Context, conf *model.CKManClickHouseConfig, sshUser, sshPassword string) error {
	if conf.Mode == model.CkClusterImport {
		return fmt.Errorf("not support this operate with import mode")
	}

	if sshUser == "" {
		return fmt.Errorf("sshUser must not be null")
	}

	if conf.SshPasswordFlag == model.SshPasswordSave && sshPassword == "" {
		return fmt.Errorf("expect sshPassword but got null")
	}

	if conf.SshPasswordFlag == model.SshPasswordNotSave {
		password := c.Query("password")
		if password == "" {
			return fmt.Errorf("expect sshPassword but got null")
		}
		conf.SshPassword = password
	}
	return nil
}
