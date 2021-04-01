package controller

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/nacos"
)

type DeployController struct {
	config      *config.CKManConfig
	nacosClient *nacos.NacosClient
}

func NewDeployController(config *config.CKManConfig, nacosClient *nacos.NacosClient) *DeployController {
	deploy := &DeployController{}
	deploy.config = config
	deploy.nacosClient = nacosClient
	return deploy
}

func DeployPackage(d deploy.Deploy, base *deploy.DeployBase, conf interface{}) (int, error) {
	log.Logger.Infof("start init deploy")
	if err := d.Init(base, conf); err != nil {
		return model.INIT_PACKAGE_FAIL, err
	}

	log.Logger.Infof("start copy packages")
	if err := d.Prepare(); err != nil {
		return model.PREPARE_PACKAGE_FAIL, err
	}

	log.Logger.Infof("start install packages")
	if err := d.Install(); err != nil {
		return model.INSTALL_PACKAGE_FAIL, err
	}

	log.Logger.Infof("start copy config files")
	if err := d.Config(); err != nil {
		return model.CONFIG_PACKAGE_FAIL, err
	}

	log.Logger.Infof("start service")
	if err := d.Start(); err != nil {
		return model.START_PACKAGE_FAIL, err
	}

	log.Logger.Infof("start check service")
	if err := d.Check(); err != nil {
		return model.CHECK_PACKAGE_FAIL, err
	}

	return model.SUCCESS, nil
}

func (d *DeployController) syncDownClusters(c *gin.Context) (err error) {
	data, err := d.nacosClient.GetConfig()
	if err != nil {
		model.WrapMsg(c, model.GET_NACOS_CONFIG_FAIL, model.GetMsg(c, model.GET_NACOS_CONFIG_FAIL), err)
		return
	}
	if data != "" {
		clickhouse.UpdateLocalCkClusterConfig([]byte(data))
	}
	return
}

func (d *DeployController) syncUpClusters(c *gin.Context) (err error) {
	clickhouse.AddCkClusterConfigVersion()
	buf, _ := clickhouse.MarshalClusters()
	clickhouse.WriteClusterConfigFile(buf)
	err = d.nacosClient.PublishConfig(string(buf))
	if err != nil {
		model.WrapMsg(c, model.PUB_NACOS_CONFIG_FAIL, model.GetMsg(c, model.PUB_NACOS_CONFIG_FAIL), err)
		return
	}
	return
}

// @Summary Deploy clickhouse
// @Description Deploy clickhouse
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.DeployCkReq true "request body"
// @Failure 200 {string} json "{"retCode":5000,"retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":5011,"retMsg":"init package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":5012,"retMsg":"prepare package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":5013,"retMsg":"install package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":5014,"retMsg":"config package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":5015,"retMsg":"start package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":5016,"retMsg":"check package failed","entity":""}"
// @Success 200 {string} json "{"retCode":0,"retMsg":"success","entity":nil}"
// @Router /api/v1/deploy/ck [post]
func (d *DeployController) DeployCk(c *gin.Context) {
	var req model.DeployCkReq
	var factory deploy.DeployFactory
	packages := make([]string, 3)

	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, model.GetMsg(c, model.INVALID_PARAMS), err)
		return
	}

	if req.ClickHouse.User == model.ClickHouseRetainUser {
		model.WrapMsg(c, model.DEPLOY_USER_RETAIN_ERROR, model.GetMsg(c, model.DEPLOY_USER_RETAIN_ERROR), nil)
		return
	}

	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, req.ClickHouse.PackageVersion, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, req.ClickHouse.PackageVersion, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, req.ClickHouse.PackageVersion, model.CkClientPackageSuffix)
	factory = deploy.CKDeployFacotry{}
	ckDeploy := factory.Create()
	base := &deploy.DeployBase{
		Hosts:    req.Hosts,
		Packages: packages,
		User:     req.User,
		Password: req.Password,
	}

	code, err := DeployPackage(ckDeploy, base, &req.ClickHouse)
	if err != nil {
		model.WrapMsg(c, code, model.GetMsg(c, code), err)
		return
	}

	conf := convertCkConfig(&req)
	conf.Mode = model.CkClusterDeploy
	if err = d.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.Store(req.ClickHouse.ClusterName, conf)
	if err = d.syncDownClusters(c); err != nil {
		return
	}
	model.WrapMsg(c, model.SUCCESS, model.GetMsg(c, model.SUCCESS), nil)
}

func convertCkConfig(req *model.DeployCkReq) model.CKManClickHouseConfig {
	conf := model.CKManClickHouseConfig{
		Port:        req.ClickHouse.CkTcpPort,
		User:        req.ClickHouse.User,
		Password:    req.ClickHouse.Password,
		Cluster:     req.ClickHouse.ClusterName,
		ZkNodes:     req.ClickHouse.ZkNodes,
		ZkPort:      req.ClickHouse.ZkPort,
		Version:     req.ClickHouse.PackageVersion,
		SshUser:     req.User,
		SshPassword: req.Password,
		Shards:      req.ClickHouse.Shards,
		Path:        req.ClickHouse.Path,
	}

	hosts := make([]string, 0)
	hostnames := make([]string, 0)
	for _, shard := range req.ClickHouse.Shards {
		if len(shard.Replicas) > 1 {
			conf.IsReplica = true
		}
		for _, replica := range shard.Replicas {
			hosts = append(hosts, replica.Ip)
			hostnames = append(hostnames, replica.HostName)
		}
	}
	conf.Hosts = hosts
	conf.Names = hostnames

	clickhouse.CkConfigFillDefault(&conf)
	return conf
}
