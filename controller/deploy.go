package controller

import (
	"fmt"
	"github.com/housepower/ckman/common"

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

func DeployPackage(d deploy.Deploy, base *deploy.DeployBase, conf interface{}) (string, error) {
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

func (d *DeployController) syncUpClusters(c *gin.Context) (err error) {
	clickhouse.AddCkClusterConfigVersion()
	buf, _ := clickhouse.MarshalClusters()
	_ = clickhouse.WriteClusterConfigFile(buf)
	err = d.nacosClient.PublishConfig(string(buf))
	if err != nil {
		model.WrapMsg(c, model.PUB_NACOS_CONFIG_FAIL, err)
		return
	}
	return
}

// @Summary Deploy clickhouse
// @Description Deploy clickhouse
// @version 1.0
// @Security ApiKeyAuth
// @Param req body model.DeployCkReq true "request body"
// @Failure 200 {string} json "{"retCode":"5000","retMsg":"invalid params","entity":""}"
// @Failure 200 {string} json "{"retCode":"5011","retMsg":"init package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5012","retMsg":"prepare package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5013","retMsg":"install package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5014","retMsg":"config package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5015","retMsg":"start package failed","entity":""}"
// @Failure 200 {string} json "{"retCode":"5016","retMsg":"check package failed","entity":""}"
// @Success 200 {string} json "{"retCode":"0000","retMsg":"success","entity":nil}"
// @Router /api/v1/deploy/ck [post]
func (d *DeployController) DeployCk(c *gin.Context) {
	var req model.DeployCkReq
	var factory deploy.DeployFactory
	packages := make([]string, 3)

	req.SavePassword = true   //save password default
	if err := model.DecodeRequestBody(c.Request, &req); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	if req.ClickHouse.User == model.ClickHouseRetainUser {
		model.WrapMsg(c, model.DEPLOY_USER_RETAIN_ERROR, nil)
		return
	}

	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, req.ClickHouse.PackageVersion, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, req.ClickHouse.PackageVersion, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, req.ClickHouse.PackageVersion, model.CkClientPackageSuffix)
	factory = deploy.CKDeployFacotry{}
	ckDeploy := factory.Create()
	base := &deploy.DeployBase{
		Hosts:        req.Hosts,
		Packages:     packages,
		User:         req.User,
		Password:     req.Password,
		PasswordFlag: getSshPasswdFlag(req.SavePassword, req.UsePubKey),
		Port:         req.Port,
		Pool:         common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault),
	}

	code, err := DeployPackage(ckDeploy, base, &req.ClickHouse)
	if err != nil {
		model.WrapMsg(c, code, err)
		return
	}

	conf := convertCkConfig(&req)
	conf.Mode = model.CkClusterDeploy
	if err = d.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(req.ClickHouse.ClusterName, conf)
	if req.ClickHouse.LogicCluster != "" {
		var newLogics []string
		logics, ok := clickhouse.CkClusters.GetLogicClusterByName(req.ClickHouse.LogicCluster)
		if ok {
			newLogics = append(newLogics, logics...)
		}
		newLogics = append(newLogics, req.ClickHouse.ClusterName)
		clickhouse.CkClusters.SetLogicClusterByName(req.ClickHouse.LogicCluster, newLogics)
	}
	if err = d.syncUpClusters(c); err != nil {
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
}

func convertCkConfig(req *model.DeployCkReq) model.CKManClickHouseConfig {
	conf := model.CKManClickHouseConfig{
		Port:            req.ClickHouse.CkTcpPort,
		HttpPort:        req.ClickHouse.CkHttpPort,
		User:            req.ClickHouse.User,
		Password:        req.ClickHouse.Password,
		Cluster:         req.ClickHouse.ClusterName,
		ZkNodes:         req.ClickHouse.ZkNodes,
		ZkPort:          req.ClickHouse.ZkPort,
		Version:         req.ClickHouse.PackageVersion,
		SshUser:         req.User,
		SshPassword:     req.Password,
		SshPasswordFlag: getSshPasswdFlag(req.SavePassword, req.UsePubKey),
		SshPort:         req.Port,
		Shards:          req.ClickHouse.Shards,
		Path:            req.ClickHouse.Path,
		LogicName:       req.ClickHouse.LogicCluster,
	}

	hosts := make([]string, 0)
	for _, shard := range req.ClickHouse.Shards {
		if len(shard.Replicas) > 1 {
			conf.IsReplica = true
		}
		for _, replica := range shard.Replicas {
			hosts = append(hosts, replica.Ip)
		}
	}
	conf.Hosts = hosts

	conf.Normalize()
	return conf
}

func getSshPasswdFlag(savePasswd, usePubkey bool) int {
	if usePubkey {
		return model.SshPasswordUsePubkey
	}
	if savePasswd {
		return model.SshPasswordSave
	}
	return model.SshPasswordNotSave
}
