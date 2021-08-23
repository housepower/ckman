package controller

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/go-errors/errors"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/nacos"
	"io/ioutil"
	"strconv"
	"strings"
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

func DeployPackage(d deploy.Deploy) (string, error) {
	log.Logger.Infof("start init deploy")
	if err := d.Init(); err != nil {
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
// @Param req body model.CKManClickHouseConfig true "request body"
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
	var conf model.CKManClickHouseConfig
	params := GetSchemaParams(GET_SCHEMA_UI_DEPLOY, conf)
	if params == nil {
		model.WrapMsg(c, model.GET_SCHEMA_UI_FAILED, errors.Errorf("type %s is not registered", GET_SCHEMA_UI_DEPLOY))
		return
	}
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}
	err = params.UnmarshalConfig(string(body), &conf)
	if err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	if err := checkDeployParams(&conf); err != nil {
		model.WrapMsg(c, model.INVALID_PARAMS, err)
		return
	}

	packages := make([]string, 3)
	packages[0] = fmt.Sprintf("%s-%s-%s", model.CkCommonPackagePrefix, conf.Version, model.CkCommonPackageSuffix)
	packages[1] = fmt.Sprintf("%s-%s-%s", model.CkServerPackagePrefix, conf.Version, model.CkServerPackageSuffix)
	packages[2] = fmt.Sprintf("%s-%s-%s", model.CkClientPackagePrefix, conf.Version, model.CkClientPackageSuffix)
	tmp := deploy.ConvertCKDeploy(&conf)
	tmp.Packages = packages
	code, err := DeployPackage(tmp)
	if err != nil {
		model.WrapMsg(c, code, err)
		return
	}

	if err := d.syncDownClusters(c); err != nil {
		return
	}
	clickhouse.CkClusters.SetClusterByName(conf.Cluster, conf)
	if conf.LogicCluster != nil {
		var newLogics []string
		logics, ok := clickhouse.CkClusters.GetLogicClusterByName(*conf.LogicCluster)
		if ok {
			newLogics = append(newLogics, logics...)
		}
		newLogics = append(newLogics, conf.Cluster)
		clickhouse.CkClusters.SetLogicClusterByName(*conf.LogicCluster, newLogics)
	}
	if err := d.syncUpClusters(c); err != nil {
		return
	}
	model.WrapMsg(c, model.SUCCESS, nil)
}

func checkDeployParams(conf *model.CKManClickHouseConfig) error {
	var err error
	if _, ok := clickhouse.CkClusters.GetClusterByName(conf.Cluster); ok {
		return errors.Errorf("cluster %s is exist", conf.Cluster)
	}

	if len(conf.Hosts) == 0 {
		return errors.Errorf("can't find any host")
	}
	if conf.Hosts, err = common.ParseHosts(conf.Hosts); err != nil {
		return err
	}
	if conf.IsReplica && len(conf.Hosts)%2 == 1 {
		return errors.Errorf("When supporting replica, the number of nodes must be even")
	}
	conf.Shards = GetShardsbyHosts(conf.Hosts, conf.IsReplica)
	if len(conf.ZkNodes) == 0 {
		return errors.Errorf("zookeeper nodes must not be empty")
	}
	if conf.ZkNodes, err = common.ParseHosts(conf.ZkNodes); err != nil {
		return err
	}
	if conf.User == "" || conf.Password == "" {
		return errors.Errorf("user or password must not be empty")
	}
	if conf.User == model.ClickHouseRetainUser {
		return errors.Errorf("clickhouse user must not be default")
	}

	if conf.SshUser == "" {
		return errors.Errorf("ssh user must not be empty")
	}

	if conf.AuthenticateType != model.SshPasswordUsePubkey && conf.SshPassword == "" {
		return errors.Errorf("ssh password must not be empty")
	}
	if !strings.HasSuffix(conf.Path, "/") {
		return errors.Errorf(fmt.Sprintf("path %s must end with '/'", conf.Path))
	}
	if err = checkAccess(conf.Path, conf); err != nil {
		return err
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
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
				}
				vers := strings.Split(conf.Version, ".")
				major, err := strconv.Atoi(vers[0])
				if err != nil {
					return err
				}
				minor, err := strconv.Atoi(vers[1])
				if err != nil {
					return err
				}
				if major < 21 || (major == 21 && minor < 9) {
					return errors.Errorf("clickhouse do not support hdfs storage policy while version < 21.9 ")
				}
			case "s3":
				if !strings.HasSuffix(disk.DiskS3.Endpoint, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
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

	conf.Mode = model.CkClusterDeploy
	conf.Normalize()
	return nil
}

func GetShardsbyHosts(hosts []string, isReplica bool) []model.CkShard {
	var shards []model.CkShard
	if isReplica {
		var shard model.CkShard
		for _, host := range hosts {
			replica := model.CkReplica{Ip: host}
			shard.Replicas = append(shard.Replicas, replica)
			if len(shard.Replicas) == 2 {
				shards = append(shards, shard)
				shard.Replicas = make([]model.CkReplica, 0)
			}
		}
	} else {
		for _, host := range hosts {
			shard := model.CkShard{Replicas: []model.CkReplica{{Ip: host}}}
			shards = append(shards, shard)
		}
	}
	return shards
}

func checkAccess(localPath string, conf *model.CKManClickHouseConfig)error {
	cmd := fmt.Sprintf(`su sshd -s /bin/bash -c "cd %s; echo $?"`, localPath)
	for _, host := range conf.Hosts {
		output, err := common.RemoteExecute(conf.SshUser, conf.SshPassword, host, conf.SshPort, cmd)
		if err != nil {
			return err
		}
		access := strings.Trim(output, "\n")
		if access == "1" {
			return errors.Errorf("have no access to enter path %s", localPath)
		}
	}
	return nil
}