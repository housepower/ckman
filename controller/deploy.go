package controller

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"

	"github.com/gin-gonic/gin"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/model"
)

type DeployController struct {
	Controller
	config *config.CKManConfig
}

func NewDeployController(config *config.CKManConfig, wrapfunc Wrapfunc) *DeployController {
	deploy := &DeployController{}
	deploy.config = config
	deploy.wrapfunc = wrapfunc
	return deploy
}

// @Summary 创建集群
// @Description 使用ckman创建集群
// @version 1.0
// @Security ApiKeyAuth
// @Tags deploy
// @Accept  json
// @Param req body model.CKManClickHouseConfig true "request body"
// @Failure 200 {string} json "{"code":"5000","msg":"invalid params","data":""}"
// @Failure 200 {string} json "{"code":"5003","msg":"数据校验失败","data":""}"
// @Failure 200 {string} json "{"code":"5801","msg":"数据插入失败","data":""}"
// @Success 200 {string} json "{"code":"0000","msg":"success","data":nil}"
// @Router /api/v2/deploy/ck [post]
func (controller *DeployController) DeployCk(c *gin.Context) {
	var conf model.CKManClickHouseConfig
	err := DecodeRequestBody(c.Request, &conf, GET_SCHEMA_UI_DEPLOY)
	if err != nil {
		controller.wrapfunc(c, model.E_INVALID_PARAMS, err)
		return
	}

	force := common.TernaryExpression(c.Query("force") == "true", true, false).(bool)

	if err := checkDeployParams(&conf, force); err != nil {
		controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, err)
		return
	}

	tmp := deploy.NewCkDeploy(conf)
	tmp.Packages = deploy.BuildPackages(conf.Version, conf.PkgType, conf.Cwd)
	if conf.KeeperWithStanalone() {
		if tmp.Packages.Keeper == "" {
			controller.wrapfunc(c, model.E_DATA_CHECK_FAILED, errors.Errorf("keeper package not found"))
			return
		}
	}
	taskId, err := deploy.CreateNewTask(conf.Cluster, model.TaskTypeCKDeploy, tmp)
	if err != nil {
		controller.wrapfunc(c, model.E_DATA_INSERT_FAILED, err)
		return
	}
	controller.wrapfunc(c, model.E_SUCCESS, taskId)
}

func checkDeployParams(conf *model.CKManClickHouseConfig, force bool) error {
	var err error
	if repository.Ps.ClusterExists(conf.Cluster) {
		return errors.Errorf("cluster %s is exist", conf.Cluster)
	}

	pkgs, ok := common.CkPackages.Load(conf.PkgType)
	if !ok {
		return errors.Errorf("pkgtype %s have no packages on  server", conf.PkgType)
	}
	found := false
	var file common.CkPackageFile
	for _, pkg := range pkgs.(common.CkPackageFiles) {
		if pkg.PkgName == conf.PkgName {
			found = true
			file = pkg
			break
		}
	}
	if found {
		conf.Version = file.Version
	} else {
		return errors.Errorf("package %s not found on server", conf.PkgName)
	}
	if strings.HasSuffix(conf.PkgType, common.PkgSuffixTgz) {
		if conf.Cwd == "" {
			return errors.Errorf("cwd can't be empty for tgz deployment")
		}
		if !strings.HasSuffix(conf.Cwd, "/") {
			return errors.Errorf(fmt.Sprintf("path %s must end with '/'", conf.Cwd))
		}
		conf.NeedSudo = false
		if err = checkAccess(conf.Cwd, conf); err != nil {
			return errors.Wrapf(err, "check access error")
		}
	} else {
		conf.NeedSudo = true
	}
	if len(conf.Shards) == 0 {
		return errors.Errorf("can't find any host")
	}

	for _, shard := range conf.Shards {
		if len(shard.Replicas) == 0 {
			return errors.Errorf("can't find any host")
		}
		for _, replica := range shard.Replicas {
			conf.Hosts = append(conf.Hosts, replica.Ip)
		}
	}

	if !force {
		if err := common.CheckCkInstance(conf); err != nil {
			return err
		}
	}

	if err = MatchingPlatfrom(conf); err != nil {
		return err
	}

	conf.IsReplica = true
	if conf.Keeper == model.ClickhouseKeeper {
		if conf.KeeperConf == nil {
			return errors.Errorf("keeper conf must not be empty")
		}
		if conf.KeeperConf.Runtime == model.KeeperRuntimeStandalone {
			if conf.KeeperConf.KeeperNodes, err = common.ParseHosts(conf.KeeperConf.KeeperNodes); err != nil {
				return err
			}
			if len(conf.KeeperConf.KeeperNodes) == 0 {
				return errors.Errorf("keeper nodes must not be empty")
			}
		} else if conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
			if strings.HasSuffix(conf.PkgType, common.PkgSuffixTgz) {
				return errors.Errorf("keeper internal runtime doesn't support tgz deployment")
			}
			conf.KeeperConf.KeeperNodes = make([]string, len(conf.Hosts))
			copy(conf.KeeperConf.KeeperNodes, conf.Hosts)

		} else {
			return errors.Errorf("keeper runtime %s is not supported", conf.KeeperConf.Runtime)
		}
		if !strings.HasSuffix(conf.KeeperConf.LogPath, "/") {
			return errors.Errorf(fmt.Sprintf("path %s must end with '/'", conf.KeeperConf.LogPath))
		}
		if !strings.HasSuffix(conf.KeeperConf.SnapshotPath, "/") {
			return errors.Errorf(fmt.Sprintf("path %s must end with '/'", conf.KeeperConf.SnapshotPath))
		}
		if err := checkAccess(conf.KeeperConf.LogPath, conf); err != nil {
			return errors.Wrapf(err, "check access error")
		}
		if err := checkAccess(conf.KeeperConf.SnapshotPath, conf); err != nil {
			return errors.Wrapf(err, "check access error")
		}
	} else {
		if len(conf.ZkNodes) == 0 {
			return errors.Errorf("zookeeper nodes must not be empty")
		}
		if conf.ZkNodes, err = common.ParseHosts(conf.ZkNodes); err != nil {
			return err
		}
	}
	if conf.LogicCluster != nil {
		if conf.Cluster == *conf.LogicCluster {
			return errors.Errorf("cluster name %s must not the same with logic name", conf.Cluster)
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
	}

	if conf.SshUser == "" {
		return errors.Errorf("ssh user must not be empty")
	}

	if !strings.HasSuffix(conf.Path, "/") {
		return errors.Errorf(fmt.Sprintf("path %s must end with '/'", conf.Path))
	}
	if err = checkAccess(conf.Path, conf); err != nil {
		return errors.Wrap(err, "")
	}

	disks := make([]string, 0)
	localPath := make([]string, 0)
	hdfsEndpoints := make([]string, 0)
	s3Endpoints := make([]string, 0)
	disks = append(disks, "default")
	localPath = append(localPath, conf.Path)
	if conf.Storage != nil {
		for _, disk := range conf.Storage.Disks {
			disks = append(disks, disk.Name)
			switch disk.Type {
			case "local":
				if !strings.HasSuffix(disk.DiskLocal.Path, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
				}
				if err = checkAccess(disk.DiskLocal.Path, conf); err != nil {
					return err
				}
				localPath = append(localPath, disk.DiskLocal.Path)
			case "hdfs":
				if !strings.HasSuffix(disk.DiskHdfs.Endpoint, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
				}
				if common.CompareClickHouseVersion(conf.Version, "21.9") < 0 {
					return errors.Errorf("clickhouse do not support hdfs storage policy while version < 21.9 ")
				}
				hdfsEndpoints = append(hdfsEndpoints, disk.DiskHdfs.Endpoint)
			case "s3":
				if !strings.HasSuffix(disk.DiskS3.Endpoint, "/") {
					return errors.Errorf(fmt.Sprintf("path %s must end with '/'", disk.DiskLocal.Path))
				}
				s3Endpoints = append(s3Endpoints, disk.DiskS3.Endpoint)
			default:
				return errors.Errorf("unsupport disk type %s", disk.Type)
			}
		}
		if err = common.EnsurePathNonPrefix(localPath); err != nil {
			return err
		}
		if err = common.EnsurePathNonPrefix(hdfsEndpoints); err != nil {
			return err
		}
		if err = common.EnsurePathNonPrefix(s3Endpoints); err != nil {
			return err
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
	if len(conf.UsersConf.Profiles) > 0 {
		for _, quota := range conf.UsersConf.Quotas {
			if common.ArraySearch(quota.Name, profiles) {
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

	conf.Mode = model.CkClusterDeploy
	conf.User = model.ClickHouseDefaultUser
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
		if len(shard.Replicas) > 0 {
			shards = append(shards, shard)
		}
	} else {
		for _, host := range hosts {
			shard := model.CkShard{Replicas: []model.CkReplica{{Ip: host}}}
			shards = append(shards, shard)
		}
	}
	return shards
}

func checkAccess(localPath string, conf *model.CKManClickHouseConfig) error {
	cmd := ""
	if conf.NeedSudo {
		cmd = fmt.Sprintf(`su clickhouse -s /bin/bash -c "cd %s && echo $?"`, localPath)
	} else {
		cmd = fmt.Sprintf("cd %s && echo $?", localPath)
	}
	for _, host := range conf.Hosts {
		sshOpts := common.SshOptions{
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			Port:             conf.SshPort,
			Host:             host,
			NeedSudo:         conf.NeedSudo,
			AuthenticateType: conf.AuthenticateType,
		}
		if conf.NeedSudo {
			// create clickhouse group and user
			var cmds []string
			//cmds = append(cmds, "groupadd -r clickhouse")
			cmds = append(cmds, "useradd -r --shell /bin/false --home-dir /nonexistent clickhouse")
			_, _ = common.RemoteExecute(sshOpts, strings.Join(cmds, ";"))
		}
		output, err := common.RemoteExecute(sshOpts, cmd)
		if err != nil {
			return err
		}
		access := strings.Trim(output, "\n")
		if access != "0" {
			return errors.Errorf("have no access to enter path %s", localPath)
		}
	}
	return nil
}

func MatchingPlatfrom(conf *model.CKManClickHouseConfig) error {
	arch := strings.Split(conf.PkgType, ".")[0]
	if arch == "amd64" {
		arch = "x86_64"
	}
	if arch == "arm64" {
		arch = "aarch64"
	}
	cmd := "uname -m"
	for _, host := range conf.Hosts {
		sshOpts := common.SshOptions{
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			Port:             conf.SshPort,
			Host:             host,
			NeedSudo:         conf.NeedSudo,
			AuthenticateType: conf.AuthenticateType,
		}
		result, err := common.RemoteExecute(sshOpts, cmd)
		if err != nil {
			return errors.Errorf("host %s get platform failed: %v", host, err)
		}
		if result != arch {
			return errors.Errorf("arch %s mismatched, pkgType: %s", arch, conf.PkgType)
		}
	}
	return nil
}
