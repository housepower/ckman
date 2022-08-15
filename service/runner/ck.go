package runner

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
	"github.com/pkg/errors"
)

func DeployCkCluster(task *model.Task, d deploy.CKDeploy) error {
	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusPrepare, model.ALL_NODES_DEFAULT)
	if err := d.Prepare(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusPrepare.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusInstall, model.ALL_NODES_DEFAULT)
	if err := d.Install(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInstall.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfig, model.ALL_NODES_DEFAULT)
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusStart, model.ALL_NODES_DEFAULT)
	if err := d.Start(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStart.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusCheck, model.ALL_NODES_DEFAULT)
	if err := d.Check(30); err != nil {
		//return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
		deploy.SetTaskStatus(task, model.TaskStatusFailed, err.Error())
	}
	return nil
}

func DestroyCkCluster(task *model.Task, d deploy.CKDeploy, conf *model.CKManClickHouseConfig) error {
	deploy.SetNodeStatus(task, model.NodeStatusStop, model.ALL_NODES_DEFAULT)
	_ = d.Stop()

	deploy.SetNodeStatus(task, model.NodeStatusUninstall, model.ALL_NODES_DEFAULT)
	if err := d.Uninstall(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInstall.EN)
	}

	//clear zkNode
	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	service, err := zookeeper.NewZkService(conf.ZkNodes, conf.ZkPort)
	if err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}
	//delete from standard path
	stdZooPath := fmt.Sprintf("/clickhouse/tables/%s", conf.Cluster)
	if err = service.DeleteAll(stdZooPath); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}
	zooPaths := clickhouse.ConvertZooPath(conf)
	if len(zooPaths) > 0 {
		for _, zooPath := range zooPaths {
			if err = service.DeleteAll(zooPath); err != nil {
				return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
			}
		}
	}
	return nil
}

func DeleteCkClusterNode(task *model.Task, conf *model.CKManClickHouseConfig, ip string) error {
	//delete zookeeper path if need
	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	ifDeleteShard := false
	shardNum := 0
	replicaNum := 0
	for i, shard := range conf.Shards {
		for j, replica := range shard.Replicas {
			if replica.Ip == ip {
				shardNum = i
				replicaNum = j
				if i+1 == len(conf.Shards) {
					if len(shard.Replicas) == 1 {
						ifDeleteShard = true
					}
				}
				break
			}
		}
	}

	index := 0
	for index < len(conf.Hosts) {
		if conf.Hosts[index] == ip {
			break
		}
		index++
	}

	if conf.IsReplica {
		service, err := zookeeper.NewZkService(conf.ZkNodes, conf.ZkPort)
		if err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
		}
		_ = clickhouse.GetReplicaZkPath(conf)
		var zooPaths []string
		for _, path := range conf.ZooPath {
			zooPath := strings.Replace(path, "{cluster}", conf.Cluster, -1)
			zooPath = strings.Replace(zooPath, "{shard}", fmt.Sprintf("%d", shardNum+1), -1)
			zooPaths = append(zooPaths, zooPath)
		}

		for _, path := range zooPaths {
			if ifDeleteShard {
				//delete the shard
				shardNode := fmt.Sprintf("%d", shardNum+1)
				err = service.DeletePathUntilNode(path, shardNode)
				if err != nil {
					return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
				}
			} else {
				// delete replica path
				replicaName := conf.Shards[shardNum].Replicas[replicaNum].Ip
				replicaPath := fmt.Sprintf("%s/replicas/%s", path, replicaName)
				log.Logger.Debugf("replicaPath: %s", replicaPath)
				err = service.DeleteAll(replicaPath)
				if err != nil {
					return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
				}
			}
		}
	}

	// stop the node
	deploy.SetNodeStatus(task, model.NodeStatusStop, model.ALL_NODES_DEFAULT)
	d := deploy.NewCkDeploy(*conf)
	d.Packages = deploy.BuildPackages(conf.Version, conf.PkgType, conf.Cwd)
	d.Conf.Hosts = []string{ip}
	if err := d.Stop(); err != nil {
		log.Logger.Warnf("can't stop node %s, ignore it", ip)
	}

	deploy.SetNodeStatus(task, model.NodeStatusUninstall, model.ALL_NODES_DEFAULT)
	if err := d.Uninstall(); err != nil {
		log.Logger.Warnf("can't uninsatll node %s, ignore it", ip)
	}

	// remove the node from conf struct
	hosts := append(conf.Hosts[:index], conf.Hosts[index+1:]...)
	shards := make([]model.CkShard, len(conf.Shards))
	copy(shards, conf.Shards)
	for i, shard := range shards {
		found := false
		for j, replica := range shard.Replicas {
			if replica.Ip == ip {
				found = true
				if len(shard.Replicas) > 1 {
					shards[i].Replicas = append(shards[i].Replicas[:j], shards[i].Replicas[j+1:]...)
				} else {
					shards = append(shards[:i], shards[i+1:]...)
				}
				break
			}
		}
		if found {
			break
		}
	}

	// update other nodes config
	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	d = deploy.NewCkDeploy(*conf)
	d.Conf.Hosts = hosts
	d.Conf.Shards = shards
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}

	conf.Hosts = hosts
	conf.Shards = shards
	return nil
}

func AddCkClusterNode(task *model.Task, conf *model.CKManClickHouseConfig, d *deploy.CKDeploy) error {
	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusPrepare, model.ALL_NODES_DEFAULT)
	if err := d.Prepare(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusPrepare.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusInstall, model.ALL_NODES_DEFAULT)
	if err := d.Install(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInstall.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfig, model.ALL_NODES_DEFAULT)
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusStart, model.ALL_NODES_DEFAULT)
	if err := d.Start(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStart.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusCheck, model.ALL_NODES_DEFAULT)
	if err := d.Check(30); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
	}

	// update other nodes config
	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	d2 := deploy.NewCkDeploy(*conf)
	d2.Conf.Shards = d.Conf.Shards
	if err := d2.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if err := d2.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	conf.Shards = d.Conf.Shards
	conf.Hosts = append(conf.Hosts, d.Conf.Hosts...)
	return nil
}

func UpgradeCkCluster(task *model.Task, d deploy.CKDeploy) error {
	switch d.Ext.UpgradePolicy {
	case model.UpgradePolicyRolling:
		for _, host := range d.Conf.Hosts {
			d.Conf.Hosts = []string{host}
			if err := upgradePackage(task, d, model.MaxTimeOut); err != nil {
				return err
			}
		}
	case model.UpgradePolicyFull:
		err := upgradePackage(task, d, 10)
		if err != model.CheckTimeOutErr {
			return err
		}
	default:
		return fmt.Errorf("not support policy %s yet", d.Ext.UpgradePolicy)
	}

	return nil
}

func upgradePackage(task *model.Task, d deploy.CKDeploy, timeout int) error {
	var node string
	if d.Ext.UpgradePolicy == model.UpgradePolicyRolling {
		node = d.Conf.Hosts[0]
	} else {
		node = model.ALL_NODES_DEFAULT
	}

	deploy.SetNodeStatus(task, model.NodeStatusInit, node)
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}
	deploy.SetNodeStatus(task, model.NodeStatusStop, node)
	if err := d.Stop(); err != nil {
		log.Logger.Warnf("stop cluster %s failed: %v", d.Conf.Cluster, err)
	}

	deploy.SetNodeStatus(task, model.NodeStatusPrepare, node)
	if err := d.Prepare(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusPrepare.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusUpgrade, node)
	if err := d.Upgrade(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusUpgrade.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfig, node)
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusStart, node)
	if err := d.Start(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStart.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusCheck, node)
	if err := d.Check(timeout); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
	}
	deploy.SetNodeStatus(task, model.NodeStatusDone, node)

	return nil
}

func ConfigCkCluster(task *model.Task, d deploy.CKDeploy) error {
	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}
	deploy.SetNodeStatus(task, model.NodeStatusConfig, model.ALL_NODES_DEFAULT)
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	if d.Ext.Restart {
		deploy.SetNodeStatus(task, model.NodeStatusRestart, model.ALL_NODES_DEFAULT)
		if err := d.Restart(); err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusRestart.EN)
		}
		_ = d.Check(30)
	}
	return nil
}
