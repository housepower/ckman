package runner

import (
	"fmt"

	"github.com/housepower/ckman/common"
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

	if d.Conf.Keeper == model.ClickhouseKeeper {
		return nil
	}
	//clear zkNode
	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	service, err := zookeeper.GetZkService(conf.Cluster)
	defer zookeeper.ZkServiceCache.Delete(conf.Cluster)
	if err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}
	//delete from standard path
	stdZooPath := fmt.Sprintf("/clickhouse/tables/%s", conf.Cluster)
	if err = service.DeleteAll(stdZooPath); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}
	taskQueuePath := fmt.Sprintf("/clickhouse/task_queue/ddl/%s", conf.Cluster)
	if err = service.DeleteAll(taskQueuePath); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}

	return nil
}

func DeleteCkClusterNode(task *model.Task, conf *model.CKManClickHouseConfig, ip string) error {
	//delete zookeeper path if need
	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	ifDeleteShard := false
	for i, shard := range conf.Shards {
		for _, replica := range shard.Replicas {
			if replica.Ip == ip {
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
		service, err := zookeeper.GetZkService(conf.Cluster)
		if err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
		}
		// err = service.CleanZoopath(*conf, conf.Cluster, ip, false)
		// if err != nil {
		// 	log.Logger.Fatalf("clean zoopath error:%v", err)
		// }
		var zooPaths []string
		query := fmt.Sprintf("SELECT zookeeper_path,replica_path FROM clusterAllReplicas('%s', system.replicas) WHERE replica_name = '%s'", conf.Cluster, ip)
		ckService := clickhouse.NewCkService(conf)
		err = ckService.InitCkService()
		if err == nil {
			data, err := ckService.QueryInfo(query)
			if err == nil {
				for i := 1; i < len(data); i++ {
					if ifDeleteShard {
						zooPaths = append(zooPaths, data[i][0].(string))
					} else {
						zooPaths = append(zooPaths, data[i][1].(string))
					}
				}
			}
		}
		for _, zoopath := range zooPaths {
			log.Logger.Debugf("delete zoopath:%s", zoopath)
			err = service.DeleteAll(zoopath)
			if err != nil {
				return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
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
	if d.Conf.Keeper == model.ClickhouseKeeper && d.Conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
		d.Ext.Restart = true
		d.Conf.KeeperConf.KeeperNodes = common.ArrayRemove(conf.Hosts, ip)
	}
	if err := d.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if err := d.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if d.Ext.Restart {
		if err := d.Restart(); err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusRestart.EN)
		}
		if err := d.Check(300); err != nil {
			log.Logger.Warnf("[%s]delnode check failed: %v", d.Conf.Cluster, err)
			//return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
		}
	}

	conf.Hosts = hosts
	conf.Shards = shards
	if d.Conf.Keeper == model.ClickhouseKeeper && d.Conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
		conf.KeeperConf.KeeperNodes = make([]string, len(d.Conf.Hosts))
		copy(conf.KeeperConf.KeeperNodes, d.Conf.Hosts)
	}
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
		log.Logger.Warnf("[%s]addnode check failed: %v", d.Conf.Cluster, err)
		//return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
	}

	// update other nodes config
	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	d2 := deploy.NewCkDeploy(*conf)
	d2.Conf.Shards = d.Conf.Shards
	if conf.Keeper == model.ClickhouseKeeper && conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
		d2.Conf.KeeperConf.KeeperNodes = make([]string, len(conf.Hosts)+len(d.Conf.Hosts))
		copy(d2.Conf.KeeperConf.KeeperNodes, append(conf.Hosts, d.Conf.Hosts...))
	}
	if err := d2.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if err := d2.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	if d.Ext.Restart {
		if err := d2.Restart(); err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
		}

		if err := d2.Check(300); err != nil {
			log.Logger.Warnf("[%s]addnode check failed: %v", d.Conf.Cluster, err)
			//return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
		}
	}

	conf.Shards = d.Conf.Shards
	conf.Hosts = append(conf.Hosts, d.Conf.Hosts...)
	if conf.Keeper == model.ClickhouseKeeper && conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
		conf.KeeperConf.KeeperNodes = make([]string, len(conf.Hosts))
		copy(conf.KeeperConf.KeeperNodes, conf.Hosts)
	}
	return nil
}

func UpgradeCkCluster(task *model.Task, d deploy.CKDeploy) error {
	switch d.Ext.Policy {
	case model.PolicyRolling:
		var rd deploy.CKDeploy
		common.DeepCopyByGob(&rd, d)
		for _, host := range d.Conf.Hosts {
			rd.Conf.Hosts = []string{host}
			if err := upgradePackage(task, rd, model.MaxTimeOut); err != nil {
				return err
			}
		}
	case model.PolicyFull:
		err := upgradePackage(task, d, 10)
		if err != nil && err != model.CheckTimeOutErr {
			return err
		}
	default:
		return fmt.Errorf("not support policy %s yet", d.Ext.Policy)
	}

	return nil
}

func upgradePackage(task *model.Task, d deploy.CKDeploy, timeout int) error {
	var node string
	if d.Ext.Policy == model.PolicyRolling {
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
		switch d.Ext.Policy {
		case model.PolicyRolling:
			var rd deploy.CKDeploy
			common.DeepCopyByGob(&rd, d)
			for _, host := range d.Conf.Hosts {
				deploy.SetNodeStatus(task, model.NodeStatusRestart, host)
				rd.Conf.Hosts = []string{host}
				if err := rd.Restart(); err != nil {
					return err
				}
				if err := rd.Check(model.MaxTimeOut); err != nil {
					return err
				}
				deploy.SetNodeStatus(task, model.NodeStatusDone, host)
			}
		case model.PolicyFull:
			deploy.SetNodeStatus(task, model.NodeStatusRestart, model.ALL_NODES_DEFAULT)
			err := d.Restart()
			if err != nil && err != model.CheckTimeOutErr {
				return err
			}
			_ = d.Check(30)
		default:
			return fmt.Errorf("not support policy %s yet", d.Ext.Policy)
		}

	}
	return nil
}
