package runner

import (
	"encoding/json"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/zookeeper"
	"github.com/pkg/errors"
)

var TaskHandleFunc = map[string]func(task *model.Task) error{
	model.TaskTypeCKDeploy:     CKDeployHandle,
	model.TaskTypeCKUpgrade:    CKUpgradeHandle,
	model.TaskTypeCKAddNode:    CKAddNodeHandle,
	model.TaskTypeCKDeleteNode: CKDeleteNodeHandle,
	model.TaskTypeCKDestory:    CKDestoryHandle,
	model.TaskTypeCKSetting:    CKSettingHandle,
	model.TaskTypeCKArchive:    CKArchiveHandle,
}

func UnmarshalConfig(config interface{}, v interface{}) error {
	data, err := json.Marshal(config)
	if err != nil {
		return errors.Wrap(err, "")
	}
	err = json.Unmarshal(data, v)
	if err != nil {
		return errors.Wrap(err, "")
	}
	switch v := v.(type) {
	case *deploy.CKDeploy:
		repository.DecodePasswd(v.Conf)
	case *model.ArchiveTableReq:
	}
	return nil
}

func CKDeployHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	if d.Conf.KeeperWithStanalone() {
		task.TaskType = model.TaskTypeKeeperDeploy
		if err := DeployKeeperCluster(task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKDeploy
	}

	if err := DeployCkCluster(task, d); err != nil {
		return err
	}

	// sync table schema when logic cluster exists
	deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)
	if d.Conf.LogicCluster != nil {
		logics, err := repository.Ps.GetLogicClusterbyName(*d.Conf.LogicCluster)
		if err == nil && len(logics) > 0 {
			for _, logic := range logics {
				if cluster, err := repository.Ps.GetClusterbyName(logic); err == nil {
					if clickhouse.SyncLogicTable(cluster, *d.Conf) {
						break
					}
				}
			}
		}
	}
	if err := repository.Ps.Begin(); err != nil {
		return err
	}
	if d.Conf.AuthenticateType != model.SshPasswordSave {
		d.Conf.SshPassword = ""
	}
	d.Conf.Watch(model.ALL_NODES_DEFAULT)
	if err := repository.Ps.CreateCluster(*d.Conf); err != nil {
		_ = repository.Ps.Rollback()
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}
	if d.Conf.LogicCluster != nil {
		logics, err := repository.Ps.GetLogicClusterbyName(*d.Conf.LogicCluster)
		if err != nil {
			if errors.Is(err, repository.ErrRecordNotFound) {
				if !common.ArraySearch(d.Conf.Cluster, logics) {
					logics = append(logics, d.Conf.Cluster)
				}
				_ = repository.Ps.CreateLogicCluster(*d.Conf.LogicCluster, logics)
			} else {
				_ = repository.Ps.Rollback()
				return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
			}
		} else {
			if !common.ArraySearch(d.Conf.Cluster, logics) {
				logics = append(logics, d.Conf.Cluster)
			}
			_ = repository.Ps.UpdateLogicCluster(*d.Conf.LogicCluster, logics)
		}
	}
	_ = repository.Ps.Commit()
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKDestoryHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	common.CloseConns(conf.Hosts)
	if err = DestroyCkCluster(task, d, &conf); err != nil {
		return err
	}

	if d.Conf.KeeperWithStanalone() {
		task.TaskType = model.TaskTypeKeeperDestory
		if err = DestroyKeeperCluster(task, d, &conf); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKDestory
	}

	deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)
	if err = repository.Ps.Begin(); err != nil {
		return err
	}
	if conf.LogicCluster != nil {
		if err = deploy.ClearLogicCluster(conf.Cluster, *conf.LogicCluster, true); err != nil {
			_ = repository.Ps.Rollback()
			return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
		}
	}

	//TODO [L] clear task record and query history
	historys, err := repository.Ps.GetQueryHistoryByCluster(conf.Cluster)
	if err == nil {
		for _, history := range historys {
			_ = repository.Ps.DeleteQueryHistory(history.CheckSum)
		}
	}

	tasks, err := repository.Ps.GetAllTasks()
	if err == nil {
		for _, t := range tasks {
			if t.ClusterName == conf.Cluster {
				//do not delete running task when destory cluster
				if t.Status == model.TaskStatusWaiting || t.Status == model.TaskStatusRunning || t.TaskId == task.TaskId {
					continue
				}
				_ = repository.Ps.DeleteTask(t.TaskId)
			}
		}
	}

	if err = repository.Ps.DeleteCluster(conf.Cluster); err != nil {
		_ = repository.Ps.Rollback()
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}
	_ = repository.Ps.Commit()

	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKDeleteNodeHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	ip := d.Conf.Hosts[0] //which node will be deleted
	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	err = DeleteCkClusterNode(task, &conf, ip)
	if err != nil {
		return err
	}
	common.CloseConns([]string{ip})

	deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)
	if err = repository.Ps.UpdateCluster(conf); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKAddNodeHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	if d.Conf.Keeper == model.ClickhouseKeeper && d.Conf.KeeperConf.Runtime == model.KeeperRuntimeInternal {
		d.Ext.Restart = true
		d.Conf.KeeperConf.KeeperNodes = append(d.Conf.KeeperConf.KeeperNodes, d.Conf.Hosts...)
	}

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	err = AddCkClusterNode(task, &conf, &d)
	if err != nil {
		return err
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	for _, host := range d.Conf.Hosts {
		tmp := &model.CKManClickHouseConfig{
			Hosts:    []string{host},
			Port:     conf.Port,
			Cluster:  conf.Cluster,
			User:     conf.User,
			Password: conf.Password,
		}

		service := clickhouse.NewCkService(tmp)
		if err := service.InitCkService(); err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
		}
		if err := service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
			if common.ExceptionAS(err, common.REPLICA_ALREADY_EXISTS) {
				//Code: 253: Replica /clickhouse/tables/XXX/XXX/replicas/{replica} already exists, clean the znode and  retry
				zkService, err := zookeeper.GetZkService(conf.Cluster)
				if err == nil {
					err = zkService.CleanZoopath(conf, conf.Cluster, conf.Hosts[0], false)
					if err == nil {
						if err = service.FetchSchemerFromOtherNode(conf.Hosts[0]); err != nil {
							log.Logger.Errorf("fetch schema from other node failed again")
						}
					}
				} else {
					log.Logger.Errorf("can't create zookeeper instance:%v", err)
				}
			} else {
				return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
			}
		}
	}
	deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)
	if err = repository.Ps.UpdateCluster(conf); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKUpgradeHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	if d.Conf.KeeperWithStanalone() {
		task.TaskType = model.TaskTypeKeeperUpgrade
		if err = UpgradeKeeperCluster(task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKUpgrade
	}

	err = UpgradeCkCluster(task, d)
	if err != nil {
		return err
	}
	conf.Version = d.Conf.Version

	if err = repository.Ps.UpdateCluster(conf); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKSettingHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	if !d.Ext.ChangeCk {
		deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)
		if err := repository.Ps.UpdateCluster(*d.Conf); err != nil {
			return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
		}
		deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
		return nil
	}

	if d.Conf.KeeperWithStanalone() {
		task.TaskType = model.TaskTypeKeeperSetting
		if err := ConfigKeeperCluster(task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKSetting
	}

	if err := ConfigCkCluster(task, d); err != nil {
		return err
	}

	if err := repository.Ps.Begin(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}
	if d.Conf.AuthenticateType != model.SshPasswordSave {
		d.Conf.SshPassword = ""
	}
	if err := repository.Ps.UpdateCluster(*d.Conf); err != nil {
		_ = repository.Ps.Rollback()
		return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
	}
	if d.Conf.LogicCluster != nil {
		logics, err := repository.Ps.GetLogicClusterbyName(*d.Conf.LogicCluster)
		if err != nil {
			if errors.Is(err, repository.ErrRecordNotFound) {
				logics = append(logics, d.Conf.Cluster)
				_ = repository.Ps.CreateLogicCluster(*d.Conf.LogicCluster, logics)
			} else {
				_ = repository.Ps.Rollback()
				return errors.Wrapf(err, "[%s]", model.NodeStatusStore.EN)
			}
		} else {
			if !common.ArraySearch(d.Conf.Cluster, logics) {
				logics = append(logics, d.Conf.Cluster)
			}
			_ = repository.Ps.UpdateLogicCluster(*d.Conf.LogicCluster, logics)
		}
	}
	_ = repository.Ps.Commit()
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKArchiveHandle(task *model.Task) error {
	var req model.ArchiveTableReq
	if err := UnmarshalConfig(task.DeployConfig, &req); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(task.ClusterName)
	if err != nil {
		return err
	}

	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	t := clickhouse.GetSuitableArchiveAdpt(req.Target)

	hosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	params := clickhouse.NewArchiveParams(hosts, conf, req)
	if err := t.Normalize(params, req); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	if err := t.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	if err := t.Clear(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusExport, model.ALL_NODES_DEFAULT)
	if err := t.Export(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusExport.EN)
	}

	t.Done(conf.Path)
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}
