package runner

import (
	"context"
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

// TaskHandleFunc dispatches to a handler for the task type. The ctx is
// cancelled when the user clicks Stop on the task (see runner.Cancel);
// handlers are expected to check ctx.Err() at phase boundaries via
// checkCancel so that "Stop" is more than just a DB flag flip.
var TaskHandleFunc = map[string]func(ctx context.Context, task *model.Task) error{
	model.TaskTypeCKDeploy:     CKDeployHandle,
	model.TaskTypeCKUpgrade:    CKUpgradeHandle,
	model.TaskTypeCKAddNode:    CKAddNodeHandle,
	model.TaskTypeCKDeleteNode: CKDeleteNodeHandle,
	model.TaskTypeCKDestory:    CKDestoryHandle,
	model.TaskTypeCKSetting:    CKSettingHandle,
	model.TaskTypeCKArchive:    CKArchiveHandle,
	model.TaskTypeCKRebalance:  CKRebalanceHandle,
}

// checkCancel returns ctx.Err() if the task has been cancelled, nil otherwise.
// Call between phases to surface a cooperative stop without sprinkling
// select-default boilerplate everywhere.
func checkCancel(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return errors.Wrap(err, "task cancelled")
	}
	return nil
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
	case *model.RebalanceTableReq:
	}
	return nil
}

func CKDeployHandle(ctx context.Context, task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	if d.Conf.KeeperWithStanalone() {
		task.TaskType = model.TaskTypeKeeperDeploy
		if err := DeployKeeperCluster(ctx, task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKDeploy
	}
	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusWating, model.ALL_NODES_DEFAULT)
	if err := DeployCkCluster(ctx, task, d); err != nil {
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

func CKDestoryHandle(ctx context.Context, task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	common.CloseConns(conf.Hosts)
	if err = DestroyCkCluster(ctx, task, d, &conf); err != nil {
		return err
	}

	if d.Conf.KeeperWithStanalone() && !d.Ext.SkipKeeper {
		if err := checkCancel(ctx); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeKeeperDestory
		if err = DestroyKeeperCluster(ctx, task, d, &conf); err != nil {
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

func CKDeleteNodeHandle(ctx context.Context, task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	ip := d.Conf.Hosts[0] //which node will be deleted
	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	err = DeleteCkClusterNode(ctx, task, &conf, ip)
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

func CKAddNodeHandle(ctx context.Context, task *model.Task) error {
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

	err = AddCkClusterNode(ctx, task, &conf, &d)
	if err != nil {
		return err
	}

	if d.Ext.SourceSchemaHost == "" {
		return errors.New("sourceSchemaHost is empty in deploy ext")
	}

	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	for _, host := range d.Conf.Hosts {
		if err := checkCancel(ctx); err != nil {
			return err
		}
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
		if err := service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost); err != nil {
			if common.ExceptionAS(err, common.REPLICA_ALREADY_EXISTS) {
				//Code: 253: Replica /clickhouse/tables/XXX/XXX/replicas/{replica} already exists, clean the znode and  retry
				zkService, err := zookeeper.GetZkService(conf.Cluster)
				if err == nil {
					err = zkService.CleanZoopath(conf, conf.Cluster, host, false)
					if err == nil {
						if err = service.FetchSchemerFromOtherNode(d.Ext.SourceSchemaHost); err != nil {
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

func CKUpgradeHandle(ctx context.Context, task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	if d.Conf.KeeperWithStanalone() && !d.Ext.SkipKeeper {
		task.TaskType = model.TaskTypeKeeperUpgrade
		if err = UpgradeKeeperCluster(ctx, task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKUpgrade
	}
	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusWating, model.ALL_NODES_DEFAULT)
	err = UpgradeCkCluster(ctx, task, d)
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

func CKSettingHandle(ctx context.Context, task *model.Task) error {
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

	if d.Conf.KeeperWithStanalone() && !d.Ext.SkipKeeper {
		task.TaskType = model.TaskTypeKeeperSetting
		if err := ConfigKeeperCluster(ctx, task, d); err != nil {
			return err
		}
		task.TaskType = model.TaskTypeCKSetting
	}

	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusWating, model.ALL_NODES_DEFAULT)

	if err := ConfigCkCluster(ctx, task, d); err != nil {
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

func CKArchiveHandle(ctx context.Context, task *model.Task) error {
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

	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusClearData, model.ALL_NODES_DEFAULT)
	if err := t.Clear(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusClearData.EN)
	}

	if err := checkCancel(ctx); err != nil {
		return err
	}
	deploy.SetNodeStatus(task, model.NodeStatusExport, model.ALL_NODES_DEFAULT)
	if err := t.Export(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusExport.EN)
	}

	t.Done(conf.Path)
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}

func CKRebalanceHandle(ctx context.Context, task *model.Task) error {
	var req model.RebalanceTableReq
	if err := UnmarshalConfig(task.DeployConfig, &req); err != nil {
		return err
	}

	conf, err := repository.Ps.GetClusterbyName(task.ClusterName)
	if err != nil {
		return err
	}

	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if len(conf.Shards) > 1 {
		onStep := func(s model.Internationalization) { deploy.SetTaskStep(task, s) }
		if err := clickhouse.RebalanceCluster(ctx, &conf, req.RTables, req.ExceptMaxShard, onStep); err != nil {
			return errors.Wrap(err, "rebalance")
		}
	}
	deploy.SetNodeStatus(task, model.NodeStatusDone, model.ALL_NODES_DEFAULT)
	return nil
}
