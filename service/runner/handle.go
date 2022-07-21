package runner

import (
	"encoding/json"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/pkg/errors"
)

var TaskHandleFunc = map[string]func(task *model.Task) error{
	model.TaskTypeCKDeploy:     CKDeployHandle,
	model.TaskTypeCKUpgrade:    CKUpgradeHandle,
	model.TaskTypeCKAddNode:    CKAddNodeHandle,
	model.TaskTypeCKDeleteNode: CKDeleteNodeHandle,
	model.TaskTypeCKDestory:    CKDestoryHandle,
	model.TaskTypeCKSetting:    CKSettingHandle,
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
	switch v.(type) {
	case *deploy.CKDeploy:
		d := v.(*deploy.CKDeploy)
		repository.DecodePasswd(d.Conf)
	case *deploy.ZKDeploy:
	}
	return nil
}

func CKDeployHandle(task *model.Task) error {
	var d deploy.CKDeploy
	if err := UnmarshalConfig(task.DeployConfig, &d); err != nil {
		return err
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

	conf, err := repository.Ps.GetClusterbyName(d.Conf.Cluster)
	if err != nil {
		return nil
	}

	err = AddCkClusterNode(task, &conf, &d)
	if err != nil {
		return err
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfigExt, model.ALL_NODES_DEFAULT)
	tmp := &model.CKManClickHouseConfig{
		Hosts:    d.Conf.Hosts,
		Port:     conf.Port,
		Cluster:  conf.Cluster,
		User:     conf.User,
		Password: conf.Password,
	}

	service := clickhouse.NewCkService(tmp)
	if err := service.InitCkService(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
	}
	if err := service.FetchSchemerFromOtherNode(conf.Hosts[0], conf.Password); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfigExt.EN)
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

	if err := ConfigCkCluster(task, d); err != nil {
		return err
	}

	deploy.SetNodeStatus(task, model.NodeStatusStore, model.ALL_NODES_DEFAULT)

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
