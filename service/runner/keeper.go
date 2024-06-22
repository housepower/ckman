package runner

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

func DeployKeeperCluster(task *model.Task, d deploy.CKDeploy) error {
	kd := deploy.NewKeeperDeploy(*d.Conf, d.Packages)
	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if err := kd.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusPrepare, model.ALL_NODES_DEFAULT)
	if err := kd.Prepare(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusPrepare.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusInstall, model.ALL_NODES_DEFAULT)
	if err := kd.Install(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInstall.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusConfig, model.ALL_NODES_DEFAULT)
	if err := kd.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusStart, model.ALL_NODES_DEFAULT)
	if err := kd.Start(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusStart.EN)
	}

	deploy.SetNodeStatus(task, model.NodeStatusCheck, model.ALL_NODES_DEFAULT)
	if err := kd.Check(30); err != nil {
		//return errors.Wrapf(err, "[%s]", model.NodeStatusCheck.EN)
		deploy.SetTaskStatus(task, model.TaskStatusFailed, err.Error())
	}
	return nil
}

func DestroyKeeperCluster(task *model.Task, d deploy.CKDeploy, conf *model.CKManClickHouseConfig) error {
	kd := deploy.NewKeeperDeploy(*d.Conf, d.Packages)
	deploy.SetNodeStatus(task, model.NodeStatusStop, model.ALL_NODES_DEFAULT)
	_ = kd.Stop()

	deploy.SetNodeStatus(task, model.NodeStatusUninstall, model.ALL_NODES_DEFAULT)
	if err := kd.Uninstall(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInstall.EN)
	}

	return nil
}

func UpgradeKeeperCluster(task *model.Task, d deploy.CKDeploy) error {
	kd := deploy.NewKeeperDeploy(*d.Conf, d.Packages)
	switch d.Ext.Policy {
	case model.PolicyRolling:
		var rd deploy.KeeperDeploy
		common.DeepCopyByGob(&rd, kd)
		for _, host := range d.Conf.KeeperConf.KeeperNodes {
			rd.Conf.KeeperConf.KeeperNodes = []string{host}
			if err := upgradeKeeperPackage(task, rd, model.MaxTimeOut); err != nil {
				return err
			}
		}
	case model.PolicyFull:
		err := upgradeKeeperPackage(task, *kd, 30)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("not support policy %s yet", d.Ext.Policy)
	}

	return nil
}

func upgradeKeeperPackage(task *model.Task, d deploy.KeeperDeploy, timeout int) error {
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

func ConfigKeeperCluster(task *model.Task, d deploy.CKDeploy) error {
	kd := deploy.NewKeeperDeploy(*d.Conf, d.Packages)
	deploy.SetNodeStatus(task, model.NodeStatusInit, model.ALL_NODES_DEFAULT)
	if err := kd.Init(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusInit.EN)
	}
	deploy.SetNodeStatus(task, model.NodeStatusConfig, model.ALL_NODES_DEFAULT)
	if err := kd.Config(); err != nil {
		return errors.Wrapf(err, "[%s]", model.NodeStatusConfig.EN)
	}

	if kd.Ext.Restart {
		switch d.Ext.Policy {
		case model.PolicyRolling:
			var rd deploy.CKDeploy
			common.DeepCopyByGob(&rd, kd)
			for _, host := range rd.Conf.KeeperConf.KeeperNodes {
				deploy.SetNodeStatus(task, model.NodeStatusRestart, host)
				rd.Conf.KeeperConf.KeeperNodes = []string{host}
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
			err := kd.Restart()
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
