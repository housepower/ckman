package repository

import (
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

// MigrateBetween 把 src 的全量数据搬到 dst，覆盖所有 7 类实体。
// 整个 dst 写入流程在单个事务中执行：任意一步失败都回滚整个迁移。
//
// 注意：本函数位于 repository 包，避免 repository 子包反向 import cmd 层。
// cmd/migrate 仍可调用此函数完成 ckmanctl migrate 子命令的工作。
// repository/sqlite 启动期迁移也复用同一函数。
func MigrateBetween(src, dst PersistentMgr) error {
	clusters, err := src.GetAllClusters()
	if err != nil {
		return errors.Wrap(err, "get clusters")
	}
	if len(clusters) == 0 {
		log.Logger.Warnf("clusters have 0 records, will migrate nothing")
	}

	logics, err := src.GetAllLogicClusters()
	if err != nil {
		return errors.Wrap(err, "get logics")
	}
	historys, err := src.GetAllQueryHistory()
	if err != nil {
		return errors.Wrap(err, "get query history")
	}
	tasks, err := src.GetAllTasks()
	if err != nil {
		return errors.Wrap(err, "get tasks")
	}
	var backups []model.Backup
	for _, conf := range clusters {
		b, err := src.GetAllBackups(conf.Cluster)
		if err != nil {
			return errors.Wrap(err, "get backups")
		}
		backups = append(backups, b...)
	}
	policies, err := src.GetAllBackupPolicies()
	if err != nil {
		return errors.Wrap(err, "get backup policies")
	}
	runs, err := src.GetAllBackupRuns()
	if err != nil {
		return errors.Wrap(err, "get backup runs")
	}

	if err = dst.Begin(); err != nil {
		return errors.Wrap(err, "begin")
	}
	rollback := func(e error) error {
		_ = dst.Rollback()
		return errors.Wrap(e, "")
	}

	for _, cluster := range clusters {
		if err = dst.CreateCluster(cluster); err != nil {
			return rollback(err)
		}
	}
	for logic, physics := range logics {
		if err = dst.CreateLogicCluster(logic, physics); err != nil {
			return rollback(err)
		}
	}
	for _, v := range historys {
		if err = dst.CreateQueryHistory(v); err != nil {
			return rollback(err)
		}
	}
	for _, v := range tasks {
		if err = dst.CreateTask(v); err != nil {
			return rollback(err)
		}
	}
	for _, v := range backups {
		if err = dst.CreateBackup(v); err != nil {
			return rollback(err)
		}
	}
	for _, p := range policies {
		if err = dst.CreateBackupPolicy(p); err != nil {
			return rollback(err)
		}
	}
	for _, r := range runs {
		if err = dst.CreateBackupRun(r); err != nil {
			return rollback(err)
		}
	}

	if err = dst.Commit(); err != nil {
		return rollback(err)
	}
	return nil
}
