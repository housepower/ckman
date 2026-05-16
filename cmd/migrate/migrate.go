package migrate

import (
	"fmt"
	"io"
	"os"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
	"github.com/pkg/errors"
)

type PersistentConfig struct {
	Policy string
	Config map[string]interface{}
}

type MigrateConfig struct {
	Source string
	Target string
	PsConf map[string]PersistentConfig `json:"persistent_config"`
}

func ParseConfig(conf string) (MigrateConfig, error) {
	var config MigrateConfig
	f, err := os.Open(conf)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	if len(data) == 0 {
		return MigrateConfig{}, errors.New("empty config file")
	}
	err = hjson.Unmarshal(data, &config)
	if err != nil {
		return MigrateConfig{}, errors.Wrap(err, "")
	}
	return config, nil
}

func PersistentCheck(config MigrateConfig, typo string) (repository.PersistentMgr, error) {
	var ps repository.PersistentMgr
	conf, ok := config.PsConf[typo]
	if !ok {
		return nil, errors.Errorf("empty persistent config %s", config.Source)
	}
	ps = repository.GetPersistentByName(conf.Policy)
	if ps == nil {
		return nil, errors.Errorf("invalid persistent policy: %s", conf.Policy)
	}
	pcfg := ps.UnmarshalConfig(conf.Config)
	if err := ps.Init(pcfg); err != nil {
		return nil, errors.Errorf("init persistent failed: %v", err)
	}
	return ps, nil
}

func MigrateBetween(src, dst repository.PersistentMgr) error {
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

func MigrateHandle(conf string) {
	config, err := ParseConfig(conf)
	if err != nil {
		fmt.Printf("parse config file %s failed: %v\n", conf, err)
		return
	}
	src, err := PersistentCheck(config, config.Source)
	if err != nil {
		fmt.Printf("source [%s] err: %v\n", config.Source, err)
		return
	}
	dst, err := PersistentCheck(config, config.Target)
	if err != nil {
		fmt.Printf("target [%s] err: %v\n", config.Target, err)
		return
	}
	if err := MigrateBetween(src, dst); err != nil {
		fmt.Printf("migrate failed: %v\n", err)
		return
	}
	fmt.Printf("From [%s] migrate to [%s] success!\n", config.Source, config.Target)
}
