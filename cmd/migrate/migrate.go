package migrate

import (
	"fmt"
	"io"
	"os"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
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

var (
	psrc repository.PersistentMgr
	pdst repository.PersistentMgr
)

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

func Migrate() error {
	clusters, err := psrc.GetAllClusters()
	if err != nil {
		return err
	}

	if len(clusters) == 0 {
		log.Logger.Warnf("clusters have 0 records, will migrate nothing")
	}

	logics, err := psrc.GetAllLogicClusters()
	if err != nil {
		return err
	}

	historys, err := psrc.GetAllQueryHistory()
	if err != nil {
		return err
	}

	tasks, err := psrc.GetAllTasks()
	if err != nil {
		return err
	}

	if err = pdst.Begin(); err != nil {
		return errors.Wrap(err, "")
	}
	for _, cluster := range clusters {
		err = pdst.CreateCluster(cluster)
		if err != nil {
			_ = pdst.Rollback()
			return errors.Wrap(err, "")
		}
	}
	for logic, physics := range logics {
		err = pdst.CreateLogicCluster(logic, physics)
		if err != nil {
			_ = pdst.Rollback()
			return errors.Wrap(err, "")
		}
	}

	for _, v := range historys {
		err = pdst.CreateQueryHistory(v)
		if err != nil {
			_ = pdst.Rollback()
			return errors.Wrap(err, "")
		}
	}

	for _, v := range tasks {
		err = pdst.CreateTask(v)
		if err != nil {
			_ = pdst.Rollback()
			return errors.Wrap(err, "")
		}
	}

	if err = pdst.Commit(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func MigrateHandle(conf string) {
	config, err := ParseConfig(conf)
	if err != nil {
		fmt.Printf("parse config file %s failed: %v\n", conf, err)
		return
	}
	psrc, err = PersistentCheck(config, config.Source)
	if err != nil {
		fmt.Printf("source [%s] err: %v\n", config.Source, err)
		return
	}
	pdst, err = PersistentCheck(config, config.Target)
	if err != nil {
		fmt.Printf("target [%s] err: %v\n", config.Target, err)
		return
	}

	if err = Migrate(); err != nil {
		fmt.Printf("migrate failed: %v\n", err)
		return
	}
	fmt.Printf("Form [%s] migrate to [%s] success!\n", config.Source, config.Target)
}
