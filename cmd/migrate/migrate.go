package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/hjson/hjson-go/v4"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	"github.com/pkg/errors"
)

/*
*
auto migrate cluster config between diffrent persistent policy
eg.

	migrate --config=/etc/ckman/conf/migrate.hjson
*/
type CmdOptions struct {
	ShowVer    bool
	ConfigFile string
}

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
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	psrc           repository.PersistentMgr
	pdst           repository.PersistentMgr
)

func initCmdOptions() {
	cmdOps = CmdOptions{
		ShowVer:    false,
		ConfigFile: "/etc/ckman/conf/migrate.hjson",
	}
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ConfigFile, "config")

	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ConfigFile, "config", cmdOps.ConfigFile, "migrate config file")
	flag.Parse()
}

func ParseConfig() (MigrateConfig, error) {
	var config MigrateConfig
	f, err := os.Open(cmdOps.ConfigFile)
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

func main() {
	log.InitLoggerConsole()
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}

	config, err := ParseConfig()
	if err != nil {
		log.Logger.Fatalf("parse config file %s failed: %v", cmdOps.ConfigFile, err)
	}
	psrc, err = PersistentCheck(config, config.Source)
	if err != nil {
		log.Logger.Fatalf("source [%s] err: %v", config.Source, err)
	}
	pdst, err = PersistentCheck(config, config.Target)
	if err != nil {
		log.Logger.Fatalf("target [%s] err: %v", config.Target, err)
	}

	if err = Migrate(); err != nil {
		log.Logger.Fatalf("migrate failed: %v", err)
	}
	log.Logger.Infof("Form [%s] migrate to [%s] success!", config.Source, config.Target)
}
