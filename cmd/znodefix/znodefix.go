package main

/*
znodefix  --cluster=abc --config=/etc/ckman/conf/ckman.hjson --node=192.168.110.8 --dryrun
*/

import (
	"flag"
	"fmt"
	"os"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
	"github.com/housepower/ckman/service/zookeeper"
)

type CmdOptions struct {
	ShowVer     bool
	ClusterName string
	Node        string
	ConfigFile  string
	Dryrun      bool
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
)

func init() {
	cmdOps = CmdOptions{
		ShowVer:    false,
		ConfigFile: "/etc/ckman/conf/migrate.hjson",
		Dryrun:     false,
	}
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ConfigFile, "config")
	common.EnvBoolVar(&cmdOps.Dryrun, "dryrun")
	common.EnvStringVar(&cmdOps.Node, "node")
	common.EnvStringVar(&cmdOps.ClusterName, "cluster")

	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ClusterName, "cluster", cmdOps.ClusterName, "fix znode on which cluster")
	flag.StringVar(&cmdOps.Node, "node", cmdOps.Node, "clean which znode on zookeeper")
	flag.StringVar(&cmdOps.ConfigFile, "config", cmdOps.ConfigFile, "ckman config file")
	flag.BoolVar(&cmdOps.Dryrun, "dryrun", cmdOps.Dryrun, "only list which znode to clean")
	flag.Parse()
}

func main() {
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if err := config.ParseConfigFile(cmdOps.ConfigFile, ""); err != nil {
		fmt.Printf("Parse config file %s fail: %v\n", cmdOps.ConfigFile, err)
		os.Exit(1)
	}

	var conf config.CKManConfig
	_ = common.DeepCopyByGob(&conf, &config.GlobalConfig)
	err := common.Gsypt.Unmarshal(&config.GlobalConfig)
	if err != nil {
		fmt.Printf("gsypt config file %s fail: %v\n", cmdOps.ConfigFile, err)
		os.Exit(1)
	}
	log.InitLoggerConsole()
	err = repository.InitPersistent()
	if err != nil {
		log.Logger.Fatalf("init persistent failed:%v", err)
	}

	cluster, err := repository.Ps.GetClusterbyName(cmdOps.ClusterName)
	if err != nil {
		log.Logger.Fatalf("get cluster %s failed:%v", cmdOps.ClusterName, err)
	}
	service, err := zookeeper.NewZkService(cluster.ZkNodes, cluster.ZkPort)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}
	err = service.CleanZoopath(cluster, cmdOps.ClusterName, cmdOps.Node, cmdOps.Dryrun)
	if err != nil {
		log.Logger.Fatalf("clean zoopath error:%v", err)
	}
	log.Logger.Info("znode fixed successfully")
}
