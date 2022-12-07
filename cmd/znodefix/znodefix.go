package main

/*
znodefix  --cluster=abc --config=/etc/ckman/conf/ckman.hjson --node=192.168.110.8 --dryrun
*/

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/go-zookeeper/zk"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	_ "github.com/housepower/ckman/repository/dm8"
	_ "github.com/housepower/ckman/repository/local"
	_ "github.com/housepower/ckman/repository/mysql"
	_ "github.com/housepower/ckman/repository/postgres"
	"github.com/housepower/ckman/service/zookeeper"
	"github.com/pkg/errors"
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
	err = CleanZoopath(cluster)
	if err != nil {
		log.Logger.Fatalf("clean zoopath error:%v", err)
	}
	log.Logger.Info("znode fixed successfully")
}

func CleanZoopath(conf model.CKManClickHouseConfig) error {
	svr, err := zookeeper.NewZkService(conf.ZkNodes, conf.ZkPort)
	if err != nil {
		return err
	}

	root := fmt.Sprintf("/clickhouse/tables/%s", cmdOps.ClusterName)
	return clean(svr, root)
}

func clean(svr *zookeeper.ZkService, znode string) error {
	_, stat, err := svr.Conn.Get(znode)
	if errors.Is(err, zk.ErrNoNode) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, znode)
	}
	base := path.Base(znode)
	if base == cmdOps.Node {
		if cmdOps.Dryrun {
			fmt.Println(znode)
		} else {
			err = svr.DeleteAll(znode)
			if err != nil {
				fmt.Printf("znode %s delete failed: %v\n", znode, err)
			} else {
				fmt.Printf("znode %s delete success\n", znode)
			}
		}
	} else if stat.NumChildren > 0 {
		children, _, err := svr.Conn.Children(znode)
		if err != nil {
			return errors.Wrap(err, znode)
		}

		for _, child := range children {
			subnode := path.Join(znode, child)
			err := clean(svr, subnode)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
