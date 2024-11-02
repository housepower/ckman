package znodes

import (
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/zookeeper"
)

type ZSuballOpts struct {
	ConfigFile  string
	ClusterName string
	Node        string
	Dryrun      bool
}

func SuballHandle(opts ZSuballOpts) {
	if err := config.ParseConfigFile(opts.ConfigFile, ""); err != nil {
		log.Logger.Fatalf("Parse config file %s fail: %v\n", opts.ConfigFile, err)
	}

	var conf config.CKManConfig
	_ = common.DeepCopyByGob(&conf, &config.GlobalConfig)
	err := common.Gsypt.Unmarshal(&config.GlobalConfig)
	if err != nil {
		log.Logger.Fatalf("gsypt config file %s fail: %v\n", opts.ConfigFile, err)
	}
	err = repository.InitPersistent()
	if err != nil {
		log.Logger.Fatalf("init persistent failed:%v", err)
	}

	cluster, err := repository.Ps.GetClusterbyName(opts.ClusterName)
	if err != nil {
		log.Logger.Fatalf("get cluster %s failed:%v", opts.ClusterName, err)
	}
	nodes, port := zookeeper.GetZkInfo(&cluster)
	service, err := zookeeper.NewZkService(nodes, port, 300)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}
	err = service.CleanZoopath(cluster, opts.ClusterName, opts.Node, opts.Dryrun)
	if err != nil {
		log.Logger.Fatalf("clean zoopath error:%v", err)
	}
	log.Logger.Info("znode fixed successfully")
}
