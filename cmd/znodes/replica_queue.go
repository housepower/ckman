package znodes

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/housepower/ckman/service/cron"
	"github.com/housepower/ckman/service/zookeeper"
)

type ZReplicaQueueOpts struct {
	ClusterName string
	ConfigFile  string
	Dryrun      bool
	NumTries    int
}

func ReplicaQueueHandle(opts ZReplicaQueueOpts) {
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

	ckService := clickhouse.NewCkService(&cluster)
	if err = ckService.InitCkService(); err != nil {
		log.Logger.Fatalf("[%s]init clickhouse service failed: %v", cluster.Cluster, err)
	}
	nodes, port := zookeeper.GetZkInfo(&cluster)
	zkService, err := zookeeper.NewZkService(nodes, port)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}
	// remove replica_queue in zookeeper
	znodes, err := cron.GetReplicaQueueZnodes(ckService, opts.NumTries)
	if err != nil {
		log.Logger.Infof("[%s]remove replica_queue from zookeeper failed: %v", cluster.Cluster, err)
	}
	fmt.Println()
	if opts.Dryrun {
		for i, znode := range znodes {
			if i == 1000 {
				break
			}
			fmt.Printf("[%4d]%s\n", i, znode)
		}
		if len(znodes) > 1000 {
			fmt.Printf("\n %d znodes, only show first 1000", len(znodes))
		}
	} else {
		deleted, notexist := cron.RemoveZnodes(zkService, znodes)
		fmt.Printf("[%s]remove [%d] replica_queue from zookeeper success, [%d] already deleted\n", cluster.Cluster, deleted, notexist)
	}
}
