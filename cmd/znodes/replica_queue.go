package znodes

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/cron"
	"github.com/housepower/ckman/service/zookeeper"
)

type ZReplicaQueueOpts struct {
	ClusterName    string
	ConfigFile     string
	Dryrun         bool
	SessionTimeout int
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
	nodes, port := zookeeper.GetZkInfo(&cluster)
	zkService, err := zookeeper.NewZkService(nodes, port, 300)
	if err != nil {
		log.Logger.Fatalf("can't create zookeeper instance:%v", err)
	}
	n := 0
	var allZnodes []string
	for _, host := range cluster.Hosts {
		conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, cluster.GetConnOption())
		if err != nil {
			log.Logger.Warnf("can't connect to clickhouse:%s", host)
			continue
		}
		// remove replica_queue in zookeeper
		znodes, err := cron.GetReplicaQueueZnodes(host, conn)
		if err != nil {
			log.Logger.Infof("[%s]remove replica_queue from zookeeper failed: %v", cluster.Cluster, err)
		}
		if opts.Dryrun {
			n += len(znodes)
			if len(allZnodes) <= 1000 {
				allZnodes = append(allZnodes, znodes...)
			}
		} else {
			deleted, notexist := cron.RemoveZnodes(zkService, znodes, true)
			fmt.Printf("[%s][%s]remove [%d] replica_queue from zookeeper success, [%d] already deleted\n", host, cluster.Cluster, deleted, notexist)
		}
	}
	if opts.Dryrun {
		for i, znode := range allZnodes {
			if i >= 1000 {
				break
			}
			fmt.Printf("[%4d]%s\n", i+1, znode)
		}
		fmt.Printf("\n%d znodes, only show first 1000 rows\n", n)
	}
}
