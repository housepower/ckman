package metacache

import (
	"fmt"
	"os"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
)

type MetacacheOpts struct {
	ClusterName string
	ConfigFile  string
	Dryrun      bool
}

func MetacacheHandle(opts MetacacheOpts) {
	if err := config.ParseConfigFile(opts.ConfigFile, ""); err != nil {
		fmt.Printf("Parse config file %s fail: %v\n", opts.ConfigFile, err)
		os.Exit(-1)
	}

	var conf config.CKManConfig
	_ = common.DeepCopyByGob(&conf, &config.GlobalConfig)
	err := common.Gsypt.Unmarshal(&config.GlobalConfig)
	if err != nil {
		fmt.Printf("gsypt config file %s fail: %v\n", opts.ConfigFile, err)
		os.Exit(-2)
	}
	err = repository.InitPersistent()
	if err != nil {
		fmt.Printf("init persistent failed:%v\n", err)
		os.Exit(-3)
	}

	cluster, err := repository.Ps.GetClusterbyName(opts.ClusterName)
	if err != nil {
		fmt.Printf("get cluster %s failed:%v\n", opts.ClusterName, err)
		os.Exit(-4)
	}

	if common.CompareClickHouseVersion(cluster.Version, "22.4.x") < 0 {
		fmt.Printf("cluster %s version %s not support metacache\n", opts.ClusterName, cluster.Version)
		os.Exit(-5)
	}

	ckConns := make(map[string]*common.Conn, len(cluster.Hosts))
	for _, host := range cluster.Hosts {
		conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, cluster.GetConnOption())
		if err == nil {
			ckConns[host] = conn
		}
	}

	if len(ckConns) == 0 {
		fmt.Printf("connect to cluster %s failed\n", opts.ClusterName)
		os.Exit(-6)
	}

	i := 0
	for host, conn := range ckConns {
		_, dbTbls, err := common.GetMergeTreeTables("MergeTree", "", conn)
		if err != nil {
			fmt.Printf("[%s]get tables of cluster %s failed:%v\n", host, opts.ClusterName, err)
			continue
		}
		for db, tbls := range dbTbls {
			for _, table := range tbls {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY SETTING use_metadata_cache = true", db, table)
				log.Logger.Debugf("[%s][%s]%s", opts.ClusterName, host, query)
				if opts.Dryrun {
					fmt.Printf("[%4d][%s][%s]%s\n", i, opts.ClusterName, host, query)
					i++
				} else {
					if err = conn.Exec(query); err != nil {
						fmt.Printf("[%s]%s failed:%v\n", host, query, err)
					}
				}
			}
		}
	}
}
