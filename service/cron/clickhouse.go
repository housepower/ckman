package cron

import (
	"context"
	"fmt"
	"strings"

	client "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/deploy"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse"
	"github.com/pkg/errors"
)

func SyncLogicSchema() error {
	log.Logger.Debugf("sync logic schema task triggered")
	logics, err := repository.Ps.GetAllLogicClusters()
	if err != nil {
		log.Logger.Errorf("get logic cluster failed:%v", err)
		return err
	}
	if len(logics) == 0 {
		return nil
	}

	//logics is a map, k: logicName, v: clusters
	for _, clusters := range logics {
		needCreateTable := make(map[string][]string)
		for _, cluster := range clusters {
			if deploy.HasEffectiveTasks(cluster) {
				//do not deal all logic cluster
				log.Logger.Debugf("cluster %s has effective tasks running, ignore sync logic schema job", cluster)
				break
			}
			conf, err := repository.Ps.GetClusterbyName(cluster)
			if err != nil {
				continue
			}
			ckService := clickhouse.NewCkService(&conf)
			err = ckService.InitCkService()
			if err != nil {
				continue
			}

			query := fmt.Sprintf(`SELECT
    database,
    name,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[4] AS local
FROM system.tables
WHERE match(engine, 'Distributed') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')) 
AND (cluster != '%s')`, cluster)
			log.Logger.Debugf("[%s] %s", cluster, query)
			rows, err := ckService.Conn.Query(context.Background(), query)
			if err != nil {
				continue
			}
			defer rows.Close()
			for rows.Next() {
				var database, table, logic, local string
				_ = rows.Scan(&database, &table, &logic, &local)
				err = syncLogicbyTable(clusters, database, local)
				if err != nil {
					var exception *client.Exception
					if errors.As(err, &exception) {
						if exception.Code == 60 {
							//means local table is not exist, will auto sync schema
							needCreateTable[cluster] = clusters
							log.Logger.Infof("[%s]table %s.%s may not exists on one of cluster %v, need to auto create", cluster, database, local, clusters)
						}
					} else {
						log.Logger.Errorf("logic %s table %s.%s sync logic table failed: %v", cluster, database, local, err)
						continue
					}
				}
			}
		}

		//needCreateTable is a map, k is base cluster, v is physic clusters
		for k, v := range needCreateTable {
			for _, cluster := range v {
				if k == cluster {
					continue
				}
				// sync cluster's table with k
				conf, err1 := repository.Ps.GetClusterbyName(k)
				con, err2 := repository.Ps.GetClusterbyName(cluster)
				if err1 == nil && err2 == nil {
					clickhouse.SyncLogicTable(conf, con)
				}
			}
		}
	}
	return nil
}

func syncLogicbyTable(clusters []string, database, localTable string) error {
	tableLists := make(map[string]common.Map)
	for _, cluster := range clusters {
		conf, err := repository.Ps.GetClusterbyName(cluster)
		if err != nil {
			return err
		}
		ckService := clickhouse.NewCkService(&conf)
		if err = ckService.InitCkService(); err != nil {
			return err
		}
		query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, localTable)
		rows, err := ckService.Conn.Query(context.Background(), query)
		if err != nil {
			return errors.Wrap(err, "")
		}
		defer rows.Close()
		tblMap := make(common.Map)
		for rows.Next() {
			var name, typ string
			if err = rows.Scan(&name, &typ); err != nil {
				return errors.Wrap(err, "")
			}
			tblMap[name] = typ
		}
		tableLists[cluster] = tblMap
	}

	allCols := make(common.Map)
	for _, cols := range tableLists {
		allCols = allCols.Union(cols).(common.Map)
	}

	needAlter := false
	for _, cols := range tableLists {
		if len(allCols) > len(cols) {
			needAlter = true
		}
	}
	if needAlter {
		log.Logger.Debugf("need alter table, table %s.%s have different columns on logic cluster", database, localTable)
		for cluster, cols := range tableLists {
			needAdds := allCols.Difference(cols).(common.Map)
			conf, err := repository.Ps.GetClusterbyName(cluster)
			if err != nil {
				return err
			}
			ckService := clickhouse.NewCkService(&conf)
			if err = ckService.InitCkService(); err != nil {
				return err
			}
			for k, v := range needAdds {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS `%s` %s", database, localTable, cluster, k, v)
				log.Logger.Debugf("query:%s", query)
				err = ckService.Conn.Exec(context.Background(), query)
				if err != nil {
					return errors.Wrap(err, "")
				}
			}

			if len(needAdds) == 0 {
				query := fmt.Sprintf("SELECT table, count() from system.columns WHERE database = '%s' AND table in ('%s%s', '%s%s') group by table", database, common.ClickHouseDistributedTablePrefix, localTable, common.ClickHouseDistTableOnLogicPrefix, localTable)
				rows, err := ckService.Conn.Query(context.Background(), query)
				if err != nil {
					return errors.Wrap(err, "")
				}
				defer rows.Close()
				needAlterDist := false
				for rows.Next() {
					var table string
					var count int
					if err = rows.Scan(&table, &count); err != nil {
						return errors.Wrap(err, "")
					}
					needAlterDist = (count != len(allCols))
					if needAlterDist {
						break
					}
				}
				if !needAlterDist {
					continue
				}
			}
			deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC",
				database, common.ClickHouseDistributedTablePrefix, localTable, cluster)
			log.Logger.Debugf(deleteSql)
			if err = ckService.Conn.Exec(context.Background(), deleteSql); err != nil {
				return errors.Wrap(err, "")
			}

			create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
				database, common.ClickHouseDistributedTablePrefix, localTable, cluster, database, localTable,
				cluster, database, localTable)
			log.Logger.Debugf(create)
			if err = ckService.Conn.Exec(context.Background(), create); err != nil {
				return errors.Wrap(err, "")
			}

			if conf.LogicCluster != nil {
				distParams := model.DistLogicTblParams{
					Database:     database,
					TableName:    localTable,
					ClusterName:  cluster,
					LogicCluster: *conf.LogicCluster,
				}
				if err = ckService.DeleteDistTblOnLogic(&distParams); err != nil {
					return err
				}
				if err = ckService.CreateDistTblOnLogic(&distParams); err != nil {
					return err
				}
			}
		}
	}
	// } else {
	// 	//FIXME: maybe distributed table not the same with local table
	// }

	return nil
}

func WatchClusterStatus() error {
	log.Logger.Debugf("watch cluster status task triggered")
	clusters, err := repository.Ps.GetAllClusters()
	if err != nil {
		return err
	}
	for _, cluster := range clusters {
		if !strings.Contains(cluster.PkgType, "tgz") {
			continue
		}
		if deploy.HasEffectiveTasks(cluster.Cluster) {
			log.Logger.Debugf("cluster %s has effective tasks running, ignore watch status job", cluster.Cluster)
			continue
		}
		for _, shard := range cluster.Shards {
			for _, replica := range shard.Replicas {
				if replica.Watch {
					if _, err := common.ConnectClickHouse(replica.Ip, cluster.Port, model.ClickHouseDefaultDB, model.ClickHouseDefaultUser, cluster.Password); err == nil {
						continue
					}
					log.Logger.Infof("cluster %s, node %s is watching required, try to restart ...", cluster.Cluster, replica.Ip)
					d := deploy.NewCkDeploy(cluster)
					d.Conf.Hosts = []string{replica.Ip}
					_ = d.Start()
				}
			}
		}
	}
	return nil
}

func SyncDistSchema() error {
	log.Logger.Debugf("sync distributed schema task triggered")
	clusters, err := repository.Ps.GetAllClusters()
	if err != nil {
		log.Logger.Errorf("get clusters failed: %v", err)
		return err
	}
	for _, conf := range clusters {
		if deploy.HasEffectiveTasks(conf.Cluster) {
			log.Logger.Debugf("cluster %s has effective tasks running, ignore sync distributed schema job", conf.Cluster)
			continue
		}
		initCKConns(conf)
		ckService := clickhouse.NewCkService(&conf)
		err := ckService.InitCkService()
		if err != nil {
			continue
		}
		query := `SELECT
    database,
    name,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[4] AS local
FROM system.tables
WHERE match(engine, 'Distributed') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))`
		rows, err := ckService.Conn.Query(context.Background(), query)
		if err != nil {
			continue
		}
		defer rows.Close()
		for rows.Next() {
			var database, table, cluster, local string
			_ = rows.Scan(&database, &table, &cluster, &local)
			if cluster != conf.Cluster {
				//ignore logic table
				continue
			}
			err := syncDistTable(table, local, database, conf, ckService)
			if err != nil {
				log.Logger.Warnf("[%s]sync distributed table schema failed: %v", conf.Cluster, err)
				continue
			}
		}
	}
	return nil
}

func syncDistTable(distTable, localTable, database string, conf model.CKManClickHouseConfig, ckServide *clickhouse.CkService) error {
	tableLists := make(map[string]common.Map)
	dbLists := make(map[string]driver.Conn)
	for _, host := range conf.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			continue
		}
		query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, localTable)
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			return errors.Wrap(err, host)
		}
		defer rows.Close()
		tblMap := make(common.Map)
		for rows.Next() {
			var name, typ string
			if err = rows.Scan(&name, &typ); err != nil {
				return errors.Wrap(err, host)
			}
			tblMap[name] = typ
		}
		tableLists[host] = tblMap
		dbLists[host] = conn
	}

	allCols := make(common.Map)
	for _, cols := range tableLists {
		allCols = allCols.Union(cols).(common.Map)
	}

	needAlter := false
	for _, cols := range tableLists {
		if len(allCols) > len(cols) {
			needAlter = true
		}
	}

	//sync local table
	if needAlter {
		log.Logger.Debugf("need alter table, table %s.%s have different columns on cluster %s", database, localTable, conf.Cluster)
		for host, cols := range tableLists {
			needAdds := allCols.Difference(cols).(common.Map)
			conn := dbLists[host]
			for k, v := range needAdds {
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN IF NOT EXISTS `%s` %s", database, localTable, k, v)
				log.Logger.Debug(query)
				err := conn.Exec(context.Background(), query)
				if err != nil {
					return errors.Wrap(err, host)
				}
			}
		}
	}

	//sync dist table
	var needAlterDist bool
	query := fmt.Sprintf("SELECT table, count() from system.columns WHERE database = '%s' AND table = '%s' group by table", database, distTable)
	rows, err := ckServide.Conn.Query(context.Background(), query)
	if err != nil {
		return errors.Wrap(err, "")
	}
	defer rows.Close()
	for rows.Next() {
		var table string
		var count uint64
		if err = rows.Scan(&table, &count); err != nil {
			return errors.Wrap(err, "")
		}
		needAlterDist = (int(count) != len(allCols))
	}
	for host, conn := range dbLists {
		if needAlterDist {
			deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` SYNC",
				database, distTable)
			log.Logger.Debugf(deleteSql)
			if err := conn.Exec(context.Background(), deleteSql); err != nil {
				return errors.Wrap(err, host)
			}

			create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
				database, distTable, database, localTable,
				conf.Cluster, database, localTable)
			log.Logger.Debugf(create)
			if err := conn.Exec(context.Background(), create); err != nil {
				return errors.Wrap(err, host)
			}
		}
	}

	return nil
}

func initCKConns(conf model.CKManClickHouseConfig) (err error) {
	for _, host := range conf.Hosts {
		_, err = common.ConnectClickHouse(host, conf.Port, model.ClickHouseDefaultDB, conf.User, conf.Password)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
	}
	return
}
