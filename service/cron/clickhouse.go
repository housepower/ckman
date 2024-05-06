package cron

import (
	"fmt"
	"strings"

	client "github.com/ClickHouse/clickhouse-go/v2"
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
			rows, err := ckService.Conn.Query(query)
			if err != nil {
				continue
			}
			defer rows.Close()
			for rows.Next() {
				var database, table, logic, local string
				_ = rows.Scan(&database, &table, &logic, &local)
				err = syncLogicbyTable(clusters, database, local)
				if err != nil {
					err = common.ClikHouseExceptionDecode(err)
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
		rows, err := ckService.Conn.Query(query)
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
			var columns []string
			for k, v := range needAdds {
				columns = append(columns, fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s ", k, v))
			}

			// 当前集群是全量的列
			if len(columns) == 0 {
				continue
			}
			conf, err := repository.Ps.GetClusterbyName(cluster)
			if err != nil {
				return err
			}
			ckService := clickhouse.NewCkService(&conf)
			if err = ckService.InitCkService(); err != nil {
				return err
			}

			columnsExpr := strings.Join(columns, ",")
			// local table
			onCluster := fmt.Sprintf("ON CLUSTER `%s`", cluster)
			if err = alterTable(ckService.Conn, database, localTable, onCluster, columnsExpr); err != nil {
				return err
			}

			// distributed table
			if err = alterTable(ckService.Conn, database, common.ClickHouseDistributedTablePrefix+localTable, onCluster, columnsExpr); err != nil {
				return err
			}

			// logic table
			if err = alterTable(ckService.Conn, database, common.ClickHouseLocalTablePrefix+localTable, onCluster, columnsExpr); err != nil {
				return err
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
					if _, err := common.ConnectClickHouse(replica.Ip, model.ClickHouseDefaultDB, cluster.GetConnOption()); err == nil {
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
		rows, err := ckService.Conn.Query(query)
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
			err := syncDistTable(table, local, database, conf)
			if err != nil {
				log.Logger.Warnf("[%s]sync distributed table schema failed: %v", conf.Cluster, err)
				continue
			}
		}
	}
	return nil
}

func syncDistTable(distTable, localTable, database string, conf model.CKManClickHouseConfig) error {
	tableLists := make(map[string]common.Map)
	dbLists := make(map[string]*common.Conn)
	for _, host := range conf.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			continue
		}
		query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s'", database, localTable)
		rows, err := conn.Query(query)
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

	if needAlter {
		log.Logger.Debugf("need alter table, table %s.%s have different columns on cluster %s", database, localTable, conf.Cluster)
		for host, cols := range tableLists {
			needAdds := allCols.Difference(cols).(common.Map)
			var columns []string
			for k, v := range needAdds {
				columns = append(columns, fmt.Sprintf("ADD COLUMN IF NOT EXISTS `%s` %s ", k, v))
			}

			// 当前节点是全量的列, 无需更新
			if len(columns) == 0 {
				continue
			}
			// local table
			if err := alterTable(dbLists[host], database, localTable, "", strings.Join(columns, ",")); err != nil {
				return err
			}

			// distributed table
			if err := alterTable(dbLists[host], database, distTable, "", strings.Join(columns, ",")); err != nil {
				return err
			}
		}
	}
	return nil
}

func initCKConns(conf model.CKManClickHouseConfig) (err error) {
	for _, host := range conf.Hosts {
		_, err = common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

func alterTable(conn *common.Conn, database, table, onCluster, col string) error {
	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` %s %s",
		database, table, onCluster, col)
	if onCluster != "" {
		query += "SETTINGS alter_sync = 0"
	}
	log.Logger.Debug(query)
	return conn.Exec(query)
}
