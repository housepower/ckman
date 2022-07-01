package cron

import (
	"fmt"
	"strings"

	client "github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/common"
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
			conf, err := repository.Ps.GetClusterbyName(cluster)
			if err != nil {
				continue
			}
			ckService := clickhouse.NewCkService(&conf)
			ckService.InitCkService()
			results, _ := ckService.GetTblLists()
			for database, tables := range results {
				var localTables []string
				for table := range tables {
					if strings.HasPrefix(table, clickhouse.ClickHouseDistTableOnLogicPrefix) {
						localTables = append(localTables, strings.TrimPrefix(table, clickhouse.ClickHouseDistTableOnLogicPrefix))
					}
				}
				if len(localTables) == 0 {
					continue
				}
				for _, localTable := range localTables {
					err = syncLogicbyTable(clusters, database, localTable)
					if err != nil {
						var exception *client.Exception
						if errors.As(err, &exception) {
							if exception.Code == 60 {
								//means local table is not exist, will auto sync schema
								needCreateTable[cluster] = clusters
								log.Logger.Infof("table %s.%s may not exists on one of cluster %v, need to auto create", database, localTable, clusters)
							}
						} else {
							log.Logger.Errorf("logic %s table %s.%s sync logic table failed: %v", cluster, database, localTable, err)
							continue
						}
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
		log.Logger.Debugf("query: %s", query)
		rows, err := ckService.DB.Query(query)
		if err != nil {
			return errors.Wrap(err, "")
		}
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
				_, err = ckService.DB.Exec(query)
				if err != nil {
					return errors.Wrap(err, "")
				}
			}

			if len(needAdds) == 0 {
				query := fmt.Sprintf("SELECT table, count() from system.columns WHERE database = '%s' AND table in ('%s%s', '%s%s') group by table", database, clickhouse.ClickHouseDistributedTablePrefix, localTable, clickhouse.ClickHouseDistTableOnLogicPrefix, localTable)
				log.Logger.Debugf("query:%s", query)
				rows, err := ckService.DB.Query(query)
				if err != nil {
					return errors.Wrap(err, "")
				}
				needAlterDist := false
				for rows.Next() {
					var table string
					var count int
					if err = rows.Scan(&table, &count); err != nil {
						return errors.Wrap(err, "")
					}
					if count < len(allCols) {
						needAlterDist = true
						break
					}
				}
				if !needAlterDist {
					continue
				}
			}
			deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC",
				database, clickhouse.ClickHouseDistributedTablePrefix, localTable, cluster)
			log.Logger.Debugf(deleteSql)
			if _, err = ckService.DB.Exec(deleteSql); err != nil {
				return errors.Wrap(err, "")
			}

			create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
				database, clickhouse.ClickHouseDistributedTablePrefix, localTable, cluster, database, localTable,
				cluster, database, localTable)
			log.Logger.Debugf(create)
			if _, err = ckService.DB.Exec(create); err != nil {
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
