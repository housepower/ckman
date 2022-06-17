package cron

import (
	"fmt"
	"strings"

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
		//For each physical cluster, its table schema can be consistent by default.
		//Therefore, any physical cluster can be selected for synchronization.
		cluster := clusters[0]
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
					log.Logger.Errorf("logic %s table %s.%s sync logic table failed: %v", cluster, database, localTable, err)
					continue
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
				query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN `%s` %s", database, localTable, cluster, k, v)
				log.Logger.Debugf("query:%s", query)
				_, err = ckService.DB.Exec(query)
				if err != nil {
					return errors.Wrap(err, "")
				}
			}

			deleteSql := fmt.Sprintf("DROP TABLE `%s`.`%s%s` ON CLUSTER `%s`",
				database, clickhouse.ClickHouseDistributedTablePrefix, localTable, cluster)
			log.Logger.Debugf(deleteSql)
			if _, err = ckService.DB.Exec(deleteSql); err != nil {
				return errors.Wrap(err, "")
			}

			create := fmt.Sprintf("CREATE TABLE `%s`.`%s%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
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

	return nil
}
