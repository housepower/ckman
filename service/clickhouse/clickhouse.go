package clickhouse

import (
	"context"
	"fmt"
	"net"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/housepower/ckman/service/clickhouse/rebalance"
	"github.com/pkg/errors"
)

var (
	distEngineReg = regexp.MustCompile(`(Distributed\s*\(\s*'[^']*',\s*')[^']*(')`)
)

func GetCkClusterConfig(conf *model.CKManClickHouseConfig) (string, error) {
	var replicas []model.CkReplica

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return model.E_CH_CONNECT_FAILED, err
	}
	hosts := conf.Hosts
	conf.Hosts = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo(fmt.Sprintf("SELECT cluster, shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='%s' ORDER BY cluster, shard_num, replica_num", conf.Cluster))
	if err != nil {
		return model.E_DATA_SELECT_FAILED, err
	}
	if len(value) == 1 {
		return model.E_RECORD_NOT_FOUND, errors.Errorf("cluster %s is not exist, or hosts %v is not in cluster %s", conf.Cluster, hosts, conf.Cluster)
	}
	shardNum := uint32(0)
	var (
		loopback   bool
		lbhostName string
	)
	for i := 1; i < len(value); i++ {
		if shardNum != value[i][1].(uint32) {
			if len(replicas) != 0 {
				shard := model.CkShard{
					Replicas: replicas,
				}
				conf.Shards = append(conf.Shards, shard)
			}
			replicas = make([]model.CkReplica, 0)
		}
		if value[i][2].(uint32) > 1 {
			conf.IsReplica = true
		}
		replica := model.CkReplica{
			Ip:       value[i][4].(string),
			HostName: value[i][3].(string),
		}
		replicas = append(replicas, replica)
		conf.Hosts = append(conf.Hosts, value[i][4].(string))
		shardNum = value[i][1].(uint32)
		// when deployed on k8s, IP is not stable, and always return 127.0.0.1
		if replica.Ip == common.NetLoopBack {
			log.Logger.Infof("found loopback")
			loopback = true
			lbhostName = replica.HostName
		}
	}

	if len(replicas) != 0 {
		shard := model.CkShard{
			Replicas: replicas,
		}
		conf.Shards = append(conf.Shards, shard)
	}

	if loopback {
		var realHost string
		query := fmt.Sprintf("SELECT host_address FROM system.clusters WHERE cluster='%s' AND host_name = '%s'", conf.Cluster, lbhostName)

		hosts, err := common.GetShardAvaliableHosts(conf)
		if err != nil {
			return model.E_CH_CONNECT_FAILED, err
		}
		conn := common.GetConnection(hosts[0])
		rows, err := conn.Query(query)
		if err != nil {
			return model.E_DATA_SELECT_FAILED, err
		}
		for rows.Next() {
			var ip string
			err = rows.Scan(&ip)
			if err != nil {
				return model.E_DATA_SELECT_FAILED, err
			}
			if ip != "" && ip != common.NetLoopBack {
				realHost = ip
				break
			}
		}
		log.Logger.Infof("realHost: %s", realHost)

		for i := range conf.Hosts {
			if conf.Hosts[i] == common.NetLoopBack {
				conf.Hosts[i] = realHost
				break
			}
		}

		for i := range conf.Shards {
			for j := range conf.Shards[i].Replicas {
				if conf.Shards[i].Replicas[j].Ip == common.NetLoopBack {
					conf.Shards[i].Replicas[j].Ip = realHost
				}
			}
		}
	}

	if conf.LogicCluster != nil {
		query := fmt.Sprintf("SELECT count() FROM system.clusters WHERE cluster = '%s'", *conf.LogicCluster)
		value, err = service.QueryInfo(query)
		if err != nil {
			return model.E_DATA_SELECT_FAILED, err
		}
		c := value[1][0].(uint64)
		if c == 0 {
			return model.E_RECORD_NOT_FOUND, fmt.Errorf("logic cluster %s not exist", *conf.LogicCluster)
		}
	}

	value, err = service.QueryInfo("SELECT version()")
	if err != nil {
		return model.E_DATA_SELECT_FAILED, err
	}
	conf.Version = value[1][0].(string)

	return model.E_SUCCESS, nil
}

func getNodeInfo(service *CkService) (string, string) {
	query := `SELECT
	formatReadableSize(sum(total_space) - sum(free_space)) AS used,
		formatReadableSize(sum(total_space)) AS total, uptime() as uptime
	FROM system.disks WHERE lower(type) = 'local'`
	value, err := service.QueryInfo(query)
	if err != nil {
		return "NA/NA", ""
	}
	usedSpace := value[1][0].(string)
	totalSpace := value[1][1].(string)
	uptime := value[1][2].(uint32)
	return fmt.Sprintf("%s/%s", usedSpace, totalSpace), common.FormatReadableTime(uptime)
}

func GetCkClusterStatus(conf *model.CKManClickHouseConfig) []model.CkClusterNode {
	index := 0
	statusList := make([]model.CkClusterNode, len(conf.Hosts))
	statusMap := make(map[string]string, len(conf.Hosts))
	diskMap := make(map[string]string, len(conf.Hosts))
	uptimeMap := make(map[string]string, len(conf.Hosts))
	var lock sync.RWMutex
	var wg sync.WaitGroup
	for _, host := range conf.Hosts {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			tmp := &model.CKManClickHouseConfig{
				Hosts:    []string{innerHost},
				Port:     conf.Port,
				HttpPort: conf.HttpPort,
				Cluster:  conf.Cluster,
				User:     conf.User,
				Password: conf.Password,
			}
			service := NewCkService(tmp)
			if err := service.InitCkService(); err != nil {
				lock.Lock()
				statusMap[innerHost] = model.CkStatusRed
				diskMap[innerHost] = "NA/NA"
				lock.Unlock()
			} else {
				lock.Lock()
				statusMap[innerHost] = model.CkStatusGreen
				diskMap[innerHost], uptimeMap[innerHost] = getNodeInfo(service)
				lock.Unlock()
			}
		})
	}
	wg.Wait()
	for i, shard := range conf.Shards {
		for j, replica := range shard.Replicas {
			status := model.CkClusterNode{
				Ip:            replica.Ip,
				HostName:      replica.HostName,
				ShardNumber:   i + 1,
				ReplicaNumber: j + 1,
				Status:        statusMap[replica.Ip],
				Disk:          diskMap[replica.Ip],
				Uptime:        uptimeMap[replica.Ip],
			}
			statusList[index] = status
			index++
		}
	}
	return statusList
}
func GetCkTableMetrics(conf *model.CKManClickHouseConfig, database string, cols []string) (map[string]*model.CkTableMetrics, error) {
	metrics := make(map[string]*model.CkTableMetrics)

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return nil, err
	}
	// get table names
	databases, dbtables, err := common.GetMergeTreeTables("MergeTree", database, service.Conn)
	if err != nil {
		return nil, err
	}
	for db, tables := range dbtables {
		for _, table := range tables {
			// init
			tableName := fmt.Sprintf("%s.%s", db, table)
			metric := &model.CkTableMetrics{
				RWStatus: true,
			}
			metrics[tableName] = metric
		}
	}

	dbs := strings.Join(databases, "','")
	var query string
	var value [][]interface{}

	// get columns
	if common.ArraySearch("columns", cols) || len(cols) == 0 {
		if err = getCkTableMetricColumns(conf, metrics, dbs); err != nil {
			return nil, err
		}
	}

	// get bytes, parts, rows
	found := false
	if common.ArraySearch("partitions", cols) || common.ArraySearch("parts", cols) ||
		common.ArraySearch("compressed", cols) || common.ArraySearch("uncompressed", cols) ||
		common.ArraySearch("rows", cols) || len(cols) == 0 {
		found = true
	}
	if found {
		query = fmt.Sprintf("SELECT table, uniqExact(partition) AS partitions, count(*) AS parts, sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed, sum(rows) AS rows, database FROM cluster('%s', system.parts) WHERE (database in ('%s')) AND (active = '1') GROUP BY table, database;", conf.Cluster, dbs)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			table := value[i][0].(string)
			database := value[i][6].(string)
			tableName := fmt.Sprintf("%s.%s", database, table)
			if metric, ok := metrics[tableName]; ok {
				if common.ArraySearch("partitions", cols) || len(cols) == 0 {
					metric.Partitions = value[i][1].(uint64)
				}
				if common.ArraySearch("parts", cols) || len(cols) == 0 {
					metric.Parts = value[i][2].(uint64)
				}
				if common.ArraySearch("compressed", cols) || len(cols) == 0 {
					metric.Compressed = value[i][3].(uint64)
				}
				if common.ArraySearch("uncompressed", cols) || len(cols) == 0 {
					metric.UnCompressed = value[i][4].(uint64)
				}
				if common.ArraySearch("rows", cols) || len(cols) == 0 {
					metric.Rows = value[i][5].(uint64)
				}
			}
		}
	}

	// get readwrite_status
	if common.ArraySearch("is_readonly", cols) || len(cols) == 0 {
		query = fmt.Sprintf("select table, is_readonly, database from cluster('%s', system.replicas) where database in ('%s')", conf.Cluster, dbs)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			table := value[i][0].(string)
			database := value[i][2].(string)
			tableName := fmt.Sprintf("%s.%s", database, table)
			if metric, ok := metrics[tableName]; ok {
				isReadonly := value[i][1].(uint8)
				if isReadonly != 0 {
					metric.RWStatus = false
				}
			}
		}
	}

	return metrics, nil
}

func getCkTableMetricColumns(conf *model.CKManClickHouseConfig, metrics map[string]*model.CkTableMetrics, dbs string) error {
	query := fmt.Sprintf("SELECT table, count() as columns, database FROM system.columns WHERE database in ('%s') GROUP BY table, database", dbs)

	var lastErr error
	var queryOK bool
	for _, host := range conf.Hosts {
		conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
		if err != nil {
			lastErr = err
			log.Logger.Warnf("connect clickhouse %s failed when get table columns: %v", host, err)
			continue
		}

		service := &CkService{
			Config: conf,
			Conn:   conn,
		}
		value, err := service.QueryInfo(query)
		if err != nil {
			lastErr = err
			log.Logger.Warnf("query table columns from %s failed: %v", host, err)
			continue
		}

		queryOK = true
		mergeCkTableMetricColumns(metrics, value)
	}
	if !queryOK {
		return lastErr
	}
	return nil
}

func mergeCkTableMetricColumns(metrics map[string]*model.CkTableMetrics, value [][]interface{}) {
	for i := 1; i < len(value); i++ {
		if len(value[i]) < 3 {
			continue
		}
		table, ok := value[i][0].(string)
		if !ok {
			continue
		}
		columns, ok := ckMetricUint64(value[i][1])
		if !ok {
			continue
		}
		database, ok := value[i][2].(string)
		if !ok {
			continue
		}
		tableName := fmt.Sprintf("%s.%s", database, table)
		if metric, ok := metrics[tableName]; ok && columns > metric.Columns {
			metric.Columns = columns
		}
	}
}

func ckMetricUint64(v interface{}) (uint64, bool) {
	switch n := v.(type) {
	case uint64:
		return n, true
	case uint32:
		return uint64(n), true
	case uint:
		return uint64(n), true
	case int64:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	case int:
		if n < 0 {
			return 0, false
		}
		return uint64(n), true
	default:
		return 0, false
	}
}

func GetCKMerges(conf *model.CKManClickHouseConfig) ([]model.CKTableMerges, error) {
	var merges []model.CKTableMerges
	query := "SELECT database, table, elapsed, progress, num_parts, result_part_name, source_part_names, total_size_bytes_compressed, bytes_read_uncompressed, bytes_written_uncompressed, rows_read, memory_usage, merge_algorithm FROM system.merges"
	log.Logger.Debug("query: %s", query)
	for _, host := range conf.Hosts {
		db, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
		if err != nil {
			return merges, err
		}
		rows, err := db.Query(query)
		if err != nil {
			return merges, err
		}
		for rows.Next() {
			var (
				databse, table, result_part_name, merge_algorithm                                                                    string
				elapsed, progress                                                                                                    float64
				memory_usage, num_parts, total_size_bytes_compressed, bytes_written_uncompressed, bytes_read_uncompressed, rows_read uint64
				source_part_names                                                                                                    []string
			)
			err = rows.Scan(&databse, &table, &elapsed, &progress, &num_parts, &result_part_name, &source_part_names, &total_size_bytes_compressed, &bytes_read_uncompressed, &bytes_written_uncompressed, &rows_read, &memory_usage, &merge_algorithm)
			if err != nil {
				return merges, err
			}
			merge := model.CKTableMerges{
				Table:           databse + "." + table,
				Host:            host,
				Elapsed:         elapsed,
				MergeStart:      time.Now().Add(time.Duration(elapsed*float64(time.Second)) * (-1)),
				Progress:        progress,
				NumParts:        num_parts,
				ResultPartName:  result_part_name,
				SourcePartNames: strings.Join(source_part_names, ","),
				Compressed:      total_size_bytes_compressed,
				Uncomressed:     bytes_read_uncompressed + bytes_written_uncompressed,
				Rows:            rows_read,
				MemUsage:        memory_usage,
				Algorithm:       merge_algorithm,
			}
			merges = append(merges, merge)
		}
		rows.Close()
	}

	return merges, nil
}

func SetTableOrderBy(conf *model.CKManClickHouseConfig, req model.OrderbyReq) error {
	hosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return err
	}

	ck := NewCkService(conf)
	if err = ck.InitCkService(); err != nil {
		return err
	}
	local, dist, err := common.GetTableNames(ck.Conn, req.Database, req.Table, req.DistName, conf.Cluster, true)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var lastError error
	query := fmt.Sprintf(`SELECT create_table_query, engine, partition_key, sorting_key FROM system.tables WHERE (database = '%s') AND (name = '%s')`, req.Database, local)
	log.Logger.Debugf(query)
	rows, err := ck.Conn.Query(query)
	if err != nil {
		return err
	}
	var createSql, engine, partition, order string
	for rows.Next() {
		err = rows.Scan(&createSql, &engine, &partition, &order)
		if err != nil {
			return err
		}
	}
	log.Logger.Debugf("createsql: %s, engine:%s, partition: %s, order: %s", createSql, engine, partition, order)
	new_partition := ""
	if req.Partitionby.Name != "" {
		switch req.Partitionby.Policy {
		case model.CkTablePartitionPolicyDay:
			new_partition = fmt.Sprintf("toYYYYMMDD(`%s`)", req.Partitionby.Name)
		case model.CkTablePartitionPolicyMonth:
			new_partition = fmt.Sprintf("toYYYYMM(`%s`)", req.Partitionby.Name)
		case model.CkTablePartitionPolicyWeek:
			new_partition = fmt.Sprintf("toYearWeek(`%s`)", req.Partitionby.Name)
		default:
			new_partition = fmt.Sprintf("toYYYYMMDD(`%s`)", req.Partitionby.Name)
		}
	}

	new_order := ""
	if len(req.Orderby) > 0 {
		new_order = strings.Join(req.Orderby, ",")
	}
	if new_partition == partition && new_order == order {
		return fmt.Errorf("partition and orderby is the same as the old")
	}
	tmpSql := fmt.Sprintf("CREATE TABLE `%s`.`tmp_%s` AS `%s`.`%s` ENGINE=%s() PARTITION BY %s ORDER BY (%s)", req.Database, local, req.Database, local, engine, new_partition, new_order)
	createSql = strings.ReplaceAll(strings.ReplaceAll(createSql, "PARTITION BY "+partition, "PARTITION BY "+new_partition), "ORDER BY ("+order, "ORDER BY ("+new_order)
	createSql = strings.ReplaceAll(createSql, fmt.Sprintf("CREATE TABLE %s.%s", req.Database, local), fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER `%s`", req.Database, local, conf.Cluster))

	max_insert_threads := runtime.NumCPU()*3/4 + 1
	for _, host := range hosts {
		host := host
		wg.Add(1)
		common.Pool.Submit(func() {
			defer wg.Done()
			conn, err := common.ConnectClickHouse(host, req.Database, conf.GetConnOption())
			if err != nil {
				lastError = err
				return
			}

			queries := []string{
				tmpSql,
				fmt.Sprintf("INSERT INTO `%s`.`tmp_%s` SELECT * FROM `%s`.`%s` SETTINGS max_insert_threads=%d, max_execution_time=0", req.Database, local, req.Database, local, max_insert_threads),
			}

			for _, query := range queries {
				log.Logger.Debugf("[%s]%s", host, query)
				err = conn.Exec(query)
				if err != nil {
					lastError = err
					return
				}
			}
		})
	}
	wg.Wait()

	// if lastError not nil, need to drop tmp table
	if lastError == nil {
		// we must ensure data move to tmptable succeed, then drop and recreate origin table
		queries := []string{
			fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", req.Database, local, conf.Cluster),
			createSql,
		}

		for _, query := range queries {
			log.Logger.Debugf("%s", query)
			err = ck.Conn.Exec(query)
			if err != nil {
				lastError = err
				break
			}
		}
	}

	for _, host := range hosts {
		host := host
		wg.Add(1)
		common.Pool.Submit(func() {
			defer wg.Done()
			db := common.GetConnection(host)
			if db == nil {
				return
			}
			if lastError == nil {
				query := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`tmp_%s` SETTINGS max_insert_threads=%d,max_execution_time=0", req.Database, local, req.Database, local, max_insert_threads)
				log.Logger.Debugf("%s: %s", host, query)
				err = ck.Conn.Exec(query)
				if err != nil {
					lastError = err
				}
			}

			cleanSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`tmp_%s` SYNC", req.Database, local)
			log.Logger.Debugf("%s: %s", host, cleanSql)
			_ = db.Exec(cleanSql)
		})
	}
	wg.Wait()
	if lastError != nil {
		return lastError
	}

	if dist != "" {
		//alter distributed table
		ck = NewCkService(conf)
		if err = ck.InitCkService(); err != nil {
			return err
		}
		deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC",
			req.Database, dist, conf.Cluster)
		log.Logger.Debugf(deleteSql)
		if err = ck.Conn.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}

		create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
			req.Database, dist, conf.Cluster, req.Database, local,
			conf.Cluster, req.Database, local)
		log.Logger.Debugf(create)
		if err = ck.Conn.Exec(create); err != nil {
			return errors.Wrap(err, "")
		}

		if conf.LogicCluster != nil {
			distParams := model.DistLogicTblParams{
				Database:     req.Database,
				TableName:    local,
				DistName:     dist,
				ClusterName:  conf.Cluster,
				LogicCluster: *conf.LogicCluster,
			}
			if err := ck.DeleteDistTblOnLogic(&distParams); err != nil {
				return err
			}
			if err := ck.CreateDistTblOnLogic(&distParams); err != nil {
				return err
			}
		}
	}

	return nil
}

func MaterializedView(conf *model.CKManClickHouseConfig, req model.MaterializedViewReq) (string, error) {
	var statement string
	ckService := NewCkService(conf)
	err := ckService.InitCkService()
	if err != nil {
		return "", err
	}
	var query string
	if req.Operate == model.OperateCreate {
		partition := ""
		switch req.Partition.Policy {
		case model.CkTablePartitionPolicyDay:
			partition = fmt.Sprintf("toYYYYMMDD(`%s`)", req.Partition.Name)
		case model.CkTablePartitionPolicyMonth:
			partition = fmt.Sprintf("toYYYYMM(`%s`)", req.Partition.Name)
		case model.CkTablePartitionPolicyWeek:
			partition = fmt.Sprintf("toYearWeek(`%s`)", req.Partition.Name)
		default:
			partition = fmt.Sprintf("toYYYYMMDD(`%s`)", req.Partition.Name)
		}

		var populate string
		if req.Populate {
			populate = "POPULATE"
		}

		query = fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` ENGINE=%s PARTITION BY %s ORDER BY (`%s`) %s AS %s",
			req.Database, req.Name, conf.Cluster, req.Engine, partition, strings.Join(req.Order, "`,`"), populate, req.Statement)
	} else if req.Operate == model.OperateDelete {
		query = fmt.Sprintf("DROP VIEW IF EXISTS `%s`.`%s` ON CLUSTER `%s`) SYNC",
			req.Database, req.Name, conf.Cluster)
	}
	if req.Dryrun {
		return query, nil
	} else {
		log.Logger.Debug(query)
		err = ckService.Conn.Exec(query)
		if err != nil {
			return "", err
		}
	}

	return statement, nil
}

func MigrateTable(conf *model.CKManClickHouseConfig, req model.MigrateTableReq) (model.MigrateTableRsp, error) {
	var resp model.MigrateTableRsp
	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return resp, err
	}

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER `%s`", req.TargetDb, conf.Cluster)
	log.Logger.Debugf(query)
	if err := service.Conn.Exec(query); err != nil {
		return resp, err
	}

	// 获取所有MergeTree表
	tables := req.IncludeTables
	if len(tables) == 0 {
		_, allTbls, err := common.GetMergeTreeTables("MergeTree", req.SourceDb, service.Conn)
		if err != nil {
			return resp, err
		}
		tables = allTbls[req.SourceDb]
	}

	// 排除指定表
	var realTables []string
	if len(req.ExcludeTables) > 0 {
		for _, tbl := range tables {
			if !common.ArraySearch(tbl, req.ExcludeTables) {
				realTables = append(realTables, tbl)
			}
		}
	} else {
		realTables = tables
	}

	//分布式表，逻辑表
	var allTables []string
	allTables = append(allTables, realTables...)
	for _, tbl := range realTables {
		distTbls, err := getDistTbls(service.Conn, req.SourceDb, tbl)
		if err != nil {
			return resp, err
		}
		allTables = append(allTables, distTbls...)
	}

	//关联的物化视图
	mvTables, err := GetVmStatus(conf)
	if err != nil {
		return resp, err
	}
	for k, mv := range mvTables {
		db := strings.Split(k, ".")[0]
		if db != req.SourceDb {
			continue
		}
		mvTbl := strings.Split(k, ".")[1]
		sourceTable := strings.Split(mv.SourceTable, ".")[1]
		if common.ArraySearch(sourceTable, realTables) {
			allTables = append(allTables, mvTbl)
		}
	}

	type TcreateSql struct {
		Sql string
		Tbl string
	}
	var createSqls []TcreateSql
	var successList []model.MigrateDetail
	var failedList []model.MigrateDetail
	var summary model.Summary
	// 获取建表语句
	for _, tbl := range allTables {
		var createSql string
		createSql, err = genCreateSql(service.Conn, req.SourceDb, tbl, req.TargetDb, conf.Cluster)
		if err != nil {
			failedList = append(failedList, model.MigrateDetail{
				Error:     err.Error(),
				TableName: tbl,
			})
			summary.Fail++
		}
		createSqls = append(createSqls, TcreateSql{
			Sql: createSql,
			Tbl: tbl,
		})
		summary.Total++
	}

	//执行建表语句
	if req.Dryrun {
		for _, cs := range createSqls {
			successList = append(successList, model.MigrateDetail{
				TableName: cs.Tbl,
				CreateSql: cs.Sql,
			})
			summary.Success++
		}
	} else {
		for _, cs := range createSqls {
			if err = service.Conn.Exec(cs.Sql); err != nil {
				failedList = append(failedList, model.MigrateDetail{
					Error:     err.Error(),
					TableName: cs.Tbl,
					CreateSql: cs.Sql,
				})
				summary.Fail++
			}
			successList = append(successList, model.MigrateDetail{
				TableName: cs.Tbl,
				//CreateSql: cs.Sql,
			})
		}

		//迁移数据
		if !req.SchemaOnly {
			for _, tbl := range realTables {
				query := fmt.Sprintf("InSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s` SETTINGS max_execution_time=0,max_insert_thread=32", req.TargetDb, tbl, req.SourceDb, tbl)
				log.Logger.Debugf("[%s] %s", tbl, query)
				hosts, err := common.GetShardAvaliableHosts(conf)
				if err != nil {
					return resp, err
				}
				wg := sync.WaitGroup{}
				wg.Add(len(hosts))
				for _, host := range hosts {
					go func(host string) {
						defer wg.Done()
						c := common.GetConnection(host)
						if c == nil {
							return
						}
						if err = c.Exec(query); err != nil {
							failedList = append(failedList, model.MigrateDetail{
								Error:     err.Error(),
								TableName: tbl,
							})
							summary.Fail++
						}
					}(host)
				}
				wg.Wait()
			}
		}
	}

	resp.Summary = summary
	resp.SuccessList = successList
	resp.FailedList = failedList

	return resp, nil
}

func genCreateSql(conn *common.Conn, database, table, target, cluster string) (string, error) {
	query := fmt.Sprintf(`SELECT replaceRegexpAll(replaceRegexpOne(create_table_query, 'CREATE TABLE( IF NOT EXISTS)?\\s+\\w+\\.\\w+', 'CREATE TABLE IF NOT EXISTS %s.%s ON CLUSTER %s'), '/clickhouse/tables/\\{cluster\\}/%s/', '/clickhouse/tables/{cluster}/%s/') AS create_sql
FROM system.tables
WHERE (database = '%s') AND (table = '%s')`,
		target, table, cluster, database, target, database, table)
	log.Logger.Debugf("[genCreateSql]query:%s", query)
	var createSql string
	if err := conn.QueryRow(query).Scan(&createSql); err != nil {
		return "", err
	}
	if strings.Contains(createSql, "Distributed") {
		createSql = strings.ReplaceAll(createSql,
			fmt.Sprintf("Distributed('%s', '%s'", cluster, database),
			fmt.Sprintf("Distributed('%s', '%s'", cluster, target))
	}
	if strings.Contains(createSql, "MATERIALIZED VIEW") {
		createSql = strings.ReplaceAll(createSql, fmt.Sprintf("%s.", database), fmt.Sprintf("%s.", target))
		if !strings.Contains(createSql, "MATERIALIZED VIEW IF NOT EXISTS") {
			createSql = strings.ReplaceAll(createSql, "MATERIALIZED VIEW", "MATERIALIZED VIEW IF NOT EXISTS")
		}
		createSql = strings.ReplaceAll(createSql, fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s ", target, table), fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s ON CLUSTER %s ", target, table, cluster))
	}
	createSql = distEngineReg.ReplaceAllString(createSql, fmt.Sprintf("${1}%s${2}", target))
	return createSql, nil
}

func getDistTbls(conn *common.Conn, database, table string) (distTbls []string, err error) {
	query := fmt.Sprintf(`SELECT name, (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster
	 FROM system.tables WHERE engine='Distributed' AND database='%s' AND match(engine_full, 'Distributed\(\'.*\', \'%s\', \'%s\'.*\)')`,
		database, database, table)
	log.Logger.Infof("executing sql=> %s", query)
	var rows *common.Rows
	if rows, err = conn.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name, cluster string
		if err = rows.Scan(&name, &cluster); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		distTbls = append(distTbls, name)
	}
	return
}

func GetVmStatus(conf *model.CKManClickHouseConfig) (map[string]*model.CkVmStatus, error) {
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return nil, err
	}
	uncompressed := "sum(0)"
	if common.CompareClickHouseVersion(conf.Version, "24.8.x") >= 0 {
		uncompressed = "sum(total_bytes_uncompressed)"
	}
	query := fmt.Sprintf(`SELECT
    database,
    name,
    sum(parts),
    sum(total_rows),
    sum(total_bytes),
    %s,
    (extractAllGroups(as_select, 'FROM ([\\w\\d_]+\\.[\\w+\\d_]+)')[1])[1] AS source_table,
    as_select
FROM cluster('%s', system.tables)
WHERE (engine = 'MaterializedView')
GROUP BY
    database,
    name,
    source_table,
    as_select`, uncompressed, conf.Cluster)

	data, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	vmStatus := make(map[string]*model.CkVmStatus)
	for i := 1; i < len(data); i++ {
		database := data[i][0].(string)
		name := data[i][1].(string)
		vmStatus[database+"."+name] = &model.CkVmStatus{
			Parts:        data[i][2].(uint64),
			Rows:         data[i][3].(uint64),
			Compressed:   data[i][4].(uint64),
			Uncompressed: data[i][5].(uint64),
			SourceTable:  data[i][6].(string),
			AsSelect:     data[i][7].(string),
		}
	}
	return vmStatus, nil
}

func GetBackgroundPool(conf *model.CKManClickHouseConfig) ([]model.BackgroundPoolRsp, error) {
	var resp []model.BackgroundPoolRsp
	for _, host := range conf.Hosts {
		tmp := conf
		tmp.Hosts = []string{host}
		service := NewCkService(tmp)
		if err := service.InitCkService(); err != nil {
			return nil, err
		}
		var merges, fetches, schedule, move, comm model.BackgroundPool
		merges.Task, merges.Size, merges.Usage = getPoolMetric(service, "MergesAndMutations")
		fetches.Task, fetches.Size, fetches.Usage = getPoolMetric(service, "Fetches")
		schedule.Task, schedule.Size, schedule.Usage = getPoolMetric(service, "Schedule")
		move.Task, move.Size, move.Usage = getPoolMetric(service, "Move")
		comm.Task, comm.Size, comm.Usage = getPoolMetric(service, "Common")
		resp = append(resp, model.BackgroundPoolRsp{
			Host: host,
			Pool: map[string]model.BackgroundPool{
				"MergesAndMutations": merges,
				"Fetches":            fetches,
				"Schedule":           schedule,
				"Move":               move,
				"Common":             comm,
			},
		})

	}
	return resp, nil
}

func getPoolMetric(service *CkService, name string) (int64, int64, float64) {
	var task, size int64
	var usage float64
	query := fmt.Sprintf("SELECT metric, value FROM system.metrics WHERE metric in ('Background%sPoolTask', 'Background%sPoolSize')", name, name)

	data, err := service.QueryInfo(query)
	if err != nil {
		log.Logger.Errorf("get pool metric error: %s", err.Error())
		return 0, 0, 0
	}
	// data[0] 是表头，data[1:] 才是数据行；IN 查询结果顺序不固定，按 metric 名分别取值，避免顺序错乱或缺行越界
	for i := 1; i < len(data); i++ {
		metric, _ := data[i][0].(string)
		value, _ := data[i][1].(int64)
		switch {
		case strings.HasSuffix(metric, "PoolTask"):
			task = value
		case strings.HasSuffix(metric, "PoolSize"):
			size = value
		}
	}
	if size == 0 {
		usage = 0
	} else {
		usage = float64(task) / float64(size)
	}
	return task, size, usage
}

func DropPartition(conf *model.CKManClickHouseConfig, database, table, partitionId string) error {
	chNodes, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION ID '%s'", database, table, partitionId)
	for _, chNode := range chNodes {
		conn := common.GetConnection(chNode)
		if conn == nil {
			log.Logger.Errorf("connect to %s failed", chNode)
			continue
		}
		err = conn.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetPartitions(conf *model.CKManClickHouseConfig, database, tableName string, limit int) (map[string]model.PartitionInfo, error) {
	partInfo := make(map[string]model.PartitionInfo)

	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return nil, err
	}
	var limitN string
	if limit > 0 {
		limitN = fmt.Sprintf("LIMIT %d", limit)
	}

	query := fmt.Sprintf(`SELECT 
    partition,
	count(name),
    sum(rows),
    sum(data_compressed_bytes),
    sum(data_uncompressed_bytes),
    min(min_time),
    max(max_time),
    disk_name,
	partition_id
FROM cluster('%s', system.parts)
WHERE (database = '%s') AND (table = '%s') AND (active = 1)
GROUP BY
    partition,
    disk_name,
	partition_id
ORDER BY partition DESC %s`, conf.Cluster, database, tableName, limitN)
	log.Logger.Infof("query: %s", query)
	value, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(value); i++ {
		partitionId := value[i][0].(string)
		part := model.PartitionInfo{
			Database:     database,
			Table:        tableName,
			Parts:        value[i][1].(uint64),
			Rows:         value[i][2].(uint64),
			Compressed:   value[i][3].(uint64),
			UnCompressed: value[i][4].(uint64),
			MinTime:      value[i][5].(time.Time),
			MaxTime:      value[i][6].(time.Time),
			DiskName:     value[i][7].(string),
			PartitionId:  value[i][8].(string),
			Status:       true,
		}
		partInfo[partitionId] = part
	}

	query = fmt.Sprintf("SELECT distinct partition_id, disk from cluster('%s', system.detached_parts) where database = '%s' and table = '%s'",
		conf.Cluster, database, tableName)
	log.Logger.Infof("query: %s", query)
	value, err = service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(value); i++ {
		partitionId := value[i][0].(string)
		part := model.PartitionInfo{
			Database:    database,
			Table:       tableName,
			DiskName:    value[i][1].(string),
			PartitionId: value[i][0].(string),
			Status:      false,
		}
		partInfo[partitionId] = part
	}

	return partInfo, nil
}

func getHostSessions(service *CkService, query, host string) ([]*model.CkSessionInfo, error) {
	list := make([]*model.CkSessionInfo, 0)

	value, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(value); i++ {
		session := new(model.CkSessionInfo)
		session.StartTime = value[i][0].(time.Time).Unix()
		session.QueryDuration = value[i][1].(uint64)
		session.Query = value[i][2].(string)
		session.User = value[i][3].(string)
		session.QueryId = value[i][4].(string)
		session.Address = value[i][5].(net.IP).String()
		session.Host = host
		list = append(list, session)
	}

	return list, nil
}

func getCkSessions(conf *model.CKManClickHouseConfig, limit int, query string) ([]*model.CkSessionInfo, error) {
	list := make([]*model.CkSessionInfo, 0)

	var lastError error
	var wg sync.WaitGroup
	for _, host := range conf.Hosts {
		innerHost := host
		wg.Add(1)
		_ = common.Pool.Submit(func() {
			defer wg.Done()
			service, err := GetCkNodeService(conf.Cluster, innerHost)
			if err != nil {
				log.Logger.Warnf("get ck node %s service error: %v", innerHost, err)
				return
			}

			sessions, err := getHostSessions(service, query, innerHost)
			if err != nil {
				lastError = err
			}
			list = append(list, sessions...)
		})
	}
	wg.Wait()
	if lastError != nil {
		return nil, lastError
	}

	sort.Sort(model.SessionList(list))
	if len(list) <= limit {
		return list, nil
	} else {
		return list[:limit], nil
	}
}

func GetCkOpenSessions(conf *model.CKManClickHouseConfig, limit int) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("select subtractSeconds(now(), elapsed) AS query_start_time, toUInt64(elapsed*1000) AS query_duration_ms, query, initial_user, initial_query_id, initial_address, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)\\?(\\w+)')[1])[3] AS tbl_name from system.processes WHERE tbl_name != '' AND tbl_name != 'processes' AND tbl_name != 'query_log' AND is_initial_query=1 ORDER BY query_duration_ms DESC limit %d", limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, limit, query)
}

func GetDistibutedDDLQueue(conf *model.CKManClickHouseConfig) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("select DISTINCT query_create_time, query, host, initiator_host, entry from cluster('%s', system.distributed_ddl_queue) where cluster = '%s' and status != 'Finished' ORDER BY query_create_time", conf.Cluster, conf.Cluster)
	log.Logger.Debugf("query:%s", query)
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return nil, err
	}

	value, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	var sessions []*model.CkSessionInfo
	if len(value) > 1 {
		sessions = make([]*model.CkSessionInfo, len(value)-1)
		for i := 1; i < len(value); i++ {
			var session model.CkSessionInfo
			startTime := value[i][0].(time.Time)
			session.StartTime = startTime.Unix()
			session.QueryDuration = uint64(time.Since(startTime).Milliseconds())
			session.Query = value[i][1].(string)
			session.Host = value[i][2].(string)
			session.Address = value[i][3].(string)
			session.QueryId = value[i][4].(string)

			sessions[i-1] = &session
		}
	} else {
		sessions = make([]*model.CkSessionInfo, 0)
	}
	return sessions, nil
}
func KillCkOpenSessions(conf *model.CKManClickHouseConfig, host, queryId, typ string) error {
	conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
	if err != nil {
		return err
	}
	if typ == "queue" {
		query := fmt.Sprintf(`SELECT
		splitByChar('.', table)[1] AS database,
		splitByChar('.', table)[2] AS tbl,
		initial_query_id
	FROM
	(
		SELECT
			(extractAllGroups(value, 'TABLE (\\w+\\.\\w+) ')[1])[1] AS table,
			(extractAllGroups(value, 'initial_query_id: (.*)\n')[1])[1] AS initial_query_id
		FROM system.zookeeper
		WHERE (path = '/clickhouse/task_queue/ddl/%s') AND (name = '%s')
	)`, conf.Cluster, queryId)
		var database, table, initial_query_id string
		log.Logger.Debugf(query)
		err := conn.QueryRow(query).Scan(&database, &table, &initial_query_id)
		if err != nil {
			return errors.Wrap(err, "")
		}
		log.Logger.Debugf("database: %s, table: %s, initial_query_id: %s", database, table, initial_query_id)
		query = fmt.Sprintf("select query_id from system.processes where initial_query_id = '%s'", initial_query_id)
		var query_id string
		log.Logger.Debugf(query)
		err = conn.QueryRow(query).Scan(&query_id)
		if err == nil {
			query = fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", query_id)
			log.Logger.Debugf(query)
			err = conn.Exec(query)
			if err != nil {
				return errors.Wrap(err, "")
			}
		} else {
			// kill mutation
			query = fmt.Sprintf("select count() from system.mutations where is_done = 0 and database = '%s' and table = '%s'", database, table)
			log.Logger.Debugf(query)
			var count uint64
			err = conn.QueryRow(query).Scan(&count)
			if err != nil {
				return errors.Wrap(err, "")
			}
			if count > 0 {
				query = fmt.Sprintf("KILL MUTATION WHERE database = '%s' AND table = '%s'", database, table)
				log.Logger.Debugf(query)
				err = conn.Exec(query)
				if err != nil {
					return errors.Wrap(err, "")
				}
			}
		}
	} else {
		query := fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", queryId)
		err = conn.Exec(query)
		if err != nil {
			return errors.Wrap(err, "")
		}
	}
	return nil
}

func GetCkSlowSessions(conf *model.CKManClickHouseConfig, cond model.SessionCond) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("SELECT query_start_time, query_duration_ms, query, initial_user, initial_query_id, initial_address, splitByChar('.', tables[1])[-1] AS tbl_name from system.query_log WHERE tbl_name != '' AND tbl_name != 'query_log' AND tbl_name != 'processes' AND type=2 AND is_initial_query=1 AND event_date  >= parseDateTimeBestEffort('%d') AND query_start_time >= parseDateTimeBestEffort('%d') AND query_start_time <= parseDateTimeBestEffort('%d') ORDER BY query_duration_ms DESC limit %d", cond.StartTime, cond.StartTime, cond.EndTime, cond.Limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, cond.Limit, query)
}
func GetZkPath(conn *common.Conn, database, table string) (string, error) {
	var err error
	var path string
	var rows *common.Rows
	query := fmt.Sprintf(`SELECT
		(extractAllGroups(create_table_query, '(MergeTree\\(\')(.*)\', \'{replica}\'\\)')[1])[2] AS zoopath
FROM system.tables
WHERE database = '%s' AND name = '%s'`, database, table)
	log.Logger.Debugf("database:%s, table:%s: query: %s", database, table, query)
	if rows, err = conn.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var result string
		if err = rows.Scan(&result); err != nil {
			err = errors.Wrapf(err, "")
			return "", err
		}
		path = result
	}

	return path, nil
}

func checkTableIfExists(database, name, cluster string) bool {
	conf, err := repository.Ps.GetClusterbyName(cluster)
	if err != nil {
		return false
	}
	hosts, err := common.GetShardAvaliableHosts(&conf)
	if err != nil {
		return false
	}
	for _, host := range hosts {
		tmp := conf
		tmp.Hosts = []string{host}
		service := NewCkService(&tmp)
		if err := service.InitCkService(); err != nil {
			log.Logger.Warnf("shard: %v init service failed: %v", tmp.Hosts, err)
			return false
		}
		if err := common.CheckTable(service.Conn, database, name); err != nil {
			log.Logger.Warnf("shard: %v, table %s does not exist", tmp.Hosts, name)
			return false
		}
	}
	return true
}

func DropTableIfExists(params model.CreateCkTableParams, ck *CkService) error {
	dropSql := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s SYNC", params.DB, params.Name, params.Cluster)
	log.Logger.Debugf(dropSql)
	err := ck.Conn.Exec(dropSql)
	if err != nil {
		return err
	}

	dropSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s SYNC", params.DB, params.DistName, params.Cluster)
	log.Logger.Debugf(dropSql)
	err = ck.Conn.Exec(dropSql)
	return err
}
func GetCKVersion(conf *model.CKManClickHouseConfig, host string) (string, error) {
	tmp := *conf
	tmp.Hosts = []string{host}
	service, err := GetCkService(conf.Cluster)
	if err != nil {
		return "", err
	}
	value, err := service.QueryInfo("SELECT version()")
	if err != nil {
		return "", err
	}
	version := value[1][0].(string)
	return version, nil
}

func SyncLogicTable(src, dst model.CKManClickHouseConfig, name ...string) bool {
	hosts, err := common.GetShardAvaliableHosts(&src)
	if err != nil || len(hosts) == 0 {
		log.Logger.Warnf("cluster %s all node is unvaliable", src.Cluster)
		return false
	}
	srcConn, err := common.ConnectClickHouse(hosts[0], model.ClickHouseDefaultDB, src.GetConnOption())
	if err != nil {
		log.Logger.Warnf("connect %s failed", hosts[0])
		return false
	}
	tableName := ""
	database := ""
	if len(name) > 0 {
		database = name[0]
		tableName = name[1]
	}
	statementsqls, err := GetLogicSchema(srcConn, *dst.LogicCluster, dst.Cluster, dst.IsReplica, database, tableName)
	if err != nil {
		log.Logger.Warnf("get logic schema failed: %v", err)
		return false
	}

	dstHost := common.PickAvailableSchemaSource(&dst)
	if dstHost == "" {
		log.Logger.Warnf("no available host in cluster %s for schema sync", dst.Cluster)
		return false
	}
	dstConn, err := common.ConnectClickHouse(dstHost, model.ClickHouseDefaultDB, dst.GetConnOption())
	if err != nil {
		log.Logger.Warnf("can't connect %s", dstHost)
		return false
	}
	for _, schema := range statementsqls {
		for _, statement := range schema.Statements {
			log.Logger.Debugf("%s", statement)
			if err := dstConn.Exec(statement); err != nil {
				log.Logger.Warnf("excute sql failed: %v", err)
				return false
			}
		}
	}
	return true
}

func RestoreReplicaTable(conf *model.CKManClickHouseConfig, host, database, table string) error {
	conn, err := common.ConnectClickHouse(host, database, conf.GetConnOption())
	if err != nil {
		return errors.Wrapf(err, "cann't connect to %s", host)
	}
	query := "SELECT is_readonly FROM system.replicas"
	var is_readonly uint8
	if err = conn.QueryRow(query).Scan(&is_readonly); err != nil {
		return errors.Wrap(err, host)
	}
	if is_readonly == 0 {
		return nil
	}

	query = "SYSTEM RESTART REPLICA " + table
	if err := conn.Exec(query); err != nil {
		return errors.Wrap(err, host)
	}
	query = "SYSTEM RESTORE REPLICA " + table
	if err := conn.Exec(query); err != nil {
		// Code: 36. DB::Exception: Replica must be readonly. (BAD_ARGUMENTS)
		if common.ExceptionAS(err, common.BAD_ARGUMENTS) {
			return nil
		}
		return errors.Wrap(err, host)
	}
	return nil
}

// GetRebalanceInfo returns the per-table row/byte distribution (grouped by
// table, with per-shard breakdown) for the requested table patterns. Thin
// wrapper kept for the legacy import path; implementation lives in
// service/clickhouse/rebalance.
func GetRebalanceInfo(conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables) ([]model.TableRebalanceInfo, error) {
	return rebalance.Info(conf, rtables)
}

// RebalanceCluster runs a rebalance for the requested tables. Thin wrapper
// kept for the legacy import path; implementation lives in
// service/clickhouse/rebalance. onStep is the per-phase progress callback
// (nil when progress reporting is not needed). ctx is checked at phase
// boundaries so the runner's Cancel can interrupt a long-running rebalance.
func RebalanceCluster(ctx context.Context, conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables, exceptMaxShard bool, onStep func(model.Internationalization)) error {
	return rebalance.Run(ctx, conf, rtables, exceptMaxShard, onStep)
}

// RebalancePlan computes the rebalance preview: what RebalanceCluster would
// do given the cluster's current state, without moving any data. Thin
// wrapper kept for the legacy import path.
func RebalancePlan(conf *model.CKManClickHouseConfig, rtables []model.RebalanceTables, exceptMaxShard bool) (*model.RebalancePlan, error) {
	return rebalance.Plan(conf, rtables, exceptMaxShard)
}

func GroupUniqArray(conf *model.CKManClickHouseConfig, req model.GroupUniqArrayReq) error {
	//创建本地聚合表，本地物化视图，分布式聚合表，分布式视图
	if err := CreateViewOnCluster(conf, req); err != nil {
		return err
	}
	// 创建逻辑聚合表和逻辑视图
	if conf.LogicCluster != nil {
		//当前集群的逻辑表和逻辑视图创建
		if err := CreateLogicViewOnCluster(conf, req); err != nil {
			return err
		}
		clusters, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			con, err := repository.Ps.GetClusterbyName(cluster)
			if err != nil {
				return err
			}
			//当前集群已经创建过了，跳过
			if con.Cluster == conf.Cluster {
				continue
			} else {
				//其他物理集群需要同步创建本地表、本地视图，分布式表、分布式视图，以及逻辑表，逻辑视图
				if err := CreateViewOnCluster(&con, req); err != nil {
					return err
				}
				if err := CreateLogicViewOnCluster(&con, req); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func CreateViewOnCluster(conf *model.CKManClickHouseConfig, req model.GroupUniqArrayReq) error {
	//前置工作
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return err
	}
	query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE (database = '%s') AND (table = '%s')", req.Database, req.Table)
	rows, err := service.Conn.Query(query)
	if err != nil {
		return err
	}
	fields := make(map[string]string, len(req.Fields)+1)
	for rows.Next() {
		var name, typ string
		err = rows.Scan(&name, &typ)
		if err != nil {
			rows.Close()
			return err
		}
		if name == req.TimeField {
			fields[name] = typ
		} else {
			for _, f := range req.Fields {
				if name == f.Name {
					fields[name] = typ
				}
			}
		}
	}
	rows.Close()

	// check 一把是否所有字段在表里都能找到
	for _, f := range req.Fields {
		if _, ok := fields[f.Name]; !ok {
			return fmt.Errorf("can't find field %s in %s.%s", f.Name, req.Database, req.Table)
		}
	}

	aggTable := fmt.Sprintf("%s%s", common.ClickHouseAggregateTablePrefix, req.Table)
	distAggTable := fmt.Sprintf("%s%s", common.ClickHouseAggDistTablePrefix, req.Table)

	mvLocal := fmt.Sprintf("%s%s", common.ClickHouseLocalViewPrefix, req.Table)
	mvDist := fmt.Sprintf("%s%s", common.ClickHouseDistributedViewPrefix, req.Table)

	var engine string
	if conf.IsReplica {
		engine = "ReplicatedReplacingMergeTree()"
	} else {
		engine = "ReplacingMergeTree"
	}

	fieldAndType := fmt.Sprintf("`%s` %s,", req.TimeField, fields[req.TimeField])
	fieldSql := fmt.Sprintf("`%s`, ", req.TimeField)
	where := " WHERE 1=1 "
	for i, f := range req.Fields {
		if i > 0 {
			fieldAndType += ","
		}
		if f.MaxSize == 0 {
			f.MaxSize = 10000
		}
		typ := fields[f.Name]
		nullable := false
		defaultValue := f.DefaultValue
		if strings.HasPrefix(typ, "Nullable(") {
			nullable = true
			typ = strings.TrimSuffix(strings.TrimPrefix(typ, "Nullable("), ")")
			if strings.Contains(typ, "Int") || strings.Contains(typ, "Float") {
				defaultValue = fmt.Sprintf("%v", f.DefaultValue)
			} else {
				defaultValue = fmt.Sprintf("'%v'", f.DefaultValue)
			}
			where += fmt.Sprintf(" AND isNotNull(`%s`) ", f.Name)
		}
		fieldAndType += fmt.Sprintf("`%s%s` AggregateFunction(groupUniqArray(%d), `%s`)", model.GroupUniqArrayPrefix, f.Name, f.MaxSize, typ)
		if nullable {
			fieldSql += fmt.Sprintf("groupUniqArrayState(%d)(ifNull(`%s`, %s)) AS `%s%s`", f.MaxSize, f.Name, defaultValue, model.GroupUniqArrayPrefix, f.Name)
		} else {
			fieldSql += fmt.Sprintf("groupUniqArrayState(%d)(`%s`) AS `%s%s`", f.MaxSize, f.Name, model.GroupUniqArrayPrefix, f.Name)
		}
	}

	view_sql := fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s GROUP BY (`%s`)", fieldSql, req.Database, req.Table, where, req.TimeField)

	// 创建本地聚合表及本地视图
	agg_query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` (%s) ENGINE = %s ORDER BY (`%s`)",
		req.Database, aggTable, conf.Cluster, fieldAndType, engine, req.TimeField)

	//需不需要partition by？
	log.Logger.Debugf("agg_query: %s", agg_query)
	err = service.Conn.Exec(agg_query)
	if err != nil {
		return err
	}
	view_query := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` TO `%s`.`%s` AS %s",
		req.Database, mvLocal, conf.Cluster, req.Database, aggTable, view_sql)

	log.Logger.Debugf("view_query: %s", view_query)
	err = service.Conn.Exec(view_query)
	if err != nil {
		return err
	}

	// 创建分布式聚合表及分布式视图
	agg_query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		req.Database, distAggTable, conf.Cluster, req.Database, aggTable, conf.Cluster, req.Database, aggTable)
	log.Logger.Debugf("agg_query: %s", agg_query)
	err = service.Conn.Exec(agg_query)
	if err != nil {
		return err
	}

	view_query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		req.Database, mvDist, conf.Cluster, req.Database, mvLocal, conf.Cluster, req.Database, mvLocal)
	log.Logger.Debugf("view_query: %s", view_query)
	err = service.Conn.Exec(view_query)
	if err != nil {
		return err
	}

	if req.Populate {
		insert_query := fmt.Sprintf("INSERT INTO `%s`.`%s` %s ", req.Database, aggTable, view_sql)
		log.Logger.Debugf("[%s]insert_query: %s", conf.Cluster, insert_query)
		hosts, err := common.GetShardAvaliableHosts(conf)
		if err != nil {
			return err
		}
		for _, host := range hosts {
			conn := common.GetConnection(host)
			if conn != nil {
				err = service.Conn.AsyncInsert(insert_query, false)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func CreateLogicViewOnCluster(conf *model.CKManClickHouseConfig, req model.GroupUniqArrayReq) error {
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return err
	}
	aggTable := fmt.Sprintf("%s%s", common.ClickHouseAggregateTablePrefix, req.Table)
	logicAggTable := fmt.Sprintf("%s%s", common.ClickHouseAggLogicTablePrefix, req.Table)

	mvLocal := fmt.Sprintf("%s%s", common.ClickHouseAggregateTablePrefix, req.Table)
	mvLogic := fmt.Sprintf("%s%s", common.ClickHouseLogicViewPrefix, req.Table)

	agg_query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		req.Database, logicAggTable, conf.Cluster, req.Database, aggTable, *conf.LogicCluster, req.Database, aggTable)
	log.Logger.Debugf("agg_query: %s", agg_query)
	err = service.Conn.Exec(agg_query)
	if err != nil {
		return err
	}

	view_query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		req.Database, mvLogic, conf.Cluster, req.Database, mvLocal, *conf.LogicCluster, req.Database, mvLocal)
	log.Logger.Debugf("view_query: %s", view_query)
	err = service.Conn.Exec(view_query)
	if err != nil {
		return err
	}
	return nil
}

func GetGroupUniqArray(conf *model.CKManClickHouseConfig, database, table string) (map[string]interface{}, error) {
	//确定是查分布式表还是逻辑表
	viewName := common.TernaryExpression(conf.LogicCluster != nil, fmt.Sprintf("%s%s", common.ClickHouseLogicViewPrefix, table), fmt.Sprintf("%s%s", common.ClickHouseDistributedViewPrefix, table)).(string)
	//根据表名查询出物化视图名，聚合函数类型
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT
    name,
    (extractAllGroups(type, 'groupUniqArray\\((\\d+)\\)')[1])[1] AS maxsize
FROM system.columns
WHERE (database = '%s') AND (table = '%s') AND (type LIKE 'AggregateFunction%%')`,
		database, viewName)
	log.Logger.Debugf(query)
	rows, err := service.Conn.Query(query)
	if err != nil {
		return nil, err
	}
	var aggFields string
	idx := 0
	for rows.Next() {
		var name, maxSize string
		err = rows.Scan(&name, &maxSize)
		if err != nil {
			rows.Close()
			return nil, err
		}
		if name != "" {
			if idx > 0 {
				aggFields += ", "
			}
			aggFields += fmt.Sprintf("groupUniqArrayMerge(%s)(%s) AS %s", maxSize, name, strings.TrimPrefix(name, model.GroupUniqArrayPrefix))
			idx++
		}
	}
	rows.Close()

	//查询
	query = fmt.Sprintf("SELECT %s FROM `%s`.`%s`", aggFields, database, viewName)
	data, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	result := make(map[string]interface{})
	keys := data[0]
	for i, key := range keys {
		value := data[1][i]
		result[key.(string)] = value
	}
	return result, nil
}

func DelGroupUniqArray(conf *model.CKManClickHouseConfig, database, table string) error {
	err := delGuaViewOnCluster(conf, database, table)
	if err != nil {
		return err
	}

	//如果有逻辑集群，还要去各个逻辑集群删除本地物化视图、分布式物化视图，逻辑物化视图
	if conf.LogicCluster != nil {
		clusters, err := repository.Ps.GetLogicClusterbyName(*conf.LogicCluster)
		if err != nil {
			return err
		}
		for _, cluster := range clusters {
			if cluster == conf.Cluster {
				err = delGuaViewOnLogic(conf, database, table)
				if err != nil {
					return err
				}
			} else {
				clus, err := repository.Ps.GetClusterbyName(cluster)
				if err != nil {
					return err
				}
				if err = delGuaViewOnCluster(&clus, database, table); err != nil {
					return err
				}
				err = delGuaViewOnLogic(&clus, database, table)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func delGuaViewOnCluster(conf *model.CKManClickHouseConfig, database, table string) error {
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return err
	}

	queries := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseLocalViewPrefix, table, conf.Cluster),
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseDistributedViewPrefix, table, conf.Cluster),
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseAggregateTablePrefix, table, conf.Cluster),
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseAggDistTablePrefix, table, conf.Cluster),
	}

	for _, query := range queries {
		log.Logger.Debugf("[%s]%s", conf.Cluster, query)
		err = service.Conn.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}

func delGuaViewOnLogic(conf *model.CKManClickHouseConfig, database, table string) error {
	service := NewCkService(conf)
	err := service.InitCkService()
	if err != nil {
		return err
	}

	queries := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseLogicViewPrefix, table, conf.Cluster),
		fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC", database, common.ClickHouseAggLogicTablePrefix, table, conf.Cluster),
	}

	for _, query := range queries {
		log.Logger.Debugf("[%s]%s", conf.Cluster, query)
		err = service.Conn.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func GetReplicatedQueue(conf *model.CKManClickHouseConfig, database, table, node string) ([]model.ReplicatedQueueRsp, error) {
	if !conf.IsReplica {
		return nil, nil
	}
	conf.Hosts = []string{node}
	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT
    node_name,
    type,
    create_time,
    num_tries,
    postpone_reason,
	last_exception
FROM system.replication_queue
WHERE (database = '%s') AND (table = '%s')  ORDER BY create_time DESC`, database, table)
	data, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	var queueInfo []model.ReplicatedQueueRsp
	for i := 1; i < len(data); i++ {
		node_name := data[i][0].(string)
		type_ := data[i][1].(string)
		create_time := data[i][2].(time.Time)
		num_tries := data[i][3].(uint32)
		postpone_reason := data[i][4].(string)
		last_exception := data[i][5].(string)
		queueInfo = append(queueInfo, model.ReplicatedQueueRsp{
			NodeName:       node_name,
			Type:           type_,
			CreateTime:     create_time,
			NumTries:       num_tries,
			PostponeReason: postpone_reason,
			LastException:  last_exception,
		})
	}
	return queueInfo, nil
}

func GetReplicatedTableStatus(conf *model.CKManClickHouseConfig) ([]model.ReplicatedTableRsp, error) {
	if !conf.IsReplica {
		return nil, nil
	}
	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`SELECT
    concat(database, '.', table) AS table,
    replica_name,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
	last_queue_update_exception,
    log_pointer,
    IF(log_pointer = 0, 100, (log_pointer - queue_size) * 100 / log_pointer) AS progress
FROM clusterAllReplicas('%s', system.replicas)
ORDER BY
    queue_size DESC`, conf.Cluster)
	data, err := service.QueryInfo(query)
	if err != nil {
		return nil, err
	}
	var tblReplicaStatus []model.ReplicatedTableRsp
	for i := 1; i < len(data); i++ {
		table := data[i][0].(string)
		replica_name := data[i][1].(string)
		queue_size := data[i][2].(uint32)
		inserts := data[i][3].(uint32)
		merges := data[i][4].(uint32)
		last_exception := data[i][5].(string)
		log_pointer := data[i][6].(uint64)
		progress := data[i][7].(float64)

		shard_replica := ""
		var found bool
		for i, shard := range conf.Shards {
			for j, replica := range shard.Replicas {
				if replica_name == replica.Ip {
					shard_replica = fmt.Sprintf("%d-%d", i+1, j+1)
					found = true
					break
				}
			}
			if found {
				break
			}
		}

		tblReplicaStatus = append(tblReplicaStatus, model.ReplicatedTableRsp{
			Table:         table,
			Node:          replica_name,
			ShardReplica:  shard_replica,
			QueueSize:     queue_size,
			Inserts:       inserts,
			Merges:        merges,
			LastException: last_exception,
			LogPointer:    log_pointer,
			Progress:      progress,
		})
	}
	return tblReplicaStatus, nil
}

func OperatePartition(conf *model.CKManClickHouseConfig, req model.OperatePartitionReq) error {
	var queries []string
	switch req.Op {
	case model.OP_PARTITION_DROP:
		if req.Status {
			query := fmt.Sprintf("ALTER TABLE `%s`.`%s`  DROP PARTITION ID '%s'", req.Database, req.Table, req.PartitionId)
			queries = append(queries, query)
		} else {
			query := fmt.Sprintf("ALTER TABLE `%s`.`%s`  DROP DETACHED PARTITION ID '%s'", req.Database, req.Table, req.PartitionId)
			queries = append(queries, query)
		}
	case model.OP_PARTITION_ATTACH:
		if req.Status {
			return fmt.Errorf("partition is already attached")
		}
		if err := fixBrokenParts(conf, req.Database, req.Table, req.PartitionId); err != nil {
			return err
		}
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION ID '%s'", req.Database, req.Table, req.PartitionId)
		queries = append(queries, query)
	case model.OP_PARTITION_DETACH:
		if !req.Status {
			return fmt.Errorf("partition is already detached")
		}
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s`  DETACH PARTITION ID '%s'", req.Database, req.Table, req.PartitionId)
		queries = append(queries, query)
	default:
		return fmt.Errorf("unsupported partition operation: %v", req.Op)
	}

	for _, host := range conf.Hosts {
		for _, query := range queries {
			log.Logger.Debugf("[%s]%s", host, query)
			conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
			if err != nil {
				return err
			}
			err = conn.Exec(query)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type DetachedInfo struct {
	PartitionId string
	Name        string
	Disk        string
	Reason      string
}

func getDetachedParts(conf *model.CKManClickHouseConfig, database, table, partitionId string) map[string][]DetachedInfo {
	result := make(map[string][]DetachedInfo)
	for _, host := range conf.Hosts {
		conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, conf.GetConnOption())
		if err != nil {
			log.Logger.Errorf("connect clickhouse failed: %s", err)
			return result
		}
		query := fmt.Sprintf(`SELECT partition_id, name, disk, reason FROM system.detached_parts WHERE database = '%s' AND table = '%s' AND partition_id = '%s'`, database, table, partitionId)
		rows, err := conn.Query(query)
		if err != nil {
			log.Logger.Errorf("query failed: %s", err)
			return result
		}
		defer rows.Close()
		var parts []DetachedInfo
		for rows.Next() {
			var partitionId, name, diskName, reason string
			if err := rows.Scan(&partitionId, &name, &diskName, &reason); err != nil {
				log.Logger.Errorf("scan failed: %s", err)
				return result
			}
			parts = append(parts, DetachedInfo{
				PartitionId: partitionId,
				Name:        name,
				Disk:        diskName,
				Reason:      reason,
			})
		}
		result[host] = parts
	}

	return result
}

func fixBrokenParts(conf *model.CKManClickHouseConfig, database, table, partitionId string) error {
	detachedParts := getDetachedParts(conf, database, table, partitionId)

	query := fmt.Sprintf("SELECT data_paths FROM system.tables WHERE database = '%s' AND name = '%s'", database, table)
	for host, parts := range detachedParts {
		var cmds []string
		var dataPaths []string
		conn := common.GetConnection(host)
		if conn == nil {
			log.Logger.Errorf("get connection failed: %s", host)
			continue
		}
		err := conn.QueryRow(query).Scan(&dataPaths)
		if err != nil {
			log.Logger.Errorf("query failed: %s", err)
			continue
		}
		type pathInfo struct {
			srcPaths []string
			dstPaths []string
		}
		var pi pathInfo
		for _, part := range parts {
			for _, dp := range dataPaths {
				if part.Reason != "" {
					dstName := strings.TrimPrefix(part.Name, part.Reason+"_")
					if part.Disk == "default" {
						if !strings.Contains(dp, "/disks/") {
							pi.srcPaths = append(pi.srcPaths, path.Join(dp, "detached", part.Name))
							pi.dstPaths = append(pi.dstPaths, path.Join(dp, "detached", dstName))
							break
						}
					} else {
						if strings.Contains(dp, fmt.Sprintf("/disks/%s/", part.Disk)) {
							pi.srcPaths = append(pi.srcPaths, path.Join(dp, "detached", part.Name))
							pi.dstPaths = append(pi.dstPaths, path.Join(dp, "detached", dstName))
							break
						}
					}
				}
			}
		}
		for i := 0; i < len(pi.srcPaths); i++ {
			cmd := fmt.Sprintf("mv %s %s; chown -R clickhouse:clickhouse %s", pi.srcPaths[i], pi.dstPaths[i], pi.dstPaths[i])
			cmds = append(cmds, cmd)
		}
		if len(cmds) == 0 {
			_, err = common.RemoteExecute(common.SshOptions{
				Host:             host,
				User:             conf.SshUser,
				Port:             conf.SshPort,
				Password:         conf.SshPassword,
				NeedSudo:         conf.NeedSudo,
				AuthenticateType: conf.AuthenticateType,
			}, strings.Join(cmds, ";"))
			if err != nil {
				log.Logger.Errorf("execute failed: %s", err)
				return err
			}
		}
	}
	return nil
}
