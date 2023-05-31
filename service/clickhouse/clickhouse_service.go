package clickhouse

import (
	"database/sql"
	"fmt"
	"math"
	"net"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/housepower/ckman/repository"
	"github.com/pkg/errors"
)

const (
	ClickHouseQueryStart        string = "QueryStart"
	ClickHouseQueryFinish       string = "QueryFinish"
	ClickHouseQueryExStart      string = "ExceptionBeforeStart"
	ClickHouseQueryExProcessing string = "ExceptionWhileProcessing"
	ClickHouseConfigVersionKey  string = "ck_cluster_config_version"
	ClickHouseClustersFile      string = "clusters.json"
	ClickHouseServiceTimeout    int    = 3600
)

type CkService struct {
	Config *model.CKManClickHouseConfig
	DB     *sql.DB
}

func NewCkService(config *model.CKManClickHouseConfig) *CkService {
	ck := &CkService{}
	config.Normalize()
	ck.Config = config
	ck.DB = nil

	return ck
}

func (ck *CkService) InitCkService() error {
	if len(ck.Config.Hosts) == 0 {
		return errors.Errorf("can't find any host")
	}

	hasConnect := false
	var lastError error
	hosts := common.Shuffle(ck.Config.Hosts)
	for _, host := range hosts {
		connect, err := common.ConnectClickHouse(host, ck.Config.Port, model.ClickHouseDefaultDB, ck.Config.User, ck.Config.Password)
		if err == nil {
			ck.DB = connect
			hasConnect = true
			break
		} else {
			lastError = err
		}
	}
	if !hasConnect {
		return lastError
	}
	return nil
}

func GetCkService(clusterName string) (*CkService, error) {
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err == nil {
		service := NewCkService(&conf)
		if err := service.InitCkService(); err != nil {
			return nil, err
		}
		return service, nil
	} else {
		return nil, errors.Errorf("can't find cluster %s service", clusterName)
	}
}

func GetCkNodeService(clusterName string, node string) (*CkService, error) {
	conf, err := repository.Ps.GetClusterbyName(clusterName)
	if err == nil {
		tmp := &model.CKManClickHouseConfig{
			Hosts:    []string{node},
			Port:     conf.Port,
			Cluster:  conf.Cluster,
			User:     conf.User,
			Password: conf.Password,
		}
		service := NewCkService(tmp)
		if err := service.InitCkService(); err != nil {
			return nil, err
		}
		return service, nil
	} else {
		return nil, errors.Errorf("can't find cluster %s %s service", clusterName, node)
	}
}

func GetCkClusterConfig(conf *model.CKManClickHouseConfig) error {
	var replicas []model.CkReplica

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return err
	}
	hosts := conf.Hosts
	conf.Hosts = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo(fmt.Sprintf("SELECT cluster, shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='%s' ORDER BY cluster, shard_num, replica_num", conf.Cluster))
	if err != nil {
		return err
	}
	if len(value) == 1 {
		return errors.Errorf("cluster %s is not exist, or hosts %v is not in cluster %s", conf.Cluster, hosts, conf.Cluster)
	}
	shardNum := uint32(0)
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
	}
	if len(replicas) != 0 {
		shard := model.CkShard{
			Replicas: replicas,
		}
		conf.Shards = append(conf.Shards, shard)
	}

	value, err = service.QueryInfo("SELECT version()")
	if err != nil {
		return err
	}
	conf.Version = value[1][0].(string)

	return nil
}

func getNodeInfo(service *CkService) (string, string) {
	query := `SELECT
	formatReadableSize(sum(total_space) - sum(free_space)) AS used,
		formatReadableSize(sum(total_space)) AS total, uptime() as uptime
	FROM system.disks WHERE type = 'local'`
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

func (ck *CkService) CreateTable(params *model.CreateCkTableParams, dryrun bool) ([]string, error) {
	var statements []string
	if ck.DB == nil {
		return statements, errors.Errorf("clickhouse service unavailable")
	}

	ensureDatabaseSql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER `%s`", params.DB, params.Cluster)
	statements = append(statements, ensureDatabaseSql)
	if !dryrun {
		_, _ = ck.DB.Exec(ensureDatabaseSql)
	}
	columns := make([]string, 0)
	for _, value := range params.Fields {
		columns = append(columns, fmt.Sprintf("`%s` %s %s", value.Name, value.Type, strings.Join(value.Options, " ")))
	}

	partition := ""
	switch params.Partition.Policy {
	case model.CkTablePartitionPolicyDay:
		partition = fmt.Sprintf("toYYYYMMDD(`%s`)", params.Partition.Name)
	case model.CkTablePartitionPolicyMonth:
		partition = fmt.Sprintf("toYYYYMM(`%s`)", params.Partition.Name)
	case model.CkTablePartitionPolicyWeek:
		partition = fmt.Sprintf("toYearWeek(`%s`)", params.Partition.Name)
	default:
		partition = fmt.Sprintf("toYYYYMMDD(`%s`)", params.Partition.Name)
	}

	for i := range params.Order {
		params.Order[i] = fmt.Sprintf("`%s`", params.Order[i])
	}

	var projections string
	for _, p := range params.Projections {
		projections += fmt.Sprintf(", PROJECTION %s (%s)", p.Name, p.Sql)
	}

	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` (%s%s) ENGINE = %s() PARTITION BY %s ORDER BY (%s)",
		params.DB, params.Name, params.Cluster, strings.Join(columns, ", "), projections, params.Engine,
		partition, strings.Join(params.Order, ", "))
	if params.TTLExpr != "" {
		create += fmt.Sprintf(" TTL %s", params.TTLExpr)
	}
	if params.StoragePolicy != "" {
		create += fmt.Sprintf(" SETTINGS storage_policy = '%s'", params.StoragePolicy)
	}
	log.Logger.Debugf(create)
	statements = append(statements, create)
	if !dryrun {
		if _, err := ck.DB.Exec(create); err != nil {
			if ok := checkTableIfExists(params.DB, params.Name, params.Cluster); !ok {
				return statements, err
			}
		}
	}
	create = fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		params.DB, params.DistName, params.Cluster, params.DB, params.Name,
		params.Cluster, params.DB, params.Name)
	log.Logger.Debugf(create)
	statements = append(statements, create)
	if !dryrun {
		if _, err := ck.DB.Exec(create); err != nil {
			if ok := checkTableIfExists(params.DB, params.DistName, params.Cluster); !ok {
				return statements, err
			}
		}
	}
	return statements, nil
}

func (ck *CkService) CreateDistTblOnLogic(params *model.DistLogicTblParams) error {
	local, _, err := common.GetTableNames(ck.DB, params.Database, params.TableName, params.DistName, params.ClusterName, true)
	if err != nil {
		return err
	}
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		params.Database, common.ClickHouseDistTableOnLogicPrefix, local, params.ClusterName,
		params.Database, local, params.LogicCluster, params.Database, local)

	log.Logger.Debug(createSql)
	if _, err := ck.DB.Exec(createSql); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (ck *CkService) DeleteDistTblOnLogic(params *model.DistLogicTblParams) error {
	local, _, err := common.GetTableNames(ck.DB, params.Database, params.TableName, params.DistName, params.ClusterName, true)
	if err != nil {
		return err
	}
	deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC",
		params.Database, common.ClickHouseDistTableOnLogicPrefix, local, params.ClusterName)
	log.Logger.Debug(deleteSql)
	if _, err := ck.DB.Exec(deleteSql); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (ck *CkService) DeleteTable(conf *model.CKManClickHouseConfig, params *model.DeleteCkTableParams) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	local, dist, err := common.GetTableNames(ck.DB, params.DB, params.Name, params.DistName, params.Cluster, true)
	if err != nil {
		return err
	}
	if dist != "" {
		deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", params.DB,
			dist, params.Cluster)
		log.Logger.Debugf(deleteSql)
		if _, err := ck.DB.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}
	}

	if local != "" {
		deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", params.DB, local, params.Cluster)
		log.Logger.Debugf(deleteSql)
		if _, err := ck.DB.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// delete zoopath
	tableName := fmt.Sprintf("%s.%s", params.DB, local)
	delete(conf.ZooPath, tableName)

	return nil
}

func (ck *CkService) AlterTable(params *model.AlterCkTableParams) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	local, dist, err := common.GetTableNames(ck.DB, params.DB, params.Name, params.DistName, params.Cluster, true)
	if err != nil {
		return err
	}
	// add column
	for _, value := range params.Add {
		add := ""
		if value.After != "" {
			add = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS `%s` %s %s AFTER `%s`",
				params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "), value.After)
		} else {
			add = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS `%s` %s %s",
				params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "))
		}
		log.Logger.Debugf(add)
		if _, err := ck.DB.Exec(add); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// modify column
	for _, value := range params.Modify {
		query := fmt.Sprintf("SELECT CAST(`%s`, '%s') FROM `%s`.`%s`", value.Name, value.Type, params.DB, local)
		log.Logger.Debug(query)
		if rows, err := ck.DB.Query(query); err != nil {
			return errors.Wrapf(err, "can't modify %s to %s", value.Name, value.Type)
		} else {
			rows.Close()
		}

		modify := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MODIFY COLUMN IF EXISTS `%s` %s %s",
			params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "))
		log.Logger.Debugf(modify)
		if _, err := ck.DB.Exec(modify); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// delete column
	for _, value := range params.Drop {
		drop := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` DROP COLUMN IF EXISTS `%s`",
			params.DB, local, params.Cluster, value)
		log.Logger.Debugf(drop)
		if _, err := ck.DB.Exec(drop); err != nil {
			return errors.Wrap(err, "")
		}
	}

	//rename column
	for _, value := range params.Rename {
		if value.From == "" || value.To == "" {
			return errors.Errorf("form %s or to %s must not be empty", value.From, value.To)
		}
		rename := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` RENAME COLUMN IF EXISTS `%s` TO `%s`",
			params.DB, local, params.Cluster, value.From, value.To)

		log.Logger.Debugf(rename)
		if _, err := ck.DB.Exec(rename); err != nil {
			return errors.Wrap(err, "")
		}
	}

	for _, p := range params.Projections {
		var query string
		if p.Action == model.ProjectionAdd {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD PROJECTION %s (%s)",
				params.DB, params.Name, params.Cluster, p.Name, p.Sql)
		} else if p.Action == model.ProjectionDrop {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` DROP PROJECTION %s", params.DB, params.Name, params.Cluster, p.Name)
		} else if p.Action == model.ProjectionClear {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` CLEAR PROJECTION %s", params.DB, params.Name, params.Cluster, p.Name)
		}
		if query != "" {
			if _, err := ck.DB.Exec(query); err != nil {
				return errors.Wrap(err, "")
			}
			if p.Action == model.ProjectionAdd {
				// trigger history data
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MATERIALIZE PROJECTION %s", params.DB, params.Name, params.Cluster, p.Name)
				if _, err := ck.DB.Exec(query); err != nil {
					return errors.Wrap(err, "")
				}
			}
		}
	}

	// 删除分布式表并重建
	deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC",
		params.DB, dist, params.Cluster)
	log.Logger.Debugf(deleteSql)
	if _, err := ck.DB.Exec(deleteSql); err != nil {
		return errors.Wrap(err, "")
	}

	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		params.DB, dist, params.Cluster, params.DB, local,
		params.Cluster, params.DB, local)
	log.Logger.Debugf(create)
	if _, err := ck.DB.Exec(create); err != nil {
		return errors.Wrap(err, "")
	}

	// 删除逻辑表并重建（如果有的话）
	conf, _ := repository.Ps.GetClusterbyName(params.Cluster)

	if conf.LogicCluster != nil {
		distParams := model.DistLogicTblParams{
			Database:     params.DB,
			TableName:    local,
			DistName:     dist,
			ClusterName:  params.Cluster,
			LogicCluster: *conf.LogicCluster,
		}
		if err := ck.DeleteDistTblOnLogic(&distParams); err != nil {
			return err
		}
		if err := ck.CreateDistTblOnLogic(&distParams); err != nil {
			return err
		}
	}

	return nil
}

func (ck *CkService) AlterTableTTL(req *model.AlterTblsTTLReq) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	for _, table := range req.Tables {
		local, _, err := common.GetTableNames(ck.DB, table.Database, table.TableName, table.DistName, ck.Config.Cluster, true)
		if err != nil {
			return err
		}
		if req.TTLType != "" {
			if req.TTLType == model.TTLTypeModify {
				if req.TTLExpr != "" {
					ttl := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MODIFY TTL %s", table.Database, local, ck.Config.Cluster, req.TTLExpr)
					log.Logger.Debugf(ttl)
					if _, err := ck.DB.Exec(ttl); err != nil {
						return errors.Wrap(err, "")
					}
				}
			} else if req.TTLType == model.TTLTypeRemove {
				ttl := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` REMOVE TTL", table.Database, local, ck.Config.Cluster)
				log.Logger.Debugf(ttl)
				if _, err := ck.DB.Exec(ttl); err != nil {
					return errors.Wrap(err, "")
				}
			}
		}
	}

	return nil
}

func (ck *CkService) DescTable(params *model.DescCkTableParams) ([]model.CkColumnAttribute, error) {
	attrs := make([]model.CkColumnAttribute, 0)
	if ck.DB == nil {
		return attrs, errors.Errorf("clickhouse service unavailable")
	}

	desc := fmt.Sprintf("DESCRIBE TABLE `%s`.`%s`", params.DB, params.Name)
	log.Logger.Debugf(desc)
	rows, err := ck.DB.Query(desc)
	if err != nil {
		return attrs, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name, types, default_type, default_expression, comments, codec_expression, ttl_expression string
		)
		if err = rows.Scan(&name, &types, &default_type, &default_expression, &comments, &codec_expression, &ttl_expression); err != nil {
			return []model.CkColumnAttribute{}, errors.Wrap(err, "")
		}
		attr := model.CkColumnAttribute{
			Name:              name,
			Type:              types,
			DefaultType:       default_type,
			DefaultExpression: default_expression,
			Comment:           comments,
			CodecExpression:   codec_expression,
			TTLExpression:     ttl_expression,
		}
		attrs = append(attrs, attr)
	}

	return attrs, nil
}

func (ck *CkService) QueryInfo(query string) ([][]interface{}, error) {
	if ck.DB == nil {
		return nil, errors.Errorf("clickhouse service unavailable")
	}

	log.Logger.Debugf(query)
	rows, err := ck.DB.Query(query)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	colData := make([][]interface{}, 0)
	colNames := make([]interface{}, len(cols))
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, colName := range cols {
		columnPointers[i] = &columns[i]
		colNames[i] = colName
	}
	colData = append(colData, colNames)

	ctps, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	colTpyes := make([]int64, len(cols))
	for i, ctp := range ctps {
		_, s, ok := ctp.DecimalSize()
		if ok {
			colTpyes[i] = s
		} else {
			colTpyes[i] = 0
		}
	}

	for rows.Next() {
		// Scan the result into the column pointers...
		if err = rows.Scan(columnPointers...); err != nil {
			return nil, errors.Wrap(err, "")
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make([]interface{}, len(cols))
		for i := range columnPointers {
			val := columnPointers[i].(*interface{})
			m[i] = *val
			if m[i] == nil {
				m[i] = "NULL"
			} else {
				if colTpyes[i] > 0 {
					var decimal float64
					if reflect.TypeOf(m[i]).Kind() == reflect.Int32 {
						v := m[i].(int32)
						decimal = float64(v) / math.Pow10(int(colTpyes[i]))
					} else if reflect.TypeOf(m[i]).Kind() == reflect.Int64 {
						v := m[i].(int64)
						decimal = float64(v) / math.Pow10(int(colTpyes[i]))
					}
					m[i] = decimal
				} else {
					if reflect.TypeOf(m[i]).Kind() == reflect.Float64 {
						v := m[i].(float64)
						if math.IsNaN(v) {
							m[i] = "NaN"
						} else if math.IsInf(v, 0) {
							m[i] = "Inf"
						}
					}
				}
			}
		}

		colData = append(colData, m)
		if len(colData) >= 10001 {
			break
		}
	}

	return colData, nil
}

func (ck *CkService) FetchSchemerFromOtherNode(host, password string) error {
	names, statements, err := GetCreateReplicaObjects(ck.DB, host, model.ClickHouseDefaultUser, password)
	if err != nil {
		return err
	}

	num := len(names)
	for i := 0; i < num; i++ {
		log.Logger.Debugf("statement: %s", statements[i])
		var e error
		if _, e = ck.DB.Exec(statements[i]); e != nil {
			return errors.Wrap(e, "")
		}
	}

	return nil
}

func GetCkTableMetrics(conf *model.CKManClickHouseConfig, database string, cols []string) (map[string]*model.CkTableMetrics, error) {
	metrics := make(map[string]*model.CkTableMetrics)

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return nil, err
	}
	// get table names
	databases, dbtables, err := common.GetMergeTreeTables("MergeTree", database, service.DB)
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
		query = fmt.Sprintf("SELECT table, count() as columns, database FROM system.columns WHERE database in ('%s') GROUP BY table, database",
			dbs)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			table := value[i][0].(string)
			database := value[i][2].(string)
			tableName := fmt.Sprintf("%s.%s", database, table)
			if metric, ok := metrics[tableName]; ok {
				metric.Columns = value[i][1].(uint64)
			}
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
		query = fmt.Sprintf("SELECT table, uniqExact(partition) AS partitions, count(*) AS parts, sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed, sum(rows) AS rows, database FROM cluster('{cluster}', system.parts) WHERE (database in ('%s')) AND (active = '1') GROUP BY table, database;", dbs)
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
		query = fmt.Sprintf("select table, is_readonly, database from cluster('{cluster}', system.replicas) where database in ('%s')", dbs)
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

	// get success, failed counts
	tables := "["
	for k := range dbtables {
		tables += fmt.Sprintf("'%s',", k)
	}
	tables = strings.TrimRight(tables, ",")
	tables += "]"
	if common.ArraySearch("queries", cols) || len(cols) == 0 {
		query = fmt.Sprintf("SELECT tables[1], type, count() AS counts from cluster('{cluster}', system.query_log) where hasAny(databases, %s) = 1 AND is_initial_query=1 AND event_date >= subtractDays(now(), 1) group by tables, type", tables)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if metric, ok := metrics[tableName]; ok {
				types := value[i][1].(string)
				if types == ClickHouseQueryFinish {
					metric.CompletedQueries = value[i][2].(uint64)
				} else if types == ClickHouseQueryExStart || types == ClickHouseQueryExProcessing {
					metric.FailedQueries = value[i][2].(uint64)
				}
			}
		}
	}

	// get query duration
	if common.ArraySearch("cost", cols) || len(cols) == 0 {
		query = fmt.Sprintf("SELECT tables[1] AS tbl_name, quantiles(0.5, 0.99, 1.0)(query_duration_ms) AS duration from cluster('{cluster}', system.query_log) where hasAny(databases, %s) = 1  AND type = 2 AND is_initial_query=1 AND event_date >= subtractDays(now(), 7) group by tables", tables)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if _, ok := metrics[tableName]; ok {
				durations := value[i][1].([]float64)
				metrics[tableName].QueryCost.Middle = common.Decimal(durations[0])
				metrics[tableName].QueryCost.SecondaryMax = common.Decimal(durations[1])
				metrics[tableName].QueryCost.Max = common.Decimal(durations[2])
			}
		}
	}

	return metrics, nil
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
	local, dist, err := common.GetTableNames(ck.DB, req.Database, req.Table, req.DistName, conf.Cluster, true)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var lastError error
	query := fmt.Sprintf(`SELECT create_table_query, engine, partition_key, sorting_key FROM system.tables WHERE (database = '%s') AND (name = '%s')`, req.Database, local)
	log.Logger.Debugf(query)
	rows, err := ck.DB.Query(query)
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
			db, err := common.ConnectClickHouse(host, conf.Port, req.Database, conf.User, conf.Password)
			if err != nil {
				lastError = err
				return
			}

			queries := []string{
				tmpSql,
				fmt.Sprintf("INSERT INTO `%s`.`tmp_%s` SELECT * FROM `%s`.`%s` SETTINGS max_insert_threads=%d", req.Database, local, req.Database, local, max_insert_threads),
			}

			for _, query := range queries {
				log.Logger.Debugf("[%s]%s", host, query)
				_, err = db.Exec(query)
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
			_, err = ck.DB.Exec(query)
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
				query := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`tmp_%s` SETTINGS max_insert_threads=%d", req.Database, local, req.Database, local, max_insert_threads)
				log.Logger.Debugf("%s: %s", host, query)
				_, err := db.Exec(query)
				if err != nil {
					lastError = err
				}
			}

			cleanSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`tmp_%s` SYNC", req.Database, local)
			log.Logger.Debugf("%s: %s", host, cleanSql)
			_, _ = db.Exec(cleanSql)
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
		if _, err := ck.DB.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}

		create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
			req.Database, dist, conf.Cluster, req.Database, local,
			conf.Cluster, req.Database, local)
		log.Logger.Debugf(create)
		if _, err := ck.DB.Exec(create); err != nil {
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
		_, err = ckService.DB.Exec(query)
		if err != nil {
			return "", err
		}
	}

	return statement, nil
}

func GetPartitions(conf *model.CKManClickHouseConfig, table string) (map[string]model.PartitionInfo, error) {
	partInfo := make(map[string]model.PartitionInfo)

	chHosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return nil, err
	}

	dbTbl := strings.SplitN(table, ".", 2)
	dabatase := dbTbl[0]
	tableName := dbTbl[1]

	for _, host := range chHosts {
		service, err := GetCkNodeService(conf.Cluster, host)
		if err != nil {
			return nil, err
		}

		query := fmt.Sprintf(`SELECT
    partition,
    sum(rows),
    sum(data_compressed_bytes),
    sum(data_uncompressed_bytes),
    min(min_time),
    max(max_time),
    disk_name
FROM system.parts
WHERE (database = '%s') AND (table = '%s')
GROUP BY
    partition,
    disk_name
ORDER BY partition ASC`, dabatase, tableName)
		log.Logger.Infof("host: %s, query: %s", host, query)
		value, err := service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			partitionId := value[i][0].(string)
			if part, ok := partInfo[partitionId]; ok {
				part.Rows += value[i][1].(uint64)
				part.Compressed += value[i][2].(uint64)
				part.UnCompressed += value[i][3].(uint64)
				minTime := value[i][4].(time.Time)
				part.MinTime = common.TernaryExpression(part.MinTime.After(minTime), minTime, part.MinTime).(time.Time)
				maxTime := value[i][5].(time.Time)
				part.MaxTime = common.TernaryExpression(part.MaxTime.Before(maxTime), maxTime, part.MinTime).(time.Time)
				part.DiskName = value[i][6].(string)
				partInfo[partitionId] = part
			} else {
				part := model.PartitionInfo{
					Database:     dabatase,
					Table:        tableName,
					Rows:         value[i][1].(uint64),
					Compressed:   value[i][2].(uint64),
					UnCompressed: value[i][3].(uint64),
					MinTime:      value[i][4].(time.Time),
					MaxTime:      value[i][5].(time.Time),
					DiskName:     value[i][6].(string),
				}
				partInfo[partitionId] = part
			}
		}
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
		session.Threads = len(value[i][6].([]uint64))
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
	query := fmt.Sprintf("select subtractSeconds(now(), elapsed) AS query_start_time, toUInt64(elapsed*1000) AS query_duration_ms,  query, initial_user, initial_query_id, initial_address, thread_ids, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(\\w+)')[1])[3] AS tbl_name from system.processes WHERE tbl_name != '' AND tbl_name != 'processes' AND tbl_name != 'query_log' AND is_initial_query=1 ORDER BY query_duration_ms DESC limit %d", limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, limit, query)
}

func KillCkOpenSessions(conf *model.CKManClickHouseConfig, host, queryId string) error {
	db, err := common.ConnectClickHouse(host, conf.Port, clickhouse.DefaultDatabase, conf.User, conf.Password)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("KILL QUERY WHERE query_id = '%s'", queryId)
	_, err = db.Exec(query)
	if err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func GetCkSlowSessions(conf *model.CKManClickHouseConfig, cond model.SessionCond) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("SELECT query_start_time, query_duration_ms, query, initial_user, initial_query_id, initial_address, thread_ids, splitByChar('.', tables[1])[-1] AS tbl_name from system.query_log WHERE tbl_name != '' AND tbl_name != 'query_log' AND tbl_name != 'processes' AND type=2 AND is_initial_query=1 AND event_date  >= parseDateTimeBestEffort('%d') AND query_start_time >= parseDateTimeBestEffort('%d') AND query_start_time <= parseDateTimeBestEffort('%d') ORDER BY query_duration_ms DESC limit %d", cond.StartTime, cond.StartTime, cond.EndTime, cond.Limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, cond.Limit, query)
}

func GetReplicaZkPath(conf *model.CKManClickHouseConfig) error {
	var err error
	service := NewCkService(conf)
	if err = service.InitCkService(); err != nil {
		log.Logger.Errorf("all hosts not available, can't get zoopath")
		return err
	}

	query := `SELECT database, name, (extractAllGroups(create_table_query, '(MergeTree\\(\')(.*)\', \'{replica}\'\\)')[1])[2] AS zoopath FROM system.tables where match(engine, 'Replicated\w*MergeTree') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))`
	log.Logger.Debug(query)
	rows, err := service.DB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	conf.ZooPath = make(map[string]string)
	for rows.Next() {
		var database, table, zoopath string
		err = rows.Scan(&database, &table, &zoopath)
		if err != nil {
			return err
		}
		tableName := fmt.Sprintf("%s.%s", database, table)
		conf.ZooPath[tableName] = zoopath
	}
	return nil
}

func GetZkPath(db *sql.DB, database, table string) (string, error) {
	var err error
	var path string
	var rows *sql.Rows
	query := fmt.Sprintf(`SELECT
		(extractAllGroups(create_table_query, '(MergeTree\\(\')(.*)\', \'{replica}\'\\)')[1])[2] AS zoopath
FROM system.tables
WHERE database = '%s' AND name = '%s'`, database, table)
	log.Logger.Debugf("database:%s, table:%s: query: %s", database, table, query)
	if rows, err = db.Query(query); err != nil {
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

func ConvertZooPath(conf *model.CKManClickHouseConfig) []string {
	var zooPaths []string

	for _, path := range conf.ZooPath {
		if path != "" {
			for index := range conf.Shards {
				// TODO[L] macros maybe not named {cluster} or {shard}
				shardNum := fmt.Sprintf("%d", index+1)
				zooPath := strings.Replace(path, "{cluster}", conf.Cluster, -1)
				zooPath = strings.Replace(zooPath, "{shard}", shardNum, -1)
				zooPaths = append(zooPaths, zooPath)
			}
		}
	}
	return zooPaths
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
		query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'", database, name)
		data, err := service.QueryInfo(query)
		if err != nil {
			log.Logger.Warnf("shard: %v , query: %v ,err: %v", tmp.Hosts, query, err)
			return false
		}
		log.Logger.Debugf("count: %d", data[1][0].(uint64))
		if data[1][0].(uint64) != 1 {
			log.Logger.Warnf("shard: %v, table %s does not exist", tmp.Hosts, name)
			return false
		}
	}
	return true
}

func DropTableIfExists(params model.CreateCkTableParams, ck *CkService) error {
	dropSql := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s SYNC", params.DB, params.Name, params.Cluster)
	log.Logger.Debugf(dropSql)
	_, err := ck.DB.Exec(dropSql)
	if err != nil {
		return err
	}

	dropSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s SYNC", params.DB, params.DistName, params.Cluster)
	log.Logger.Debugf(dropSql)
	_, err = ck.DB.Exec(dropSql)
	return err
}

func (ck *CkService) ShowCreateTable(tbname, database string) (string, error) {
	query := fmt.Sprintf("SELECT create_table_query FROM system.tables WHERE database = '%s' AND name = '%s'", database, tbname)
	value, err := ck.QueryInfo(query)
	if err != nil {
		return "", err
	}
	schema := value[1][0].(string)
	return schema, nil
}

type RebalanceTables struct {
	Database  string
	DistTable string
	Table     string
	Columns   []string
}

func (ck *CkService) GetRebalanceTables() ([]RebalanceTables, error) {
	query := fmt.Sprintf(`SELECT
    t2.database AS database,
	t2.name AS dist,
    t2.local AS table,
    groupArray(t1.name) AS rows
FROM system.columns AS t1
INNER JOIN
(
	SELECT
    database,
	name,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[2] AS cluster,
    (extractAllGroups(engine_full, '(Distributed\\(\')(.*)\',\\s+\'(.*)\',\\s+\'(.*)\'(.*)')[1])[4] AS local
FROM system.tables
WHERE match(engine, 'Distributed') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
AND cluster = '%s')
) AS t2 ON t1.table = t2.name and t1.database=t2.database
WHERE (multiSearchAny(t1.type, ['Int', 'Float', 'Date', 'String', 'Decimal']) = '1')
GROUP BY
    database,
	dist,
    table
ORDER BY 
    database
`, ck.Config.Cluster)

	log.Logger.Debug(query)
	rows, err := ck.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tblLists := make([]RebalanceTables, 0)
	for rows.Next() {
		var database, dist, table string
		var cols []string
		err = rows.Scan(&database, &dist, &table, &cols)
		if err != nil {
			return nil, err
		}
		tblLists = append(tblLists, RebalanceTables{
			Database:  database,
			DistTable: dist,
			Table:     table,
			Columns:   cols,
		})
	}
	return tblLists, nil
}

func (ck *CkService) GetTblLists() (map[string]map[string][]string, error) {
	query := `SELECT
    t2.database AS database,
    t2.name AS table,
    groupArray(t1.name) AS rows
FROM system.columns AS t1
INNER JOIN
(
    SELECT
        database,
        name
    FROM system.tables
    WHERE match(engine, 'Distributed') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') )
) AS t2 ON t1.table = t2.name and t1.database=t2.database
GROUP BY
    database,
    table
ORDER BY 
    database`

	tblLists := make(map[string]map[string][]string)
	value, err := ck.QueryInfo(query)
	for i := 1; i < len(value); i++ {
		tblMapping := make(map[string][]string)
		database := value[i][0].(string)
		table := value[i][1].(string)
		cols := value[i][2].([]string)
		tableMap, isExist := tblLists[database]
		if isExist {
			tblMapping = tableMap
		}
		tblMapping[table] = cols
		tblLists[database] = tblMapping
	}
	return tblLists, err
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
	srcDB, err := common.ConnectClickHouse(hosts[0], src.Port, model.ClickHouseDefaultDB, src.User, src.Password)
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
	statementsqls, err := GetLogicSchema(srcDB, *dst.LogicCluster, dst.Cluster, dst.IsReplica, database, tableName)
	if err != nil {
		log.Logger.Warnf("get logic schema failed: %v", err)
		return false
	}

	dstDB, err := common.ConnectClickHouse(dst.Hosts[0], dst.Port, model.ClickHouseDefaultDB, dst.User, dst.Password)
	if err != nil {
		log.Logger.Warnf("can't connect %s", dst.Hosts[0])
		return false
	}
	for _, schema := range statementsqls {
		for _, statement := range schema.Statements {
			log.Logger.Debugf("%s", statement)
			if _, err := dstDB.Exec(statement); err != nil {
				log.Logger.Warnf("excute sql failed: %v", err)
				return false
			}
		}
	}
	return true
}

func RestoreReplicaTable(conf *model.CKManClickHouseConfig, host, database, table string) error {
	db, err := common.ConnectClickHouse(host, conf.Port, database, conf.User, conf.Password)
	if err != nil {
		return errors.Wrapf(err, "cann't connect to %s", host)
	}
	query := "SYSTEM RESTART REPLICA " + table
	if _, err := db.Exec(query); err != nil {
		return errors.Wrap(err, host)
	}
	query = "SYSTEM RESTORE REPLICA " + table
	if _, err := db.Exec(query); err != nil {
		return errors.Wrap(err, host)
	}
	return nil
}

func RebalanceCluster(conf *model.CKManClickHouseConfig, keys []model.RebalanceShardingkey, allTable, exceptMaxShard bool) error {
	var err error
	var exceptHost, target string
	service := NewCkService(conf)
	if err = service.InitCkService(); err != nil {
		return err
	}

	//check the full scale, if table not in the request, rebalance by partition
	keys, err = paddingKeys(keys, service, allTable)
	if err != nil {
		return err
	}

	hosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return err
	}
	if err = checkBasicTools(conf, hosts, keys); err != nil {
		return err
	}

	if exceptMaxShard {
		exceptHost = hosts[len(hosts)-1]
		hosts = hosts[:len(hosts)-1]
		target, err = checkDiskSpace(hosts, exceptHost)
		if err != nil {
			return err
		}
	}

	log.Logger.Debugf("keys: %d, %#v", len(keys), keys)
	for _, key := range keys {
		if exceptMaxShard {
			if err = MoveExceptToOthers(conf, exceptHost, target, key.Database, key.Table); err != nil {
				return err
			}
		}
		rebalancer := &CKRebalance{
			Cluster:    conf.Cluster,
			Hosts:      hosts,
			Port:       conf.Port,
			User:       conf.User,
			Password:   conf.Password,
			Database:   key.Database,
			Table:      key.Table,
			DistTable:  key.DistTable,
			DataDir:    conf.Path,
			OsUser:     conf.SshUser,
			OsPassword: conf.SshPassword,
			OsPort:     conf.SshPort,
			RepTables:  make(map[string]string),
		}
		defer rebalancer.Close()

		if key.ShardingKey != "" {
			//rebalance by shardingkey
			log.Logger.Infof("table %s.%s rebalance by shardingkey", key.Database, key.Table)
			if err = getShardingType(&key, service.DB); err != nil {
				return err
			}
			rebalancer.Shardingkey = key
			if err = RebalanceByShardingkey(conf, rebalancer); err != nil {
				return err
			}
		} else {
			//rebalance by partition
			log.Logger.Infof("table %s.%s rebalance by partition", key.Database, key.Table)
			err = RebalanceByPartition(conf, rebalancer)
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func checkBasicTools(conf *model.CKManClickHouseConfig, hosts []string, keys []model.RebalanceShardingkey) error {
	var chkrsync, chkawk bool
	for _, key := range keys {
		if chkawk && chkrsync {
			break
		}
		if key.ShardingKey != "" {
			chkawk = true
		} else {
			// by partition
			if !conf.IsReplica {
				chkrsync = true
			}
		}
	}
	for _, host := range hosts {
		opts := common.SshOptions{
			User:             conf.SshUser,
			Password:         conf.SshPassword,
			Port:             conf.SshPort,
			Host:             host,
			NeedSudo:         conf.NeedSudo,
			AuthenticateType: conf.AuthenticateType,
		}

		var cmds []string
		if chkawk {
			cmds = append(cmds, "which awk >/dev/null 2>&1 ;echo $?")
		}
		if chkrsync {
			cmds = append(cmds, "which rsync >/dev/null 2>&1 ;echo $?")
		}
		for _, cmd := range cmds {
			if output, err := common.RemoteExecute(opts, cmd); err != nil {
				return err
			} else {
				if strings.TrimSuffix(output, "\n") != "0" {
					return errors.Errorf("excute cmd:[%s] on %s failed", cmd, host)
				}
			}
		}
	}
	return nil
}

func checkDiskSpace(hosts []string, exceptHost string) (string, error) {
	var needSpace, maxLeftSpace int
	var target string
	query := `SELECT sum(total_bytes)
FROM system.tables
WHERE match(engine, 'MergeTree') AND (database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA'))
SETTINGS skip_unavailable_shards = 1`
	log.Logger.Debugf("[%s]%s", exceptHost, query)
	db := common.GetConnection(exceptHost)
	rows, err := db.Query(query)
	if err != nil {
		return "", errors.Wrap(err, exceptHost)
	}
	for rows.Next() {
		rows.Scan(&needSpace)
	}
	query = "SELECT free_space FROM system.disks"
	for _, host := range hosts {
		db = common.GetConnection(host)
		log.Logger.Debugf("[%s]%s", host, query)
		rows, err := db.Query(query)
		if err != nil {
			return "", errors.Wrap(err, exceptHost)
		}
		var freeSpace int
		for rows.Next() {
			rows.Scan(&freeSpace)
		}
		if maxLeftSpace*2 < freeSpace {
			target = host
			maxLeftSpace = freeSpace
		}
	}
	if maxLeftSpace <= needSpace {
		return "", fmt.Errorf("need %s space on the disk, but not enough", common.ConvertDisk(uint64(needSpace)))
	}
	return target, nil
}

func paddingKeys(keys []model.RebalanceShardingkey, service *CkService, allTable bool) ([]model.RebalanceShardingkey, error) {
	var results []model.RebalanceShardingkey
	resps, err := service.GetRebalanceTables()
	if err != nil {
		return keys, err
	}
	//k: database, v:tables
	for _, rt := range resps {
		key := model.RebalanceShardingkey{
			Database:  rt.Database,
			Table:     rt.Table,
			DistTable: rt.DistTable,
		}
		found := false
		for _, elem := range keys {
			elem.Table = common.TernaryExpression(strings.HasPrefix(elem.Table, "^"), elem.Table, "^"+elem.Table).(string)
			elem.Table = common.TernaryExpression(strings.HasSuffix(elem.Table, "$"), elem.Table, elem.Table+"$").(string)
			reg, err := regexp.Compile(elem.Table)
			if err != nil {
				return keys, err
			}
			if key.Database == elem.Database && (reg.MatchString(key.Table)) {
				if found {
					return keys, fmt.Errorf("table %s matches more than one regexp expression", key.Table)
				}
				if common.ArraySearch(elem.ShardingKey, rt.Columns) {
					found = true
					key.ShardingKey = elem.ShardingKey
					results = append(results, key)
					//break
				} else {
					return keys, fmt.Errorf("shardingkey %s not found in %s.%s", elem.ShardingKey, elem.Database, key.Table)
				}
			}
			if allTable && !found {
				results = append(results, key)
			}
		}
	}

	return results, nil
}

func getShardingType(key *model.RebalanceShardingkey, db *sql.DB) error {
	query := fmt.Sprintf("SELECT type FROM system.columns WHERE (database = '%s') AND (table = '%s') AND (name = '%s') ",
		key.Database, key.Table, key.ShardingKey)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var typ string
		rows.Scan(&typ)
		key.ShardingType = WhichType(typ)
	}
	if key.ShardingType.Nullable || key.ShardingType.Array {
		return errors.Errorf("invalid shardingKey %s, expect its type be numerical or string", key.ShardingKey)
	}
	return nil
}

func RebalanceByPartition(conf *model.CKManClickHouseConfig, rebalancer *CKRebalance) error {
	var err error
	if err = rebalancer.InitCKConns(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}

	if err = rebalancer.GetRepTables(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}

	if err = rebalancer.DoRebalanceByPart(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}
	log.Logger.Infof("rebalance done")
	return nil
}

// 200w data costs 4s
func RebalanceByShardingkey(conf *model.CKManClickHouseConfig, rebalancer *CKRebalance) error {
	var err error
	if err = rebalancer.InitCKConns(); err != nil {
		log.Logger.Errorf("got error %+v", err)
		return err
	}

	if err = rebalancer.CreateTemporaryTable(); err != nil {
		return err
	}

	if err = rebalancer.InsertPlan(); err != nil {
		return err
	}

	if err = rebalancer.MoveBack(); err != nil {
		return errors.Wrapf(err, "table %s.%s rebalance failed, data can be corrupted, please move back from temp table[%s] manually", rebalancer.Database, rebalancer.Table, fmt.Sprintf("tmp_%s", rebalancer.Table))
	}

	rebalancer.Cleanup()
	return nil
}

func MoveExceptToOthers(conf *model.CKManClickHouseConfig, except, target, database, table string) error {
	max_insert_threads := runtime.NumCPU()*3/4 + 1
	query := fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM remote('%s', '%s', '%s', '%s', '%s') SETTINGS max_insert_threads=%d",
		database, table, except, database, table, conf.User, conf.Password, max_insert_threads)
	log.Logger.Debugf("[%s] %s", target, query)
	db := common.GetConnection(target)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	query = fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`", database, table)
	log.Logger.Debugf("[%s] %s", except, query)
	db = common.GetConnection(except)
	_, err = db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
