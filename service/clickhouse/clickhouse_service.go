package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/MakeNowJust/heredoc"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

const (
	ClickHouseDistributedTablePrefix string = "dist_"
	ClickHouseDefaultDB              string = "default"
	ClickHouseDefaultUser            string = "clickhouse"
	ClickHouseDefaultPassword        string = "Ck123456!"
	ClickHouseClustersFile           string = "clusters.json"
	ClickHouseDefaultPort            int    = 9000
	ClickHouseDefaultZkPort          int    = 2181
	ClickHouseServiceTimeout         int    = 3600
	ClickHouseQueryStart             string = "QueryStart"
	ClickHouseQueryFinish            string = "QueryFinish"
	ClickHouseQueryExStart           string = "ExceptionBeforeStart"
	ClickHouseQueryExProcessing      string = "ExceptionWhileProcessing"
)

var CkClusters sync.Map
var CkServices sync.Map

type CkService struct {
	Config *model.CKManClickHouseConfig
	DB     *sql.DB
}

type ClusterService struct {
	Service *CkService
	Timeout int64
}

func CkConfigFillDefault(config *model.CKManClickHouseConfig) {
	if config.DB == "" {
		config.DB = ClickHouseDefaultDB
	}
	if config.Port == 0 {
		config.Port = ClickHouseDefaultPort
	}
	if config.ZkPort == 0 {
		config.ZkPort = ClickHouseDefaultZkPort
	}
}

func NewCkService(config *model.CKManClickHouseConfig) *CkService {
	ck := &CkService{}

	CkConfigFillDefault(config)
	ck.Config = config
	ck.DB = nil

	return ck
}

func (ck *CkService) Stop() error {
	if ck.DB != nil {
		ck.DB.Close()
	}

	ck.DB = nil
	return nil
}

func (ck *CkService) InitCkService() error {
	if len(ck.Config.Hosts) == 0 {
		return fmt.Errorf("can't find any host")
	}

	dataSourceName := fmt.Sprintf("tcp://%s:%d?service=%s&username=%s&password=%s",
		ck.Config.Hosts[0], ck.Config.Port, ck.Config.DB, ck.Config.User, ck.Config.Password)
	if len(ck.Config.Hosts) > 1 {
		otherHosts := make([]string, 0)
		for _, host := range ck.Config.Hosts[1:] {
			otherHosts = append(otherHosts, fmt.Sprintf("%s:%d", host, ck.Config.Port))
		}
		dataSourceName += "&alt_hosts="
		dataSourceName += strings.Join(otherHosts, ",")
		dataSourceName += "&connection_open_strategy=random"
	}

	log.Logger.Infof("connect clickhouse with dsn '%s'", dataSourceName)
	connect, err := sql.Open("clickhouse", dataSourceName)
	if err != nil {
		return err
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Logger.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}

	ck.DB = connect
	return nil
}

func UnmarshalClusters() error {
	localFile := path.Join(config.GetWorkDirectory(), "conf", ClickHouseClustersFile)

	_, err := os.Stat(localFile)
	if err != nil {
		// file does not exist
		return nil
	}

	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		return err
	}

	clustersMap := make(map[string]model.CKManClickHouseConfig)
	err = json.Unmarshal(data, &clustersMap)
	if err != nil {
		return err
	}

	for key, value := range clustersMap {
		CkClusters.Store(key, value)
	}

	return nil
}

func MarshalClusters() error {
	clustersMap := make(map[string]model.CKManClickHouseConfig)
	CkClusters.Range(func(k, v interface{}) bool {
		clustersMap[k.(string)] = v.(model.CKManClickHouseConfig)
		return true
	})

	data, err := json.Marshal(clustersMap)
	if err != nil {
		return err
	}

	localFile := path.Join(config.GetWorkDirectory(), "conf", ClickHouseClustersFile)
	localFd, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	defer localFd.Close()

	num, err := localFd.Write(data)
	if err != nil {
		return err
	}

	if num != len(data) {
		return fmt.Errorf("do not write enough data")
	}

	return nil
}

func GetCkService(clusterName string) (*CkService, error) {
	cluster, ok := CkServices.Load(clusterName)
	if ok {
		clusterService := cluster.(*ClusterService)
		if time.Now().Unix() < clusterService.Timeout {
			return clusterService.Service, nil
		} else {
			// Fixme, Service maybe in use
			clusterService.Service.Stop()
		}
	}

	conf, ok := CkClusters.Load(clusterName)
	if ok {
		ckConfig := conf.(model.CKManClickHouseConfig)
		service := NewCkService(&ckConfig)
		if err := service.InitCkService(); err != nil {
			return nil, err
		}
		clusterService := &ClusterService{
			Service: service,
			Timeout: time.Now().Add(time.Second * time.Duration(ClickHouseServiceTimeout)).Unix(),
		}
		CkServices.Store(clusterName, clusterService)
		return service, nil
	} else {
		return nil, fmt.Errorf("can't find cluster %s service", clusterName)
	}
}

func GetCkNodeService(clusterName string, node string) (*CkService, error) {
	key := clusterName + node
	cluster, ok := CkServices.Load(key)
	if ok {
		clusterService := cluster.(*ClusterService)
		if time.Now().Unix() < clusterService.Timeout {
			return clusterService.Service, nil
		} else {
			// Fixme, Service maybe in use
			clusterService.Service.Stop()
		}
	}

	conf, ok := CkClusters.Load(clusterName)
	if ok {
		ckConfig := conf.(model.CKManClickHouseConfig)
		tmp := &model.CKManClickHouseConfig{
			Hosts:    []string{node},
			Port:     ckConfig.Port,
			Cluster:  ckConfig.Cluster,
			DB:       ckConfig.DB,
			User:     ckConfig.User,
			Password: ckConfig.Password,
		}
		service := NewCkService(tmp)
		if err := service.InitCkService(); err != nil {
			return nil, err
		}
		clusterService := &ClusterService{
			Service: service,
			Timeout: time.Now().Add(time.Second * time.Duration(ClickHouseServiceTimeout)).Unix(),
		}
		CkServices.Store(key, clusterService)
		return service, nil
	} else {
		return nil, fmt.Errorf("can't find cluster %s service", key)
	}
}

func GetCkClusterConfig(req model.CkImportConfig, conf *model.CKManClickHouseConfig) error {
	var replicas []model.CkReplica
	conf.Hosts = req.Hosts
	conf.Port = req.Port
	conf.Cluster = req.Cluster
	conf.User = req.User
	conf.Password = req.Password
	conf.ZkNodes = req.ZkNodes
	conf.ZkPort = req.ZkPort

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return err
	}
	defer service.Stop()
	conf.Hosts = make([]string, 0)
	conf.Names = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo("select cluster, shard_num, replica_num, host_name, host_address from system.clusters order by cluster, shard_num, replica_num")
	if err != nil {
		return err
	}
	shardNum := uint32(0)
	for i := 1; i < len(value); i++ {
		if value[i][0].(string) == conf.Cluster {
			if shardNum != value[i][1].(uint32) {
				if shardNum != 0 {
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
			conf.Names = append(conf.Names, value[i][3].(string))
			shardNum = value[i][1].(uint32)
		}
	}
	shard := model.CkShard{
		Replicas: replicas,
	}
	conf.Shards = append(conf.Shards, shard)

	value, err = service.QueryInfo("select version()")
	if err != nil {
		return err
	}
	conf.Version = value[1][0].(string)

	return nil
}

func GetCkClusterStatus(conf *model.CKManClickHouseConfig) []model.CkClusterNode {
	index := 0
	statusList := make([]model.CkClusterNode, len(conf.Hosts))

	for i, shard := range conf.Shards {
		for j, replica := range shard.Replicas {
			tmp := &model.CKManClickHouseConfig{
				Hosts:    []string{replica.Ip},
				Port:     conf.Port,
				Cluster:  conf.Cluster,
				User:     conf.User,
				Password: conf.Password,
			}
			status := model.CkClusterNode{
				Ip:            replica.Ip,
				HostName:      replica.HostName,
				ShardNumber:   i + 1,
				ReplicaNumber: j + 1,
			}
			service := NewCkService(tmp)
			if err := service.InitCkService(); err != nil {
				status.Status = model.CkStatusRed
			} else {
				status.Status = model.CkStatusGreen
			}
			service.Stop()
			statusList[index] = status
			index++
		}

	}

	return statusList
}

func (ck *CkService) CreateTable(params *model.CreateCkTableParams) error {
	if ck.DB == nil {
		return fmt.Errorf("clickhouse service unavailable")
	}

	columns := make([]string, 0)
	for _, value := range params.Fields {
		columns = append(columns, fmt.Sprintf("%s %s", value.Name, value.Type))
	}

	partition := ""
	switch params.Partition.Policy {
	case model.CkTablePartitionPolicyDay:
		partition = fmt.Sprintf("toYYYYMMDD(%s)", params.Partition.Name)
	case model.CkTablePartitionPolicyMonth:
		partition = fmt.Sprintf("toYYYYMM(%s)", params.Partition.Name)
	case model.CkTablePartitionPolicyWeek:
		partition = fmt.Sprintf("toYearWeek(%s)", params.Partition.Name)
	default:
		partition = fmt.Sprintf("toYYYYMMDD(%s)", params.Partition.Name)
	}

	create := fmt.Sprintf(`CREATE TABLE %s.%s ON CLUSTER %s (%s) ENGINE = %s() PARTITION BY %s ORDER BY (%s)`,
		params.DB, params.Name, params.Cluster, strings.Join(columns, ","), params.Engine,
		partition, strings.Join(params.Order, ","))
	if params.Engine == model.ClickHouseReplicaDefaultEngine {
		create = fmt.Sprintf(`CREATE TABLE %s.%s ON CLUSTER %s (%s) ENGINE = %s('/clickhouse/tables/{cluster}/{shard}/%s', '{replica}') PARTITION BY %s ORDER BY (%s)`,
			params.DB, params.Name, params.Cluster, strings.Join(columns, ","), params.Engine, params.Name,
			partition, strings.Join(params.Order, ","))
	}
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	create = fmt.Sprintf(`CREATE TABLE %s.%s%s ON CLUSTER %s AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster, params.Name,
		params.Cluster, params.DB, params.Name)
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DeleteTable(params *model.DeleteCkTableParams) error {
	if ck.DB == nil {
		return fmt.Errorf("clickhouse service unavailable")
	}

	delete := fmt.Sprintf("DROP TABLE %s.%s%s ON CLUSTER %s", params.DB, ClickHouseDistributedTablePrefix,
		params.Name, params.Cluster)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	delete = fmt.Sprintf("DROP TABLE %s.%s ON CLUSTER %s", params.DB, params.Name, params.Cluster)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) AlterTable(params *model.AlterCkTableParams) error {
	if ck.DB == nil {
		return fmt.Errorf("clickhouse service unavailable")
	}

	// add column
	for _, value := range params.Add {
		add := ""
		if value.After != "" {
			add = fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s ADD COLUMN %s %s AFTER %s",
				params.DB, params.Name, params.Cluster, value.Name, value.Type, value.After)
		} else {
			add = fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s ADD COLUMN %s %s",
				params.DB, params.Name, params.Cluster, value.Name, value.Type)
		}
		if _, err := ck.DB.Exec(add); err != nil {
			return err
		}
	}

	// modify column
	for _, value := range params.Modify {
		modify := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s MODIFY COLUMN %s %s",
			params.DB, params.Name, params.Cluster, value.Name, value.Type)
		if _, err := ck.DB.Exec(modify); err != nil {
			return err
		}
	}

	// delete column
	for _, value := range params.Drop {
		drop := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s DROP COLUMN %s",
			params.DB, params.Name, params.Cluster, value)
		if _, err := ck.DB.Exec(drop); err != nil {
			return err
		}
	}

	// 删除分布式表并重建
	delete := fmt.Sprintf("DROP TABLE %s.%s%s ON CLUSTER %s",
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	create := fmt.Sprintf(`CREATE TABLE %s.%s%s ON CLUSTER %s AS %s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster, params.Name,
		params.Cluster, params.DB, params.Name)
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DescTable(params *model.DescCkTableParams) ([]model.CkTableAttribute, error) {
	attrs := make([]model.CkTableAttribute, 0)
	if ck.DB == nil {
		return attrs, fmt.Errorf("clickhouse service unavailable")
	}

	desc := fmt.Sprintf("DESCRIBE TABLE %s.%s", params.DB, params.Name)
	rows, err := ck.DB.Query(desc)
	if err != nil {
		return attrs, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			name, types, default_type, default_expression, comments, codec_expression, ttl_expression string
		)
		if err := rows.Scan(&name, &types, &default_type, &default_expression, &comments, &codec_expression, &ttl_expression); err != nil {
			return []model.CkTableAttribute{}, err
		}
		attr := model.CkTableAttribute{
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
		return nil, fmt.Errorf("clickhouse service unavailable")
	}

	rows, err := ck.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
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

	for rows.Next() {
		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make([]interface{}, len(cols))
		for i, _ := range columnPointers {
			val := columnPointers[i].(*interface{})
			m[i] = *val
		}

		colData = append(colData, m)
	}

	return colData, nil
}

func (ck *CkService) FetchSchemerFromOtherNode(host string) error {
	names, statements, err := getCreateReplicaObjects(ck.DB, host)
	if err != nil {
		return err
	}

	num := len(names)
	for i := 0; i < num; i++ {
		if _, err = ck.DB.Exec(statements[i]); err != nil {
			return err
		}
	}

	return nil
}

func getObjectListFromClickHouse(db *sql.DB, query string) (names, statements []string, err error) {
	// Some data available, let's fetch it
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, statement string
		if err := rows.Scan(&name, &statement); err != nil {
			return nil, nil, err
		}
		names = append(names, name)
		statements = append(statements, statement)
	}

	return
}

func getCreateReplicaObjects(db *sql.DB, srcHost string) (names, statements []string, err error) {
	system_tables := fmt.Sprintf("remote('%s', system, tables)", srcHost)

	sqlDBs := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM system.tables
		WHERE database != 'system'
		SETTINGS skip_unavailable_shards = 1`,
		"system.tables", system_tables,
	))

	sqlTables := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM system.tables
		WHERE database != 'system' AND create_table_query != '' AND name not like '.inner.%'
		SETTINGS skip_unavailable_shards = 1`,
		"system.tables",
		system_tables,
	))

	names1, statements1, err := getObjectListFromClickHouse(db, sqlDBs)
	if err != nil {
		return nil, nil, err
	}
	names2, statements2, err := getObjectListFromClickHouse(db, sqlTables)
	if err != nil {
		return nil, nil, err
	}

	names = append(names1, names2...)
	statements = append(statements1, statements2...)
	return
}

func GetCkTableMetrics(conf *model.CKManClickHouseConfig) (map[string]*model.CkTableMetrics, error) {
	metrics := make(map[string]*model.CkTableMetrics)

	for _, host := range conf.Hosts {
		service, err := GetCkNodeService(conf.Cluster, host)
		if err != nil {
			return nil, err
		}

		// get table names
		query := fmt.Sprintf("SELECT DISTINCT name FROM system.tables WHERE (database = '%s') AND (engine LIKE '%%MergeTree%%')",
			service.Config.DB)
		value, err := service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if _, ok := metrics[tableName]; !ok {
				metric := &model.CkTableMetrics{}
				metrics[tableName] = metric
			}
		}

		// get columns
		query = fmt.Sprintf("SELECT table, count() AS columns FROM system.columns WHERE database = '%s' GROUP BY table",
			service.Config.DB)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if metric, ok := metrics[tableName]; ok {
				metric.Columns = value[i][1].(uint64)
			}
		}

		// get bytes, parts, rows
		query = fmt.Sprintf("SELECT table, sum(bytes_on_disk) AS bytes, count() AS parts, sum(rows) AS rows FROM system.parts WHERE (active = 1) AND (database = '%s') GROUP BY table",
			service.Config.DB)
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if metric, ok := metrics[tableName]; ok {
				metric.Space += value[i][1].(uint64)
				metric.Parts += value[i][2].(uint64)
				metric.Rows += value[i][3].(uint64)
			}
		}

		// get success, failed counts
		query = fmt.Sprintf("SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name, type, count() AS counts from system.query_log where tbl_name != '' AND is_initial_query=1 AND event_time >= subtractDays(now(), 1) group by tbl_name, type")
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if metric, ok := metrics[tableName]; ok {
				types := value[i][1].(string)
				if types == ClickHouseQueryFinish {
					metric.CompletedQueries += value[i][2].(uint64)
				} else if types == ClickHouseQueryExStart || types == ClickHouseQueryExProcessing {
					metric.FailedQueries += value[i][2].(uint64)
				}
			}
		}

		// get query duration
		query = fmt.Sprintf("SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name, quantiles(0.5, 0.99, 1.0)(query_duration_ms) AS duration from system.query_log where tbl_name != '' AND type = 2 AND is_initial_query=1 AND event_time >= subtractDays(now(), 7) group by tbl_name")
		value, err = service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		for i := 1; i < len(value); i++ {
			tableName := value[i][0].(string)
			if metric, ok := metrics[tableName]; ok {
				durations := value[i][1].([]float64)
				if durations[0] > metric.QueryCost.Middle {
					metric.QueryCost.Middle = durations[0]
				}
				if durations[1] > metric.QueryCost.SecondaryMax {
					metric.QueryCost.SecondaryMax = durations[1]
				}
				if durations[2] > metric.QueryCost.Max {
					metric.QueryCost.Max = durations[2]
				}
			}
		}
	}

	return metrics, nil
}
