package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/MakeNowJust/heredoc"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
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
	ZkStatusDefaultPort              int    = 8080
	ClickHouseQueryStart             string = "QueryStart"
	ClickHouseQueryFinish            string = "QueryFinish"
	ClickHouseQueryExStart           string = "ExceptionBeforeStart"
	ClickHouseQueryExProcessing      string = "ExceptionWhileProcessing"
	ClickHouseConfigVersionKey       string = "ck_cluster_config_version"
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
	if config.ZkStatusPort == 0 {
		config.ZkStatusPort = ZkStatusDefaultPort
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
		return errors.Errorf("can't find any host")
	}

	dataSourceName := fmt.Sprintf("tcp://%s:%d?service=%s&username=%s&password=%s",
		ck.Config.Hosts[0], ck.Config.Port, url.QueryEscape(ck.Config.DB), url.QueryEscape(ck.Config.User), url.QueryEscape(ck.Config.Password))
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
		return errors.Wrapf(err, "")
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			log.Logger.Errorf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return errors.Wrapf(err, "")
	}

	ck.DB = connect
	return nil
}

func ReadClusterConfigFile() ([]byte, error) {
	localFile := path.Join(config.GetWorkDirectory(), "conf", ClickHouseClustersFile)

	_, err := os.Stat(localFile)
	if err != nil {
		// file does not exist
		return nil, nil
	}

	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return data, nil
}

func UnmarshalClusters(data []byte) (map[string]interface{}, error) {
	if data == nil || len(data) == 0 {
		return nil, nil
	}

	clustersMap := make(map[string]interface{})
	err := json.Unmarshal(data, &clustersMap)
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return clustersMap, nil
}

func AddCkClusterConfigVersion() {
	version, ok := CkClusters.Load(ClickHouseConfigVersionKey)
	if !ok {
		CkClusters.Store(ClickHouseConfigVersionKey, 0)
	}

	CkClusters.Store(ClickHouseConfigVersionKey, version.(int)+1)
}

func UpdateLocalCkClusterConfig(data []byte) (updated bool, err error) {
	var ck map[string]interface{}
	ck, err = UnmarshalClusters(data)
	if err != nil || ck == nil {
		return
	}

	srcVersion, ok := ck[ClickHouseConfigVersionKey]
	if !ok {
		err = errors.Errorf("can't find source version")
		return
	}
	dstVersion, ok := CkClusters.Load(ClickHouseConfigVersionKey)
	if !ok {
		err = errors.Errorf("can't find version")
		return
	}

	if int(srcVersion.(float64)) <= dstVersion.(int) {
		return
	}

	// delete old CkClusters config
	CkClusters.Range(func(k, v interface{}) bool {
		CkClusters.Delete(k)
		return true
	})

	// merge new CkClusters config
	for key, value := range ck {
		v, ok := value.(map[string]interface{})
		if ok {
			var conf model.CKManClickHouseConfig
			jsonStr, _ := json.Marshal(v)
			_ = json.Unmarshal(jsonStr, &conf)
			CkClusters.Store(key, conf)
		} else {
			CkClusters.Store(key, int(value.(float64)))
		}
	}

	updated = true
	return
}

func ParseCkClusterConfigFile() error {
	data, err := ReadClusterConfigFile()
	if err != nil {
		return err
	}

	if data != nil {
		CkClusters.Store(ClickHouseConfigVersionKey, -1)
		_, err := UpdateLocalCkClusterConfig(data)
		if err != nil {
			return err
		}
	}

	_, ok := CkClusters.Load(ClickHouseConfigVersionKey)
	if !ok {
		CkClusters.Store(ClickHouseConfigVersionKey, 0)
	}

	return err
}

func MarshalClusters() ([]byte, error) {
	clustersMap := make(map[string]interface{})
	CkClusters.Range(func(k, v interface{}) bool {
		if _, ok := v.(model.CKManClickHouseConfig); ok {
			clustersMap[k.(string)] = v.(model.CKManClickHouseConfig)
		} else {
			clustersMap[k.(string)] = v.(int)
		}

		return true
	})

	data, err := json.Marshal(clustersMap)
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return data, nil
}

func WriteClusterConfigFile(data []byte) error {
	localFile := path.Join(config.GetWorkDirectory(), "conf", ClickHouseClustersFile)
	localFd, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrapf(err, "")
	}
	defer localFd.Close()

	num, err := localFd.Write(data)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	if num != len(data) {
		return errors.Errorf("didn't write enough data")
	}

	return nil
}

func UpdateCkClusterConfigFile() error {
	data, err := MarshalClusters()
	if err != nil {
		return err
	}

	return WriteClusterConfigFile(data)
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
		return nil, errors.Errorf("can't find cluster %s service", clusterName)
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
		return nil, errors.Errorf("can't find cluster %s service", key)
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
	conf.ZkStatusPort = req.ZkStatusPort

	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		return err
	}
	defer service.Stop()
	conf.Hosts = make([]string, 0)
	conf.Names = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo(fmt.Sprintf("SELECT cluster, shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='%s' ORDER BY cluster, shard_num, replica_num", req.Cluster))
	if err != nil {
		return err
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
		conf.Names = append(conf.Names, value[i][3].(string))
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
		return errors.Errorf("clickhouse service unavailable")
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

	create := fmt.Sprintf(`CREATE TABLE %s.%s ON CLUSTER %s (%s) ENGINE = %s() PARTITION BY %s ORDER BY (%s)`,
		params.DB, params.Name, params.Cluster, strings.Join(columns, ", "), params.Engine,
		partition, strings.Join(params.Order, ", "))
	if params.Engine == model.ClickHouseDefaultReplicaEngine || params.Engine == model.ClickHouseReplicaReplacingEngine {
		create = fmt.Sprintf(`CREATE TABLE %s.%s ON CLUSTER %s (%s) ENGINE = %s('/clickhouse/tables/{cluster}/{shard}/%s/%s', '{replica}') PARTITION BY %s ORDER BY (%s)`,
			params.DB, params.Name, params.Cluster, strings.Join(columns, ", "), params.Engine, params.DB, params.Name,
			partition, strings.Join(params.Order, ", "))
	}
	log.Logger.Debugf(create)
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	create = fmt.Sprintf(`CREATE TABLE %s.%s%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster, params.DB, params.Name,
		params.Cluster, params.DB, params.Name)
	log.Logger.Debugf(create)
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DeleteTable(params *model.DeleteCkTableParams) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	delete := fmt.Sprintf("DROP TABLE %s.%s%s ON CLUSTER %s", params.DB, ClickHouseDistributedTablePrefix,
		params.Name, params.Cluster)
	log.Logger.Debugf(delete)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	delete = fmt.Sprintf("DROP TABLE %s.%s ON CLUSTER %s", params.DB, params.Name, params.Cluster)
	log.Logger.Debugf(delete)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) AlterTable(params *model.AlterCkTableParams) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	// add column
	for _, value := range params.Add {
		add := ""
		if value.After != "" {
			add = fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s ADD COLUMN `%s` %s %s AFTER `%s`",
				params.DB, params.Name, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "), value.After)
		} else {
			add = fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s ADD COLUMN `%s` %s %s",
				params.DB, params.Name, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "))
		}
		log.Logger.Debugf(add)
		if _, err := ck.DB.Exec(add); err != nil {
			return err
		}
	}

	// modify column
	for _, value := range params.Modify {
		modify := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s MODIFY COLUMN `%s` %s %s",
			params.DB, params.Name, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "))
		log.Logger.Debugf(modify)
		if _, err := ck.DB.Exec(modify); err != nil {
			return err
		}
	}

	// delete column
	for _, value := range params.Drop {
		drop := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s DROP COLUMN `%s`",
			params.DB, params.Name, params.Cluster, value)
		log.Logger.Debugf(drop)
		if _, err := ck.DB.Exec(drop); err != nil {
			return err
		}
	}

	// 删除分布式表并重建
	delete := fmt.Sprintf("DROP TABLE %s.%s%s ON CLUSTER %s",
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster)
	log.Logger.Debugf(delete)
	if _, err := ck.DB.Exec(delete); err != nil {
		return err
	}

	create := fmt.Sprintf(`CREATE TABLE %s.%s%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster, params.DB, params.Name,
		params.Cluster, params.DB, params.Name)
	log.Logger.Debugf(create)
	if _, err := ck.DB.Exec(create); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DescTable(params *model.DescCkTableParams) ([]model.CkColumnAttribute, error) {
	attrs := make([]model.CkColumnAttribute, 0)
	if ck.DB == nil {
		return attrs, errors.Errorf("clickhouse service unavailable")
	}

	desc := fmt.Sprintf("DESCRIBE TABLE %s.%s", params.DB, params.Name)
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
		if err := rows.Scan(&name, &types, &default_type, &default_expression, &comments, &codec_expression, &ttl_expression); err != nil {
			return []model.CkColumnAttribute{}, err
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

	//TODO mabe replicas[0] disconnected?
	var chHosts []string
	for _, shard := range conf.Shards {
		chHosts = append(chHosts, shard.Replicas[0].Ip)
	}
	for _, host := range chHosts {
		service, err := GetCkNodeService(conf.Cluster, host)
		if err != nil {
			return nil, err
		}

		// get table names
		query := fmt.Sprintf("SELECT DISTINCT database, name FROM system.tables WHERE  (engine LIKE '%%MergeTree%%') AND database != 'system'")
		value, err := service.QueryInfo(query)
		if err != nil {
			return nil, err
		}
		var databases []string
		for i := 1; i < len(value); i++ {
			databse := value[i][0].(string)
			databases = append(databases, databse)
			name := value[i][1].(string)
			tableName := fmt.Sprintf("%s.%s", databse, name)
			if _, ok := metrics[tableName]; !ok {
				metric := &model.CkTableMetrics{}
				metrics[tableName] = metric
			}
		}

		// get columns
		for _, database := range databases {
			query = fmt.Sprintf("SELECT table, count() AS columns FROM system.columns WHERE database = '%s' GROUP BY table",
				database)
			value, err = service.QueryInfo(query)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(value); i++ {
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
				if metric, ok := metrics[tableName]; ok {
					metric.Columns = value[i][1].(uint64)
				}
			}

			// get bytes, parts, rows
			query = fmt.Sprintf("SELECT table, sum(bytes_on_disk) AS bytes, count() AS parts, sum(rows) AS rows FROM system.parts WHERE (active = 1) AND (database = '%s') GROUP BY table",
				database)
			value, err = service.QueryInfo(query)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(value); i++ {
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
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
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
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
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
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
	}

	return metrics, nil
}

func getHostSessions(service *CkService, query string) ([]*model.CkSessionInfo, error) {
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
		list = append(list, session)
	}

	return list, nil
}

func getCkSessions(conf *model.CKManClickHouseConfig, limit int, query string) ([]*model.CkSessionInfo, error) {
	list := make([]*model.CkSessionInfo, 0)

	for _, host := range conf.Hosts {
		service, err := GetCkNodeService(conf.Cluster, host)
		if err != nil {
			return nil, err
		}

		sessions, err := getHostSessions(service, query)
		if err != nil {
			return nil, err
		}
		list = append(list, sessions...)
	}

	sort.Sort(model.SessionList(list))
	if len(list) <= limit {
		return list, nil
	} else {
		return list[:limit], nil
	}
}

func GetCkOpenSessions(conf *model.CKManClickHouseConfig, limit int) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("select subtractSeconds(now(), elapsed) AS query_start_time, toUInt64(elapsed*1000) AS query_duration_ms,  query, initial_user, initial_query_id, initial_address, thread_ids, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name from system.processes WHERE tbl_name != '' AND tbl_name != 'processes' AND tbl_name != 'query_log' AND is_initial_query=1 ORDER BY query_duration_ms DESC limit %d", limit)

	return getCkSessions(conf, limit, query)
}

func GetCkSlowSessions(conf *model.CKManClickHouseConfig, limit int) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("SELECT query_start_time, query_duration_ms, query, initial_user, initial_query_id, initial_address, thread_ids, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name from system.query_log WHERE tbl_name != '' AND tbl_name != 'query_log' AND tbl_name != 'processes' AND type=2 AND is_initial_query=1 AND query_start_time >= subtractDays(now(), 7) ORDER BY query_duration_ms DESC limit %d", limit)

	return getCkSessions(conf, limit, query)
}
