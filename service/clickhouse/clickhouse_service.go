package clickhouse

import (
	"database/sql"
	"fmt"
	"github.com/MakeNowJust/heredoc"
	json "github.com/bytedance/sonic"
	"github.com/bytedance/sonic/encoder"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
	"io/ioutil"
	"net"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	ClickHouseDistributedTablePrefix string = "dist_"
	ClickHouseDistTableOnLogicPrefix string = "dist_logic_"
	ClickHouseQueryStart             string = "QueryStart"
	ClickHouseQueryFinish            string = "QueryFinish"
	ClickHouseQueryExStart           string = "ExceptionBeforeStart"
	ClickHouseQueryExProcessing      string = "ExceptionWhileProcessing"
	ClickHouseConfigVersionKey       string = "ck_cluster_config_version"
	ClickHouseClustersFile           string = "clusters.json"
	ClickHouseServiceTimeout         int    = 3600
)

var CkClusters *model.CkClusters

type CkService struct {
	Config *model.CKManClickHouseConfig
	DB     *sql.DB
}

func init() {
	CkClusters = model.NewCkClusters()
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
	for _, host := range ck.Config.Hosts {
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
	if len(data) == 0 {
		return nil, nil
	}

	clustersMap := make(map[string]interface{})
	err := json.Unmarshal(data, &clustersMap)
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return clustersMap, nil
}

func convertFormatVersionV2(data []byte) (*model.CkClusters, error) {
	var ck map[string]interface{}
	clusters := model.NewCkClusters()
	var err error
	ck, err = UnmarshalClusters(data)
	if err != nil || ck == nil {
		return clusters, err
	}
	for key, value := range ck {
		v, ok := value.(map[string]interface{})
		if ok {
			var conf model.CKManClickHouseConfig
			jsonStr, _ := json.Marshal(v)
			_ = json.Unmarshal(jsonStr, &conf)
			conf.Normalize()
			clusters.SetClusterByName(key, conf)
		} else {
			clusters.SetConfigVersion(int(value.(float64)))
		}
	}
	return clusters, nil
}

func AddCkClusterConfigVersion() {
	version := CkClusters.GetConfigVersion()
	if version == -1 {
		CkClusters.SetConfigVersion(0)
	}
	CkClusters.SetConfigVersion(version + 1)
}

func UpdateLocalCkClusterConfig(data []byte) (updated bool, err error) {
	clusters := model.NewCkClusters()
	err = json.Unmarshal(data, &clusters)
	if err != nil {
		return false, err
	}

	if clusters.FormatVersion != model.CurrentFormatVersion {
		// need convert to V2
		log.Logger.Infof("need convert clusters.json to V2")
		clusters, err = convertFormatVersionV2(data)
		if err != nil {
			return false, err
		}
	}
	srcVersion := clusters.GetConfigVersion()
	dstVersion := CkClusters.GetConfigVersion()
	if srcVersion <= dstVersion {
		return false, nil
	}

	CkClusters.ClearClusters()
	CkClusters.SetConfigVersion(srcVersion)

	for key, value := range clusters.GetClusters() {
		value.Password = common.DesDecrypt(value.Password)
		if value.SshPassword != "" {
			value.SshPassword = common.DesDecrypt(value.SshPassword)
		}
		CkClusters.SetClusterByName(key, value)
	}
	for key, value := range clusters.GetLogicClusters() {
		CkClusters.SetLogicClusterByName(key, value)
	}
	CkClusters.FormatVersion = model.CurrentFormatVersion
	return true, nil
}

func ParseCkClusterConfigFile() error {
	data, err := ReadClusterConfigFile()
	if err != nil {
		return err
	}

	if data != nil {
		CkClusters.SetConfigVersion(-1)
		_, err := UpdateLocalCkClusterConfig(data)
		if err != nil {
			return err
		}
	}

	if CkClusters.GetConfigVersion() == -1 {
		CkClusters.FormatVersion = model.CurrentFormatVersion
		CkClusters.SetConfigVersion(0)
	}

	return err
}

func MarshalClusters() ([]byte, error) {
	tmp := model.NewCkClusters()
	_ = common.DeepCopyByGob(tmp, CkClusters)
	for key, value := range tmp.GetClusters() {
		value.Password = common.DesEncrypt(value.Password)
		if value.SshPasswordFlag == model.SshPasswordNotSave || value.SshPasswordFlag == model.SshPasswordUsePubkey {
			value.SshPassword = ""
		}
		if value.SshPassword != "" {
			value.SshPassword = common.DesEncrypt(value.SshPassword)
		}
		tmp.SetClusterByName(key, value)
	}
	data, err := encoder.EncodeIndented(tmp, "", "  ")
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
	conf, ok := CkClusters.GetClusterByName(clusterName)
	if ok {
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
	conf, ok := CkClusters.GetClusterByName(clusterName)
	if ok {
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
	conf.Hosts = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo(fmt.Sprintf("SELECT cluster, shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='%s' ORDER BY cluster, shard_num, replica_num", conf.Cluster))
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

func getNodeDisk(service *CkService) string {
	query := "SELECT free_space, total_space FROM system.disks"
	value, err := service.QueryInfo(query)
	if err != nil {
		return "NA/NA"
	}
	freeSpace := value[1][0].(uint64)
	totalSpace := value[1][1].(uint64)
	usedSpace := totalSpace - freeSpace
	return fmt.Sprintf("%s/%s", common.ConvertDisk(usedSpace), common.ConvertDisk(totalSpace))
}

func GetCkClusterStatus(conf *model.CKManClickHouseConfig) []model.CkClusterNode {
	index := 0
	statusList := make([]model.CkClusterNode, len(conf.Hosts))
	statusMap := make(map[string]string, len(conf.Hosts))
	diskMap := make(map[string]string, len(conf.Hosts))
	var lock sync.RWMutex
	pool := common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault)
	for _, host := range conf.Hosts {
		innerHost := host
		_ = pool.Submit(func() {
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
				diskMap[innerHost] = getNodeDisk(service)
				lock.Unlock()
			}
		})
	}
	pool.Wait()
	for i, shard := range conf.Shards {
		for j, replica := range shard.Replicas {
			status := model.CkClusterNode{
				Ip:            replica.Ip,
				HostName:      replica.HostName,
				ShardNumber:   i + 1,
				ReplicaNumber: j + 1,
				Status:        statusMap[replica.Ip],
				Disk:          diskMap[replica.Ip],
			}
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

	ensureDatabaseSql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s ON CLUSTER %s", params.DB, params.Cluster)
	_, _ = ck.DB.Exec(ensureDatabaseSql)

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
		if ok := checkTableIfExists(params.DB, params.Name, params.Cluster); !ok {
			return err
		}
	}

	create = fmt.Sprintf(`CREATE TABLE %s.%s%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster, params.DB, params.Name,
		params.Cluster, params.DB, params.Name)
	log.Logger.Debugf(create)
	if _, err := ck.DB.Exec(create); err != nil {
		name := fmt.Sprintf("%s%s", ClickHouseDistributedTablePrefix, params.Name)
		if ok := checkTableIfExists(params.DB, name, params.Cluster); !ok {
			return err
		}
	}

	return nil
}

func (ck *CkService) CreateDistTblOnLogic(params *model.CreateDistTblParams) error {
	if !checkTableIfExists(params.Database, params.TableName, params.ClusterName) {
		return fmt.Errorf("table %s.%s is not exist on cluster %s", params.Database, params.TableName, params.ClusterName)
	}
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.Database, ClickHouseDistTableOnLogicPrefix, params.TableName, params.ClusterName,
		params.Database, params.TableName, params.LogicName, params.Database, params.TableName)

	if _, err := ck.DB.Exec(createSql); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DeleteTable(conf *model.CKManClickHouseConfig, params *model.DeleteCkTableParams) error {
	if ck.DB == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	deleteSql := fmt.Sprintf("DROP TABLE %s.%s%s ON CLUSTER %s", params.DB, ClickHouseDistributedTablePrefix,
		params.Name, params.Cluster)
	log.Logger.Debugf(deleteSql)
	if _, err := ck.DB.Exec(deleteSql); err != nil {
		return err
	}

	deleteSql = fmt.Sprintf("DROP TABLE %s.%s ON CLUSTER %s", params.DB, params.Name, params.Cluster)
	log.Logger.Debugf(deleteSql)
	if _, err := ck.DB.Exec(deleteSql); err != nil {
		return err
	}

	//delete zoopath
	tableName := fmt.Sprintf("%s.%s", params.DB, params.Name)
	delete(conf.ZooPath, tableName)

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
		for i := range columnPointers {
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

	chHosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return nil, err
	}

	for _, host := range chHosts {
		service, err := GetCkNodeService(conf.Cluster, host)
		if err != nil {
			return nil, err
		}

		// get table names
		var databases []string
		var dbtables map[string][]string
		if databases, dbtables, err = common.GetMergeTreeTables("MergeTree", service.DB); err != nil {
			return nil, err
		}

		for db, tables := range dbtables {
			for _, table := range tables {
				tableName := fmt.Sprintf("%s.%s", db, table)
				if _, ok := metrics[tableName]; !ok {
					metric := &model.CkTableMetrics{
						RWStatus: true,
					}
					metrics[tableName] = metric
				}
			}
		}

		// get columns
		var query string
		var value [][]interface{}
		for _, database := range databases {
			query = fmt.Sprintf("SELECT table, count() AS columns FROM system.columns WHERE database = '%s' GROUP BY table",
				database)
			log.Logger.Infof("host: %s, query: %s", host, query)
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
			log.Logger.Infof("host: %s, query: %s", host, query)
			value, err = service.QueryInfo(query)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(value); i++ {
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
				if metric, ok := metrics[tableName]; ok {
					metric.DiskSpace += value[i][1].(uint64)
					metric.Parts += value[i][2].(uint64)
					metric.Rows += value[i][3].(uint64)
				}
			}

			// get readwrite_status
			query = fmt.Sprintf("select table, is_readonly from system.replicas where database = '%s'", database)
			value, err = service.QueryInfo(query)
			if err != nil {
				return nil, err
			}
			for i := 1; i < len(value); i++ {
				table := value[i][0].(string)
				tableName := fmt.Sprintf("%s.%s", database, table)
				if metric, ok := metrics[tableName]; ok {
					isReadonly := value[i][1].(uint8)
					if isReadonly != 0 {
						metric.RWStatus = false
					}
				}
			}

			// get success, failed counts
			query = "SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name, type, count() AS counts from system.query_log where tbl_name != '' AND is_initial_query=1 AND event_time >= subtractDays(now(), 1) group by tbl_name, type"
			log.Logger.Infof("host: %s, query: %s", host, query)
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
			query = "SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name, quantiles(0.5, 0.99, 1.0)(query_duration_ms) AS duration from system.query_log where tbl_name != '' AND type = 2 AND is_initial_query=1 AND event_time >= subtractDays(now(), 7) group by tbl_name"
			log.Logger.Infof("host: %s, query: %s", host, query)
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

	for key, metric := range metrics {
		metrics[key].Space = common.ConvertDisk(metric.DiskSpace)
		metrics[key].QueryCost.Max = common.Decimal(metric.QueryCost.Max)
		metrics[key].QueryCost.Middle = common.Decimal(metric.QueryCost.Middle)
		metrics[key].QueryCost.SecondaryMax = common.Decimal(metric.QueryCost.SecondaryMax)
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

	pool := common.NewWorkerPool(common.MaxWorkersDefault, 2*common.MaxWorkersDefault)
	var lastError error
	for _, host := range conf.Hosts {
		innerHost := host
		_ = pool.Submit(func() {
			service, err := GetCkNodeService(conf.Cluster, innerHost)
			if err != nil {
				log.Logger.Warnf("get ck node %s service error: %v", innerHost, err)
				return
			}

			sessions, err := getHostSessions(service, query)
			if err != nil {
				lastError = err
			}
			list = append(list, sessions...)
		})
	}
	pool.Wait()
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
	query := fmt.Sprintf("select subtractSeconds(now(), elapsed) AS query_start_time, toUInt64(elapsed*1000) AS query_duration_ms,  query, initial_user, initial_query_id, initial_address, thread_ids, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name from system.processes WHERE tbl_name != '' AND tbl_name != 'processes' AND tbl_name != 'query_log' AND is_initial_query=1 ORDER BY query_duration_ms DESC limit %d", limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, limit, query)
}

func GetCkSlowSessions(conf *model.CKManClickHouseConfig, cond model.SessionCond) ([]*model.CkSessionInfo, error) {
	query := fmt.Sprintf("SELECT query_start_time, query_duration_ms, query, initial_user, initial_query_id, initial_address, thread_ids, (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(dist_)?(\\w+)')[1])[4] AS tbl_name from system.query_log WHERE tbl_name != '' AND tbl_name != 'query_log' AND tbl_name != 'processes' AND type=2 AND is_initial_query=1 AND query_start_time >= parseDateTimeBestEffort('%d') AND query_start_time <= parseDateTimeBestEffort('%d') ORDER BY query_duration_ms DESC limit %d", cond.StartTime, cond.EndTime, cond.Limit)
	log.Logger.Debugf("query: %s", query)
	return getCkSessions(conf, cond.Limit, query)
}

func GetReplicaZkPath(conf *model.CKManClickHouseConfig) error {
	var err error
	service := NewCkService(conf)
	if err := service.InitCkService(); err != nil {
		log.Logger.Errorf("all hosts not available, can't get zoopath")
		return err
	}

	databases, dbtables, err := common.GetMergeTreeTables("Replicated\\\\w*MergeTree", service.DB)
	if err != nil {
		return err
	}

	//clear and reload again
	conf.ZooPath = make(map[string]string)
	for _, database := range databases {
		if tables, ok := dbtables[database]; ok {
			for _, table := range tables {
				path, err := getReplicaZkPath(service.DB, database, table)
				if err != nil {
					return err
				}
				tableName := fmt.Sprintf("%s.%s", database, table)
				conf.ZooPath[tableName] = path
			}
		}
	}
	return nil
}

func getReplicaZkPath(db *sql.DB, database, table string) (string, error) {
	var err error
	var path string
	var rows *sql.Rows
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table)
	log.Logger.Debugf("database:%s, table:%s: query: %s", database, table, query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return "", err
	}

	for rows.Next() {
		var result string
		if err = rows.Scan(&result); err != nil {
			err = errors.Wrapf(err, "")
			return "", err
		}

		myExp := regexp.MustCompile(`(ReplicatedMergeTree\(').*\b'`)
		paths := myExp.FindStringSubmatch(result)
		if len(paths) > 0 {
			path = strings.Split(paths[0], "'")[1]
		}
	}

	return path, nil
}

func ConvertZooPath(conf *model.CKManClickHouseConfig) []string {
	var zooPaths []string

	chHosts, err := common.GetShardAvaliableHosts(conf)
	if err != nil {
		return []string{}
	}
	for _, path := range conf.ZooPath {
		if path != "" {
			for index := range chHosts {
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
	conf, ok := CkClusters.GetClusterByName(cluster)
	if !ok {
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

func DropTableIfExists(params model.CreateCkTableParams, ck *CkService) {
	dropSql := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s ON CLUSTER %s", params.DB, params.Name, params.Cluster)
	log.Logger.Debugf(dropSql)
	_, _ = ck.DB.Exec(dropSql)

	dropSql = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s%s ON CLUSTER %s", params.DB, ClickHouseDistributedTablePrefix, params.Name, params.Cluster)
	log.Logger.Debugf(dropSql)
	_, _ = ck.DB.Exec(dropSql)
}

func (ck *CkService)ShowCreateTable(tbname, database string) (string, error) {
	query := fmt.Sprintf("SELECT create_table_query FROM system.tables WHERE database = '%s' AND name = '%s'", database, tbname)
	value, err := ck.QueryInfo(query)
	if err != nil {
		return "", err
	}
	schema := value[1][0].(string)
	return schema, nil
}
