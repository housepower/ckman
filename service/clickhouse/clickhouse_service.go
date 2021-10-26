package clickhouse

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/housepower/ckman/business"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/config"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
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

var json = jsoniter.ConfigCompatibleWithStandardLibrary
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

	data, err := os.ReadFile(localFile)
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
		if value.Password != "" {
			value.Password = common.DesDecrypt(value.Password)
		}
		if value.Mode == model.CkClusterDeploy && value.User != model.ClickHouseDefaultUser {
			//TODO move normal user to userconf
			log.Logger.Infof("user %s is not default, move to userconf", value.User)
			user := model.User{
				Name:     value.User,
				Password: value.Password,
				Quota:    model.ClickHouseUserQuotaDefault,
				Profile:  model.ClickHouseUserProfileDefault,
				Networks: model.Networks{IPs: []string{model.ClickHouseUserNetIpDefault}},
			}
			value.UsersConf.Users = append(value.UsersConf.Users, user)
			value.User = model.ClickHouseDefaultUser
			value.Password = ""
			log.Logger.Debugf("user:%s, password:%s", value.User, value.Password)
		}
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
		if value.Password != "" {
			value.Password = common.DesEncrypt(value.Password)
		}
		if value.AuthenticateType == model.SshPasswordNotSave || value.AuthenticateType == model.SshPasswordUsePubkey {
			value.SshPassword = ""
		}
		if value.SshPassword != "" {
			value.SshPassword = common.DesEncrypt(value.SshPassword)
		}
		tmp.SetClusterByName(key, value)
	}
	data, err := json.MarshalIndent(tmp, "", "  ")
	if err != nil {
		return nil, errors.Wrapf(err, "")
	}

	return data, nil
}

func WriteClusterConfigFile(data []byte) error {
	localFile := path.Join(config.GetWorkDirectory(), "conf", ClickHouseClustersFile)
	_ = os.Rename(localFile, fmt.Sprintf("%s.last", localFile))
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
	hosts := conf.Hosts
	conf.Hosts = make([]string, 0)
	conf.Shards = make([]model.CkShard, 0)

	value, err := service.QueryInfo(fmt.Sprintf("SELECT cluster, shard_num, replica_num, host_name, host_address FROM system.clusters WHERE cluster='%s' ORDER BY cluster, shard_num, replica_num", conf.Cluster))
	if err != nil {
		return err
	}
	if len(value) == 1 {
		err := fmt.Errorf("cluster %s is not exist, or hosts %v is not in cluster %s", conf.Cluster, hosts, conf.Cluster)
		return errors.Wrap(err, "")
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
	query := `SELECT
	formatReadableSize(total_space - free_space) AS used,
		formatReadableSize(total_space) AS total
	FROM system.disks`
	value, err := service.QueryInfo(query)
	if err != nil {
		return "NA/NA"
	}
	usedSpace := value[1][0].(string)
	totalSpace := value[1][1].(string)
	return fmt.Sprintf("%s/%s", usedSpace, totalSpace)
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
		create = fmt.Sprintf(`CREATE TABLE %s.%s ON CLUSTER %s (%s) ENGINE = %s('/clickhouse/tables/{cluster}/%s/%s/{shard}', '{replica}') PARTITION BY %s ORDER BY (%s)`,
			params.DB, params.Name, params.Cluster, strings.Join(columns, ", "), params.Engine, params.DB, params.Name,
			partition, strings.Join(params.Order, ", "))
	}
	if params.TTLExpr != "" {
		create += fmt.Sprintf(" TTL %s", params.TTLExpr)
	}
	if params.StoragePolicy != "" {
		create += fmt.Sprintf(" SETTINGS storage_policy = '%s'", params.StoragePolicy)
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

func (ck *CkService) CreateDistTblOnLogic(params *model.DistLogicTblParams) error {
	if !checkTableIfExists(params.Database, params.TableName, params.ClusterName) {
		return fmt.Errorf("table %s.%s is not exist on cluster %s", params.Database, params.TableName, params.ClusterName)
	}
	createSql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
		params.Database, ClickHouseDistTableOnLogicPrefix, params.TableName, params.ClusterName,
		params.Database, params.TableName, params.LogicCluster, params.Database, params.TableName)

	if _, err := ck.DB.Exec(createSql); err != nil {
		return err
	}

	return nil
}

func (ck *CkService) DeleteDistTblOnLogic(params *model.DistLogicTblParams) error {
	deleteSql := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s%s ON CLUSTER %s`,
		params.Database, ClickHouseDistTableOnLogicPrefix, params.TableName, params.ClusterName)
	if _, err := ck.DB.Exec(deleteSql); err != nil {
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

	// delete zoopath
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

	if params.TTLType != "" {
		if params.TTLType == model.TTLTypeModify {
			if params.TTLExpr != "" {
				ttl := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s MODIFY TTL %s", params.DB, params.Name, params.Cluster, params.TTLExpr)
				log.Logger.Debugf(ttl)
				if _, err := ck.DB.Exec(ttl); err != nil {
					return err
				}
			}
		} else if params.TTLType == model.TTLTypeRemove {
			ttl := fmt.Sprintf("ALTER TABLE %s.%s ON CLUSTER %s REMOVE TTL", params.DB, params.Name, params.Cluster)
			log.Logger.Debugf(ttl)
			if _, err := ck.DB.Exec(ttl); err != nil {
				return err
			}
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

	// 删除逻辑表并重建（如果有的话）
	conf, _ := CkClusters.GetClusterByName(params.Cluster)

	if conf.LogicCluster != nil {
		distParams := model.DistLogicTblParams{
			Database:     params.DB,
			TableName:    params.Name,
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

func (ck *CkService) FetchSchemerFromOtherNode(host, password string) error {
	names, statements, err := business.GetCreateReplicaObjects(ck.DB, host, model.ClickHouseDefaultUser, password)
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
			query = fmt.Sprintf("SELECT table, uniqExact(partition) AS partitions, count(*) AS parts, sum(data_compressed_bytes) AS compressed, sum(data_uncompressed_bytes) AS uncompressed, sum(rows) AS rows FROM system.parts WHERE (database = '%s') AND (active = '1') GROUP BY table;",
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
					metric.Partitions += value[i][1].(uint64)
					metric.Parts += value[i][2].(uint64)
					metric.Compressed += value[i][3].(uint64)
					metric.UnCompressed += value[i][4].(uint64)
					metric.Rows += value[i][5].(uint64)
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
			query = "SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(\\w+)')[1])[3] AS tbl_name, type, count() AS counts from system.query_log where tbl_name != '' AND is_initial_query=1 AND event_time >= subtractDays(now(), 1) group by tbl_name, type"
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
			query = "SELECT (extractAllGroups(query, '(from|FROM)\\s+(\\w+\\.)?(\\w+)')[1])[3] AS tbl_name, quantiles(0.5, 0.99, 1.0)(query_duration_ms) AS duration from system.query_log where tbl_name != '' AND type = 2 AND is_initial_query=1 AND event_time >= subtractDays(now(), 7) group by tbl_name"
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

	// clear and reload again
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
	query := fmt.Sprintf(`SELECT
		(extractAllGroups(create_table_query, '(MergeTree\\(\')(.*)\', \'{replica}\'\\)')[1])[2] AS zoopath
FROM system.tables
WHERE database = '%s' AND name = '%s'`, database, table)
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
		path = result
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

func (ck *CkService) ShowCreateTable(tbname, database string) (string, error) {
	query := fmt.Sprintf("SELECT create_table_query FROM system.tables WHERE database = '%s' AND name = '%s'", database, tbname)
	value, err := ck.QueryInfo(query)
	if err != nil {
		return "", err
	}
	schema := value[1][0].(string)
	return schema, nil
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
    WHERE match(engine, 'Distributed') AND (database != 'system')
) AS t2 ON t1.table = t2.name
GROUP BY
    database,
    table
ORDER BY 
    database`

	tblLists := make(map[string]map[string][]string)
	tblMapping := make(map[string][]string)
	var preValue string
	value, err := ck.QueryInfo(query)
	for i := 1; i < len(value); i++ {
		database := value[i][0].(string)
		table := value[i][1].(string)
		cols := value[i][2].([]string)
		tblMapping[table] = cols
		if preValue != "" && preValue != database {
			tblLists[preValue] = tblMapping
			tblMapping = make(map[string][]string)
		}
		preValue = database
	}
	tblLists[preValue] = tblMapping
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
