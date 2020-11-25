package clickhouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"gitlab.eoitek.net/EOI/ckman/common"
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
)

var CkClusters sync.Map
var CkServices sync.Map

type CkService struct {
	Config *config.CKManClickHouseConfig
	DB     *sql.DB
}

type ClusterService struct {
	Service *CkService
	Time    int64
}

func CkConfigFillDefault(config *config.CKManClickHouseConfig) {
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

func NewCkService(config *config.CKManClickHouseConfig) *CkService {
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
	localFile := path.Join(common.GetWorkDirectory(), "conf", ClickHouseClustersFile)

	_, err := os.Stat(localFile)
	if err != nil {
		// file does not exist
		return nil
	}

	data, err := ioutil.ReadFile(localFile)
	if err != nil {
		return err
	}

	clustersMap := make(map[string]config.CKManClickHouseConfig)
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
	clustersMap := make(map[string]config.CKManClickHouseConfig)
	CkClusters.Range(func(k, v interface{}) bool {
		clustersMap[k.(string)] = v.(config.CKManClickHouseConfig)
		return true
	})

	data, err := json.Marshal(clustersMap)
	if err != nil {
		return err
	}

	localFile := path.Join(common.GetWorkDirectory(), "conf", ClickHouseClustersFile)
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
		return clusterService.Service, nil
	} else {
		conf, ok := CkClusters.Load(clusterName)
		if ok {
			ckConfig := conf.(config.CKManClickHouseConfig)
			service := NewCkService(&ckConfig)
			if err := service.InitCkService(); err != nil {
				return nil, err
			}
			clusterService := &ClusterService{
				Service: service,
				Time:    time.Now().Unix(),
			}
			CkServices.Store(clusterName, clusterService)
			return service, nil
		} else {
			return nil, fmt.Errorf("can't find cluster %s service", clusterName)
		}
	}
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
