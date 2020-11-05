package clickhouse

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"gitlab.eoitek.net/EOI/ckman/config"
	"gitlab.eoitek.net/EOI/ckman/log"
	"gitlab.eoitek.net/EOI/ckman/model"
	"strings"
)

const (
	ClickHouseDistributedTablePrefix string = "dist_"
)

type CkService struct {
	config *config.CKManClickHouseConfig
	DB     *sql.DB
}

func NewCkService(config *config.CKManClickHouseConfig) *CkService {
	ck := &CkService{}
	ck.config = config
	return ck
}

func (ck *CkService) InitCkService() error {
	dataSourceName := fmt.Sprintf("tcp://%s?service=%s&username=%s&password=%s",
		ck.config.Hosts[0], ck.config.DB, ck.config.User, ck.config.Password)
	if len(ck.config.Hosts) > 1 {
		otherHosts := ck.config.Hosts[1:]
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

func (ck *CkService) Stop() error {
	if ck.DB != nil {
		return ck.DB.Close()
	}

	return nil
}

func (ck *CkService) CreateTable(params *model.CreateCkTableParams) error {
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
