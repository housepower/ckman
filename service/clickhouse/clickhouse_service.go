package clickhouse

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

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
	Conn   *common.Conn
}

func NewCkService(config *model.CKManClickHouseConfig) *CkService {
	ck := &CkService{}
	config.Normalize()
	ck.Config = config
	ck.Conn = nil

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
		connect, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, ck.Config.GetConnOption())
		if err == nil {
			ck.Conn = connect
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

func (ck *CkService) CreateTable(params *model.CreateCkTableParams, dryrun bool) ([]string, error) {
	var statements []string
	if ck.Conn == nil {
		return statements, errors.Errorf("clickhouse service unavailable")
	}

	ensureDatabaseSql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER `%s`", params.DB, params.Cluster)
	statements = append(statements, ensureDatabaseSql)
	if !dryrun {
		_ = ck.Conn.Exec(ensureDatabaseSql)
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

	settings := make(map[string]interface{})
	// if common.CompareClickHouseVersion(ck.Config.Version, "22.4.x") > 0 {
	// 	settings["use_metadata_cache"] = true
	// }

	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` (%s%s%s) ENGINE = %s() PARTITION BY %s ORDER BY (%s)",
		params.DB, params.Name, params.Cluster, strings.Join(columns, ", "), params.IndexExpr, projections, params.Engine,
		partition, strings.Join(params.Order, ", "))
	if params.TTLExpr != "" {
		create += fmt.Sprintf(" TTL %s", params.TTLExpr)
	}
	if params.StoragePolicy != "" {
		settings["storage_policy"] = params.StoragePolicy
	}
	if len(settings) > 0 {
		create += " SETTINGS "
		idx := 1
		for k, v := range settings {
			if idx == len(settings) {
				create += fmt.Sprintf("%s = '%v'", k, v)
			} else {
				create += fmt.Sprintf("%s = '%v',", k, v)
			}
			idx++
		}
	}

	log.Logger.Debugf(create)
	statements = append(statements, create)
	if !dryrun {
		if err := ck.Conn.Exec(create); err != nil {
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
		if err := ck.Conn.Exec(create); err != nil {
			if ok := checkTableIfExists(params.DB, params.DistName, params.Cluster); !ok {
				return statements, err
			}
		}
	}
	return statements, nil
}

func (ck *CkService) CreateDistTblOnLogic(params *model.DistLogicTblParams) error {
	local, _, err := common.GetTableNames(ck.Conn, params.Database, params.TableName, params.DistName, params.ClusterName, true)
	if err != nil {
		return err
	}
	createSql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		params.Database, common.ClickHouseDistTableOnLogicPrefix, local, params.ClusterName,
		params.Database, local, params.LogicCluster, params.Database, local)

	log.Logger.Debug(createSql)
	if err := ck.Conn.Exec(createSql); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (ck *CkService) DeleteDistTblOnLogic(params *model.DistLogicTblParams) error {
	local, _, err := common.GetTableNames(ck.Conn, params.Database, params.TableName, params.DistName, params.ClusterName, true)
	if err != nil {
		return err
	}
	deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s%s` ON CLUSTER `%s` SYNC",
		params.Database, common.ClickHouseDistTableOnLogicPrefix, local, params.ClusterName)
	log.Logger.Debug(deleteSql)
	if err := ck.Conn.Exec(deleteSql); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (ck *CkService) DeleteTable(conf *model.CKManClickHouseConfig, params *model.DeleteCkTableParams) error {
	if ck.Conn == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	local, dist, err := common.GetTableNames(ck.Conn, params.DB, params.Name, params.DistName, params.Cluster, true)
	if err != nil {
		return err
	}
	if dist != "" {
		deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", params.DB,
			dist, params.Cluster)
		log.Logger.Debugf(deleteSql)
		if err := ck.Conn.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}
	}

	if local != "" {
		deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC", params.DB, local, params.Cluster)
		log.Logger.Debugf(deleteSql)
		if err := ck.Conn.Exec(deleteSql); err != nil {
			return errors.Wrap(err, "")
		}
	}

	return nil
}

func (ck *CkService) AlterTable(params *model.AlterCkTableParams) error {
	if ck.Conn == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	local, dist, err := common.GetTableNames(ck.Conn, params.DB, params.Name, params.DistName, params.Cluster, true)
	if err != nil {
		return err
	}
	// add column
	for _, value := range params.Add {
		add := ""
		if value.After != "" {
			add = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS `%s` %s %s AFTER `%s` %s",
				params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "), value.After, common.WithAlterSync(ck.Config.Version))
		} else {
			add = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD COLUMN IF NOT EXISTS `%s` %s %s %s",
				params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "), common.WithAlterSync(ck.Config.Version))
		}
		log.Logger.Debugf(add)
		if err := ck.Conn.Exec(add); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// modify column
	for _, value := range params.Modify {
		query := fmt.Sprintf("SELECT CAST(`%s`, '%s') FROM `%s`.`%s`", value.Name, value.Type, params.DB, local)
		log.Logger.Debug(query)
		if rows, err := ck.Conn.Query(query); err != nil {
			return errors.Wrapf(err, "can't modify %s to %s", value.Name, value.Type)
		} else {
			rows.Close()
		}

		modify := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MODIFY COLUMN IF EXISTS `%s` %s %s %s",
			params.DB, local, params.Cluster, value.Name, value.Type, strings.Join(value.Options, " "), common.WithAlterSync(ck.Config.Version))
		log.Logger.Debugf(modify)
		if err := ck.Conn.Exec(modify); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// delete column
	for _, value := range params.Drop {
		drop := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` DROP COLUMN IF EXISTS `%s` %s",
			params.DB, local, params.Cluster, value, common.WithAlterSync(ck.Config.Version))
		log.Logger.Debugf(drop)
		if err := ck.Conn.Exec(drop); err != nil {
			return errors.Wrap(err, "")
		}
	}

	//rename column
	for _, value := range params.Rename {
		if value.From == "" || value.To == "" {
			return errors.Errorf("form %s or to %s must not be empty", value.From, value.To)
		}
		rename := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` RENAME COLUMN IF EXISTS `%s` TO `%s` %s",
			params.DB, local, params.Cluster, value.From, value.To, common.WithAlterSync(ck.Config.Version))

		log.Logger.Debugf(rename)
		if err := ck.Conn.Exec(rename); err != nil {
			return errors.Wrap(err, "")
		}
	}

	for _, p := range params.Projections {
		var query string
		if p.Action == model.ProjectionAdd {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD PROJECTION %s (%s)",
				params.DB, params.Name, params.Cluster, p.Name, p.Sql)
		} else if p.Action == model.ProjectionDrop {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` DROP PROJECTION %s", params.DB, local, params.Cluster, p.Name)
		} else if p.Action == model.ProjectionClear {
			query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` CLEAR PROJECTION %s", params.DB, local, params.Cluster, p.Name)
		}
		if query != "" {
			if err := ck.Conn.Exec(query); err != nil {
				return errors.Wrap(err, "")
			}
			if p.Action == model.ProjectionAdd {
				// trigger history data
				query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MATERIALIZE PROJECTION %s", params.DB, local, params.Cluster, p.Name)
				if err := ck.Conn.Exec(query); err != nil {
					return errors.Wrap(err, "")
				}
			}
		}
	}

	for _, index := range params.AddIndex {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` ADD INDEX %s %s TYPE %s GRANULARITY %d",
			params.DB, local, params.Cluster, index.Name, index.Field, index.Type, index.Granularity)

		if err := ck.Conn.Exec(query); err != nil {
			return errors.Wrap(err, "")
		}

		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MATERIALIZE INDEX %s", params.DB, local, params.Cluster, index.Name)
		if err := ck.Conn.Exec(query); err != nil {
			return errors.Wrap(err, "")
		}
	}

	for _, index := range params.DropIndex {
		query := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` DROP INDEX %s", params.DB, local, params.Cluster, index.Name)

		if err := ck.Conn.Exec(query); err != nil {
			return errors.Wrap(err, "")
		}
	}

	// 删除分布式表并重建
	deleteSql := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` ON CLUSTER `%s` SYNC",
		params.DB, dist, params.Cluster)
	log.Logger.Debugf(deleteSql)
	if err := ck.Conn.Exec(deleteSql); err != nil {
		return errors.Wrap(err, "")
	}

	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` ON CLUSTER `%s` AS `%s`.`%s` ENGINE = Distributed(`%s`, `%s`, `%s`, rand())",
		params.DB, dist, params.Cluster, params.DB, local,
		params.Cluster, params.DB, local)
	log.Logger.Debugf(create)
	if err := ck.Conn.Exec(create); err != nil {
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
	if ck.Conn == nil {
		return errors.Errorf("clickhouse service unavailable")
	}

	var wg sync.WaitGroup
	wg.Add(len(req.Tables))
	var lastErr error
	for _, table := range req.Tables {
		go func(table model.AlterTblTTL) {
			defer wg.Done()
			local, _, err := common.GetTableNames(ck.Conn, table.Database, table.TableName, table.DistName, ck.Config.Cluster, true)
			if err != nil {
				lastErr = err
				return
			}
			if req.TTLType != "" {
				if req.TTLType == model.TTLTypeModify {
					if req.TTLExpr != "" {
						ttl := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` MODIFY TTL %s %s", table.Database, local, ck.Config.Cluster, req.TTLExpr, common.WithAlterSync(ck.Config.Version))
						log.Logger.Debugf(ttl)
						if err := ck.Conn.Exec(ttl); err != nil {
							if common.ExceptionAS(err, common.UNFINISHED) {
								var create_table_query string
								query := fmt.Sprintf("select create_table_query from system.tables where database = '%s' and name = '%s'", table.Database, local)
								err = ck.Conn.QueryRow(query).Scan(&create_table_query)
								if err != nil {
									lastErr = err
									return
								}
								if strings.Contains(create_table_query, req.TTLExpr) || strings.Contains(create_table_query, strings.ReplaceAll(req.TTLExpr, "`", "")) {
									return
								}
							}
							lastErr = err
							return
						}
					}
				} else if req.TTLType == model.TTLTypeRemove {
					ttl := fmt.Sprintf("ALTER TABLE `%s`.`%s` ON CLUSTER `%s` REMOVE TTL %s", table.Database, local, ck.Config.Cluster, common.WithAlterSync(ck.Config.Version))
					log.Logger.Debugf(ttl)
					if err := ck.Conn.Exec(ttl); err != nil {
						if common.ExceptionAS(err, common.UNFINISHED) {
							return
						}
						lastErr = err
						return
					}
				}
			}
		}(table)
	}

	wg.Wait()
	return lastErr
}

func (ck *CkService) DescTable(params *model.DescCkTableParams) ([]model.CkColumnAttribute, error) {
	attrs := make([]model.CkColumnAttribute, 0)
	if ck.Conn == nil {
		return attrs, errors.Errorf("clickhouse service unavailable")
	}

	desc := fmt.Sprintf("DESCRIBE TABLE `%s`.`%s`", params.DB, params.Name)
	log.Logger.Debugf(desc)
	rows, err := ck.Conn.Query(desc)
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
	if ck.Conn == nil {
		return nil, errors.Errorf("clickhouse service unavailable")
	}

	log.Logger.Debugf(query)
	rows, err := ck.Conn.Query(query)
	if err != nil {
		if err != io.EOF {
			return nil, errors.Wrap(err, "")
		} else {
			return [][]interface{}{
				{"result"},
				{"OK"},
			}, nil
		}
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	colData := make([][]interface{}, 0)
	colNames := make([]interface{}, len(cols))

	var columnPointers []interface{}
	ctps, _ := rows.ColumnTypes()
	for _, ctp := range ctps {
		if ctp.ScanType().Kind() == reflect.Ptr {
			column := reflect.New(ctp.ScanType().Elem()).Interface()
			columnPointers = append(columnPointers, column)
		} else {
			column := reflect.New(ctp.ScanType()).Interface()
			columnPointers = append(columnPointers, column)
		}
	}

	for i, colName := range cols {
		colNames[i] = colName
	}
	colData = append(colData, colNames)

	for rows.Next() {
		// Scan the result into the column pointers...
		if err = rows.Scan(columnPointers...); err != nil {
			return nil, errors.Wrap(err, "")
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make([]interface{}, len(cols))
		for i := range columnPointers {
			val := reflect.ValueOf(columnPointers[i]).Elem()
			m[i] = val.Interface()
			if val.CanSet() {
				val.SetZero()
			}
		}

		colData = append(colData, m)
		if len(colData) >= 10001 {
			break
		}
	}

	return colData, nil
}

func (ck *CkService) FetchSchemerFromOtherNode(host string) error {
	conn, err := common.ConnectClickHouse(host, model.ClickHouseDefaultDB, ck.Config.GetConnOption())
	if err != nil {
		return err
	}
	names, statements, err := GetCreateReplicaObjects(conn)
	if err != nil {
		return err
	}

	num := len(names)
	for i := 0; i < num; i++ {
		log.Logger.Debugf("statement: %s", statements[i])
		if err := ck.Conn.Exec(statements[i]); err != nil {
			log.Logger.Warnf("execute [%s] failed: %v", statements[i], err)
			return err
		}
	}

	return nil
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
	rows, err := ck.Conn.Query(query)
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
	if err != nil {
		return nil, err
	}
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

	query = `SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')`
	value, err = ck.QueryInfo(query)
	for i := 1; i < len(value); i++ {
		database := value[i][0].(string)
		if _, ok := tblLists[database]; !ok {
			tblLists[database] = make(map[string][]string)
		}
	}

	return tblLists, err
}

func DMLOnLogic(logics []string, req model.DMLOnLogicReq) error {
	var query string
	if req.Manipulation == model.DML_DELETE {
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` %s WHERE (1=1)", req.Database, req.Table, req.Manipulation)
	} else if req.Manipulation == model.DML_UPDATE {
		var kv string
		for k, v := range req.KV {
			kv += fmt.Sprintf(" `%s` = '%s',", k, v)
		}
		kv = kv[:len(kv)-1]
		query = fmt.Sprintf("ALTER TABLE `%s`.`%s` %s %s WHERE (1=1)", req.Database, req.Table, req.Manipulation, kv)
	}

	if req.Cond != "" {
		query += fmt.Sprintf(" AND (%s)", req.Cond)
	}
	var wg sync.WaitGroup
	var lastErr error
	for _, cluster := range logics {
		conf, err := repository.Ps.GetClusterbyName(cluster)
		if err != nil {
			return err
		}
		hosts, err := common.GetShardAvaliableHosts(&conf)
		if err != nil {
			return err
		}
		for _, host := range hosts {
			wg.Add(1)
			go func(host string) {
				defer wg.Done()
				conn := common.GetConnection(host)
				if conn == nil {
					lastErr = fmt.Errorf("%s can't connect clickhouse", host)
					return
				}
				log.Logger.Debugf("[%s]%s", host, query)
				err = conn.Exec(query)
				if err != nil {
					lastErr = err
					return
				}
			}(host)
		}
	}
	wg.Wait()
	return lastErr
}
