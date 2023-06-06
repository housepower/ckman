package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/MakeNowJust/heredoc"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
)

// GetObjectListFromClickHouse
func GetObjectListFromClickHouse(conn driver.Conn, query string) (names, statements []string, err error) {
	// Fetch data from any of specified services
	log.Logger.Infof("Run query: %+v", query)

	// Some data available, let's fetch it
	var rows driver.Rows
	if rows, err = conn.Query(context.Background(), query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name, statement string
		if err = rows.Scan(&name, &statement); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		names = append(names, name)
		statements = append(statements, statement)
	}
	return
}

// GetCreateReplicaObjects returns a list of objects that needs to be created on a host in a cluster
func GetCreateReplicaObjects(conn driver.Conn, host, user, password string) (names, statements []string, err error) {
	system_tables := fmt.Sprintf("remote('%s', system, tables, '%s', '%s')", host, user, password)

	//default database is always exists
	sqlDBs := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			database AS name, 
			concat('CREATE DATABASE IF NOT EXISTS "', name, '"') AS create_db_query
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', 'default')
		SETTINGS skip_unavailable_shards = 1`,
		"system.tables", system_tables,
	))
	sqlTables := heredoc.Doc(strings.ReplaceAll(`
		SELECT DISTINCT 
			name, 
			replaceRegexpOne(create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)', 'CREATE \\1 IF NOT EXISTS')
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') 
        AND create_table_query != '' AND name NOT LIKE '.inner%'
		ORDER BY if(engine='Distributed', 1, 0), if(match(create_table_query, 'CREATE (MATERIALIZED )?VIEW'), 1, 0), name
		SETTINGS skip_unavailable_shards = 1`,
		"system.tables",
		system_tables,
	))

	names1, statements1, err := GetObjectListFromClickHouse(conn, sqlDBs)
	if err != nil {
		return
	}
	names2, statements2, err := GetObjectListFromClickHouse(conn, sqlTables)
	if err != nil {
		return
	}
	names = append(names1, names2...)
	statements = append(statements1, statements2...)
	return
}

type LogicSchema struct {
	SqlType    string
	Statements []string
}

func GetLogicSchema(conn driver.Conn, logicName, clusterName string, replica bool, database, tableName string) ([]LogicSchema, error) {
	var engine, replacingengine string
	var expr *regexp.Regexp
	if replica {
		engine = "ReplicatedMergeTree('/clickhouse/tables/{cluster}/{{.database}}/{{.localtbl}}/{shard}', '{replica}')"
		replacingengine = "ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{{.database}}/{{.localtbl}}/{shard}', '{replica}')"
		expr = regexp.MustCompile("((Replicated)?(Replacing)?MergeTree(\\(.*'{replica}'\\))?)")
	} else {
		engine = "MergeTree()"
		replacingengine = "ReplacingMergeTree()"
		expr = regexp.MustCompile("(Replicated(Replacing)?MergeTree\\(.*'{replica}'\\))")
	}
	tbNameFilter := ""
	if database != "" {
		tbNameFilter += fmt.Sprintf(" AND (t2.database = '%s')", database)
	}
	if tableName != "" {
		tbNameFilter += fmt.Sprintf(" AND (t2.localtbl = '%s')", tableName)
	}
	query := fmt.Sprintf(`SELECT
    t1.database AS db,
    t2.logictbl AS logictbl,
    t2.localtbl AS localtbl,
    replaceRegexpOne(t1.create_table_query, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)\\s+\\w+.\\w+', 'CREATE \\1 IF NOT EXISTS {{.database}}.{{.localtbl}} ON CLUSTER {{.clusterName}}') AS localsql,
    replaceRegexpOne(t2.logicsql, 'CREATE (TABLE|VIEW|MATERIALIZED VIEW)\\s+\\w+.\\w+', 'CREATE \\1 IF NOT EXISTS {{.database}}.{{.logictbl}} ON CLUSTER {{.clusterName}}') AS logicsql
FROM system.tables AS t1
INNER JOIN 
(
    SELECT DISTINCT
        database,
        name AS logictbl,
        (extractAllGroups(create_table_query, '(Distributed).*\'(%s)\',\\s+\'(\\w+)\',\\s+\'(\\w+)\'(.*)\\)')[1])[4] AS localtbl,
        create_table_query AS logicsql
    FROM system.tables
    WHERE (engine = 'Distributed') AND match(create_table_query, 'Distributed.*\'%s\'')
) AS t2 ON (t1.database = t2.database) AND (t1.name = t2.localtbl)%s`, logicName, logicName, tbNameFilter)
	log.Logger.Debugf("query:%s", query)
	rows, err := conn.Query(context.Background(), query)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}

	defer rows.Close()
	var databases, dbsqls, localsqls, distsqls, logicsqls []string
	for rows.Next() {
		var database, logictbl, localtbl, localsql, logicsql string
		if err = rows.Scan(&database, &logictbl, &localtbl, &localsql, &logicsql); err != nil {
			return nil, err
		}
		if database != "default" {
			if !common.ArraySearch(database, databases) {
				dbsql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` ON CLUSTER `%s`", database, clusterName)
				dbsqls = append(dbsqls, dbsql)
			}
		}
		databases = append(databases, database)

		if strings.Contains(localsql, "ReplacingMergeTree") {
			localsql = expr.ReplaceAllString(localsql, replacingengine)
		} else {
			localsql = expr.ReplaceAllString(localsql, engine)
		}
		replaceTmpl := map[string]interface{}{
			"database":    database,
			"localtbl":    localtbl,
			"clusterName": clusterName,
			"logictbl":    logictbl,
		}
		if err = common.ReplaceTemplateString(&localsql, replaceTmpl); err != nil {
			return nil, err
		}
		localsqls = append(localsqls, localsql)

		distsql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.dist_%s ON CLUSTER %s AS %s.%s ENGINE = Distributed(%s, %s, %s, rand())`,
			database, localtbl, clusterName, database, localtbl, clusterName, database, localtbl)
		distsqls = append(distsqls, distsql)

		if err = common.ReplaceTemplateString(&logicsql, replaceTmpl); err != nil {
			return nil, err
		}
		logicsqls = append(logicsqls, logicsql)
	}
	statementsqls := []LogicSchema{
		{
			SqlType:    "dbsql",
			Statements: dbsqls,
		},
		{
			SqlType:    "localsql",
			Statements: localsqls,
		},
		{
			SqlType:    "distsql",
			Statements: distsqls,
		},
		{
			SqlType:    "logicsql",
			Statements: logicsqls,
		},
	}
	return statementsqls, nil
}
