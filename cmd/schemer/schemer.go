package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/MakeNowJust/heredoc"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Create objects on a new ClickHouse instance.
// Refers to https://github.com/Altinity/clickhouse-operator/blob/master/pkg/model/schemer.go

var (
	srcHost  = "192.168.101.106"
	dstHost  = "192.168.101.108"
	port     = 9000
	username = "eoi"
	password = "123456"
)

// getObjectListFromClickHouse
func getObjectListFromClickHouse(db *sql.DB, query string) (names, statements []string, err error) {
	// Fetch data from any of specified services
	log.Infof("Run query: %+v", query)

	// Some data available, let's fetch it
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	for rows.Next() {
		var name, statement string
		if err := rows.Scan(&name, &statement); err == nil {
			names = append(names, name)
			statements = append(statements, statement)
		} else {
			err = errors.Wrapf(err, "")
			log.Errorf("UNABLE to scan row err: %+v", err)
		}
	}
	return
}

// getCreateReplicaObjects returns a list of objects that needs to be created on a host in a cluster
func getCreateReplicaObjects(db *sql.DB) (names, statements []string, err error) {

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
		err = errors.Wrapf(err, "")
		return
	}
	names2, statements2, err := getObjectListFromClickHouse(db, sqlTables)
	if err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	names = append(names1, names2...)
	statements = append(statements1, statements2...)
	return
}

func main() {
	var names, statements []string
	var err error
	var db *sql.DB
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		dstHost, port, "default", username, password)
	if db, err = sql.Open("clickhouse", dsn); err != nil {
		err = errors.Wrapf(err, "")
		return
	}

	if names, statements, err = getCreateReplicaObjects(db); err != nil {
		log.Fatalf("got error %+v", err)
	}
	log.Infof("names: %+v", names)
	log.Infof("statements: %+v", statements)
	num := len(names)
	for i := 0; i < num; i++ {
		log.Infof("executing %s", statements[i])
		if _, err = db.Exec(statements[i]); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
}
