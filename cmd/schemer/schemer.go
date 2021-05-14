package main

import (
	"database/sql"
	"flag"
	"fmt"
	"github.com/housepower/ckman/model"
	"os"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/MakeNowJust/heredoc"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
)

// Create objects on a new ClickHouse instance.
// Refers to https://github.com/Altinity/clickhouse-operator/blob/master/pkg/model/schemer.go

type CmdOptions struct {
	ShowVer    bool
	SrcHost    string
	DstHost    string
	ChPort     int
	ChUser     string
	ChPassword string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer: false,
		ChPort:  9000,
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.SrcHost, "src-host")
	common.EnvStringVar(&cmdOps.DstHost, "dst-host")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.SrcHost, "src-host", cmdOps.SrcHost, "clickhouse source host")
	flag.StringVar(&cmdOps.DstHost, "dst-host", cmdOps.DstHost, "clickhouse destination host")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.Parse()
}

// getObjectListFromClickHouse
func getObjectListFromClickHouse(db *sql.DB, query string) (names, statements []string, err error) {
	// Fetch data from any of specified services
	log.Logger.Infof("Run query: %+v", query)

	// Some data available, let's fetch it
	var rows *sql.Rows
	if rows, err = db.Query(query); err != nil {
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

// getCreateReplicaObjects returns a list of objects that needs to be created on a host in a cluster
func getCreateReplicaObjects(db *sql.DB) (names, statements []string, err error) {
	system_tables := fmt.Sprintf("remote('%s', system, tables)", cmdOps.SrcHost)

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
	log.InitLoggerConsole()
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if cmdOps.SrcHost == "" || cmdOps.DstHost == "" {
		log.Logger.Fatalf("need to specify clickhouse source host and dest host")
	} else if cmdOps.ChUser == "" || cmdOps.ChPassword == "" {
		log.Logger.Fatalf("need to specify clickhouse username and password")
	}

	db, err = common.ConnectClickHouse(cmdOps.DstHost, cmdOps.ChPort, model.ClickHouseDefaultDB, cmdOps.ChUser, cmdOps.ChPassword)
	if err != nil {
		return
	}

	if names, statements, err = getCreateReplicaObjects(db); err != nil {
		log.Logger.Fatalf("got error %+v", err)
	}
	log.Logger.Infof("names: %+v", names)
	log.Logger.Infof("statements: %+v", statements)
	num := len(names)
	for i := 0; i < num; i++ {
		log.Logger.Infof("executing %s", statements[i])
		if _, err = db.Exec(statements[i]); err != nil {
			return
		}
	}
}
