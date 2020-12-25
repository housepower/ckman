package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"gitlab.eoitek.net/EOI/ckman/common"
)

// purge data of given time range

type CmdOptions struct {
	ShowVer    bool
	ChHosts    string
	ChPort     int
	ChUser     string
	ChPassword string
	ChDatabase string
	ChTables   string
	TsBegin    string
	TsEnd      string
	PartitionBy  string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chHosts        []string
	chTables       []string
	tsBegin        time.Time
	tsEnd          time.Time
	chConns        map[string]*sql.DB
	cntErrors      int32
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:    false,
		ChPort:     9000,
		ChDatabase: "default",
		TsBegin:    "1970-01-01T00:00:00Z",
		TsEnd:      "1970-01-01T00:00:00Z",
		PartitionBy:  "day",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChDatabase, "ch-database")
	common.EnvStringVar(&cmdOps.ChTables, "ch-tables")
	common.EnvStringVar(&cmdOps.TsBegin, "ts-begin")
	common.EnvStringVar(&cmdOps.TsEnd, "ts-end")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChDatabase, "ch-database", cmdOps.ChDatabase, "clickhouse database")
	flag.StringVar(&cmdOps.ChTables, "ch-tables", cmdOps.ChTables, "a list of comma-separated table")
	flag.StringVar(&cmdOps.TsBegin, "ts-begin", cmdOps.TsBegin, "timestamp begin in ISO8601 format, for example 1970-01-01T00:00:00Z")
	flag.StringVar(&cmdOps.TsEnd, "ts-end", cmdOps.TsEnd, "timestamp end in ISO8601 format")
	flag.StringVar(&cmdOps.PartitionBy, "partition-by", cmdOps.PartitionBy, `"month", or "day"`)
	flag.Parse()
}

func initConns() (err error) {
	chConns = make(map[string]*sql.DB)
	for _, host := range chHosts {
		if len(host) == 0 {
			continue
		}
		var db *sql.DB
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			host, cmdOps.ChPort, cmdOps.ChDatabase, cmdOps.ChUser, cmdOps.ChPassword)
		if db, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		chConns[host] = db
		log.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

func tsIsTruncated(ts time.Time, duration string) bool{
	return (duration=="day" && ts.Hour()==0 && ts.Minute()==0 && ts.Second()==0 && ts.Nanosecond()==0) || (duration=="month" && ts.Day()==1 && ts.Hour()==0 && ts.Minute()==0 && ts.Second()==0 && ts.Nanosecond()==0)
}

func convToChTs(ts time.Time) string {
	return ts.Format("2006-01-02 15:04:05")
}

func purge(tsBegin, tsEnd time.Time) (err error) {
	for _, table := range chTables {
		for _, host := range chHosts {
			if err = purgeTableHost(tsBegin, tsEnd, table, host); err != nil {
				return
			}
		}
	}
	return
}

func purgeTableHost(tsBegin, tsEnd time.Time, table, host string) (err error) {
	db := chConns[host]
	var rows *sql.Rows
	query := fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE database='%s' AND table='%s' AND min_time>='%s' AND max_time<'%s' ORDER BY partition;", cmdOps.ChDatabase, table, convToChTs(tsBegin), convToChTs(tsEnd))
	log.Infof("host %s: query: %s", host, query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var patt string
		if err = rows.Scan(&patt); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		partitions = append(partitions, patt)
	}
	for _, patt := range partitions {
		query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION '%s'", table, patt)
		log.Infof("host %s: query: %s", host, query)
		if _, err = db.Exec(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
	}
	return
}

func main() {
	var err error
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	initCmdOptions()
	if cmdOps.ShowVer {
		fmt.Println("Build Timestamp:", BuildTimeStamp)
		fmt.Println("Git Commit Hash:", GitCommitHash)
		os.Exit(0)
	}
	if len(cmdOps.ChHosts) == 0 {
		log.Fatalf("need to specify clickhouse hosts")
		os.Exit(0)
	}
	if len(cmdOps.ChTables) == 0 {
		log.Fatalf("need to specify clickhouse tables")
	}

	chHosts = strings.Split(strings.TrimSpace(cmdOps.ChHosts), ",")
	if err = initConns(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	chTables = strings.Split(cmdOps.ChTables, ",")

	if tsBegin, err = time.Parse(time.RFC3339, cmdOps.TsBegin); err != nil {
		log.Fatalf("failed to parse timestamp %s, layout %s", cmdOps.TsBegin, time.RFC3339)
	}
	if tsEnd, err = time.Parse(time.RFC3339, cmdOps.TsEnd); err != nil {
		log.Fatalf("failed to parse timestamp %s, layout %s", cmdOps.TsEnd, time.RFC3339)
	}
	if !tsIsTruncated(tsBegin, cmdOps.PartitionBy) || !tsIsTruncated(tsEnd, cmdOps.PartitionBy) {
		log.Fatalf("tsBegin or tsEnd is not truncated to %s", cmdOps.PartitionBy)
		return
	}
	if err = purge(tsBegin, tsEnd); err != nil {
		log.Fatalf("got error %+v", err)
	}
	log.Infof("purge done")

	return
}
