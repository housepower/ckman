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
	DtBegin    string
	DtEnd      string
	PartitionBy  string
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chHosts        []string
	chTables       []string
	chConns        map[string]*sql.DB
	cntErrors      int32
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:    false,
		ChPort:     9000,
		ChDatabase: "default",
		DtBegin:    "1970-01-01",
		DtEnd:      "1970-01-01",
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChDatabase, "ch-database")
	common.EnvStringVar(&cmdOps.ChTables, "ch-tables")
	common.EnvStringVar(&cmdOps.DtBegin, "dt-begin")
	common.EnvStringVar(&cmdOps.DtEnd, "dt-end")

	// 3. Replace options with the corresponding CLI parameter if present.
	flag.BoolVar(&cmdOps.ShowVer, "v", cmdOps.ShowVer, "show build version and quit")
	flag.StringVar(&cmdOps.ChHosts, "ch-hosts", cmdOps.ChHosts, "a list of comma-separated clickhouse host ip address, one host from each shard")
	flag.IntVar(&cmdOps.ChPort, "ch-port", cmdOps.ChPort, "clickhouse tcp listen port")
	flag.StringVar(&cmdOps.ChUser, "ch-user", cmdOps.ChUser, "clickhouse user")
	flag.StringVar(&cmdOps.ChPassword, "ch-password", cmdOps.ChPassword, "clickhouse password")
	flag.StringVar(&cmdOps.ChDatabase, "ch-database", cmdOps.ChDatabase, "clickhouse database")
	flag.StringVar(&cmdOps.ChTables, "ch-tables", cmdOps.ChTables, "a list of comma-separated table")
	flag.StringVar(&cmdOps.DtBegin, "dt-begin", cmdOps.DtBegin, "date begin(inclusive) in ISO8601 format, for example 1970-01-01")
	flag.StringVar(&cmdOps.DtEnd, "dt-end", cmdOps.DtEnd, "date end(exclusive) in ISO8601 format")
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

func convToChDate(ts time.Time) string {
	return ts.Format("2006-01-02")
}

// purgeTable purges specified time range
func purgeTable(table string) (err error) {
	var dateExpr []string
	// ensure the table is partitioned by a Date/DateTime column
	for _, host := range chHosts {
		db := chConns[host]
		var rows *sql.Rows
		query := fmt.Sprintf("SELECT count(), max(max_date)!='1970-01-01', max(toDate(max_time))!='1970-01-01' FROM system.parts WHERE database='%s' AND table='%s'", cmdOps.ChDatabase, table);
		log.Infof("host %s: query: %s", host, query)
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		rows.Next()
		var i1, i2, i3 int
		if err = rows.Scan(&i1, &i2, &i3); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if i1==0 {
			continue
		} else if i2==0 && i3==0 {
			err = errors.Errorf("table %s is not partitioned by a Date/DateTime column", table)
			return
		} else if i2==1 {
			dateExpr = []string{"min_date", "max_date"}
		} else {
			dateExpr = []string{"toDate(min_time)", "toDate(max_time)"}
		}
		break
	}
	if len(dateExpr)!=2 {
		log.Infof("table %s doesn't exist, or is empty", table)
		return
	}

	// ensure no partition runs across the time range boundary
	for _, host := range chHosts {
		db := chConns[host]
		var rows *sql.Rows
		query := fmt.Sprintf("SELECT partition FROM (SELECT partition, countIf(%s>='%s' AND %s<'%s') AS c1, countIf(%s<'%s' OR %s>='%s') AS c2 FROM system.parts WHERE database='%s' AND table='%s' GROUP BY partition HAVING c1!=0 AND c2!=0)", dateExpr[0], cmdOps.DtBegin, dateExpr[1], cmdOps.DtEnd, dateExpr[0], cmdOps.DtBegin, dateExpr[1], cmdOps.DtEnd, cmdOps.ChDatabase, table);
		log.Infof("host %s: query: %s", host, query)
		if rows, err = db.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		for rows.Next() {
			var patt string
			if err = rows.Scan(&patt); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			err = errors.Errorf("table %s partition %s runs across the time range boundary", table, patt)
			return
		}
	}

	// purge partitions
	for _, host := range chHosts {
		db := chConns[host]
		var rows *sql.Rows
		query := fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE database='%s' AND table='%s' AND %s>='%s' AND %s<'%s' ORDER BY partition;", cmdOps.ChDatabase, table, dateExpr[0], cmdOps.DtBegin, dateExpr[1], cmdOps.DtEnd)
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
	}
	return
}


func purge() (err error) {
	for _, table := range chTables {
		if err = purgeTable(table); err != nil {
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

	if err = purge(); err != nil {
		log.Fatalf("got error %+v", err)
	}
	log.Infof("purge done")

	return
}
