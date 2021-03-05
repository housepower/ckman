package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/colinmarc/hdfs/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/housepower/ckman/common"
)

// export data of given time range to HDFS

type CmdOptions struct {
	ShowVer     bool
	ChHosts     string
	ChPort      int
	ChUser      string
	ChPassword  string
	ChDatabase  string
	ChTables    string
	DtBegin     string
	DtEnd       string
	MaxFileSize int
	HdfsAddr    string
	HdfsUser    string
	HdfsDir     string
	Parallelism int
}

var (
	cmdOps         CmdOptions
	GitCommitHash  string
	BuildTimeStamp string
	chHosts        []string
	chTables       []string
	hdfsDir        string

	tryDateIntervals     = []string{"1 year", "1 month", "1 week", "1 day"}
	tryDateTimeIntervals = []string{"1 year", "1 month", "1 week", "1 day", "4 hour", "1 hour"}

	chConns    map[string]*sql.DB
	pattInfo   map[string][]string // table -> Date/DateTime column, type
	globalPool *common.WorkerPool
	wg         sync.WaitGroup
	cntErrors  int32
	estSize    uint64
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:     false,
		ChPort:      9000,
		ChDatabase:  "default",
		DtBegin:     "1970-01-01",
		MaxFileSize: 1e10, //10GB, Parquet files need be small, nearly equal size
		HdfsUser:    "root",
		HdfsDir:     "",
		Parallelism: 4, // >=4 is capable of saturating HDFS cluster(3 DataNodes with HDDs) write bandwidth 150MB/s
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
	common.EnvIntVar(&cmdOps.MaxFileSize, "max-file-size")
	common.EnvStringVar(&cmdOps.HdfsAddr, "hdfs-addr")
	common.EnvStringVar(&cmdOps.HdfsUser, "hdfs-user")
	common.EnvStringVar(&cmdOps.HdfsDir, "hdfs-dir")

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
	flag.IntVar(&cmdOps.MaxFileSize, "max-file-size", cmdOps.MaxFileSize, "max parquet file size")

	flag.StringVar(&cmdOps.HdfsAddr, "hdfs-addr", cmdOps.HdfsAddr, "hdfs_name_node_active_ip:port")
	flag.StringVar(&cmdOps.HdfsUser, "hdfs-user", cmdOps.HdfsUser, "hdfs user")
	flag.StringVar(&cmdOps.HdfsDir, "hdfs-dir", cmdOps.HdfsDir, "hdfs dir, under which a subdirectory will be created according to the given timestamp range, defaults to /user/<hdfs-user>")
	flag.IntVar(&cmdOps.Parallelism, "parallelism", cmdOps.Parallelism, "how many time slots are allowed to export to HDFS at the same time")
	flag.Parse()

	// 4. Normalization
	if cmdOps.HdfsDir == "" || cmdOps.HdfsDir == "." {
		cmdOps.HdfsDir = fmt.Sprintf("/user/%s", cmdOps.HdfsUser)
	}
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

func selectUint64(host, query string) (res uint64, err error) {
	db := chConns[host]
	var rows *sql.Rows
	log.Infof("host %s: query: %s", host, query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	defer rows.Close()
	rows.Next()
	if err = rows.Scan(&res); err != nil {
		err = errors.Wrapf(err, "")
		return
	}
	return
}

func getSortingInfo() (err error) {
	pattInfo = make(map[string][]string)
	for _, table := range chTables {
		var name, typ string
		for _, host := range chHosts {
			db := chConns[host]
			var rows *sql.Rows
			query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database='%s' AND table='%s' AND is_in_partition_key=1 AND type IN ('Date', 'DateTime')", cmdOps.ChDatabase, table)
			log.Infof("host %s: query: %s", host, query)
			if rows, err = db.Query(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			defer rows.Close()
			for rows.Next() {
				if name != "" {
					err = errors.Errorf("table %s has multiple Date/DateTime columns in sorting key", table)
					return
				}
				if err = rows.Scan(&name, &typ); err != nil {
					err = errors.Wrapf(err, "")
					return
				}
				pattInfo[table] = []string{name, typ}
			}
			if name != "" {
				break
			}
		}
	}
	return
}

func formatDate(dt, typ string) string {
	if typ == "DateTime" {
		return fmt.Sprintf("toDateTime('%s')", dt)
	}
	return fmt.Sprintf("'%s'", dt)
}

func formatTimestamp(ts time.Time, typ string) string {
	if typ == "DateTime" {
		return fmt.Sprintf("'%s'", ts.Format("2006-01-02"))
	}
	return fmt.Sprintf("'%s'", ts.Format("2006-01-02 15:04:05"))
}

// https://www.slideshare.net/databricks/the-parquet-format-and-performance-optimization-opportunities
// P22 sorted data helps to predicate pushdown
// P25 avoid many small files
// P27 avoid few huge files - 1GB?
func getSlots(host, table string) (slots []time.Time, err error) {
	var sizePerRow float64
	var rowsCnt uint64
	var compressed uint64
	// get size-per-row
	if rowsCnt, err = selectUint64(host, fmt.Sprintf("SELECT count() FROM %s", table)); err != nil {
		return
	}
	if rowsCnt == 0 {
		return
	}
	if compressed, err = selectUint64(host, fmt.Sprintf("SELECT sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1", cmdOps.ChDatabase, table)); err != nil {
		return
	}
	sizePerRow = float64(compressed) / float64(rowsCnt)

	maxRowsCnt := uint64(float64(cmdOps.MaxFileSize) / sizePerRow)
	slots = make([]time.Time, 0)
	var slot time.Time
	db := chConns[host]

	colName := pattInfo[table][0]
	colType := pattInfo[table][1]
	var totalRowsCnt uint64
	if totalRowsCnt, err = selectUint64(host, fmt.Sprintf("SELECT count() FROM %s WHERE `%s`>=%s AND `%s`<%s", table, colName, formatDate(cmdOps.DtBegin, colType), colName, formatDate(cmdOps.DtEnd, colType))); err != nil {
		return
	}
	tblEstSize := totalRowsCnt * uint64(sizePerRow)
	log.Infof("host %s: totol rows to export: %d, estimated size (in bytes): %d", host, totalRowsCnt, tblEstSize)
	atomic.AddUint64(&estSize, tblEstSize)

	sqlTmpl3 := "SELECT toStartOfInterval(`%s`, INTERVAL %s) AS slot, count() FROM %s WHERE `%s`>=%s AND `%s`<%s GROUP BY slot ORDER BY slot"
	var tryIntervals []string
	if colType == "Date" {
		tryIntervals = tryDateIntervals
	} else {
		tryIntervals = tryDateTimeIntervals
	}
	for i, interval := range tryIntervals {
		slots = slots[:0]
		var rows1 *sql.Rows
		query1 := fmt.Sprintf(sqlTmpl3, colName, interval, table, formatDate(cmdOps.DtBegin, colType), colName, formatDate(cmdOps.DtEnd, colType))
		log.Infof("host %s: query: %s", host, query1)
		if rows1, err = db.Query(query1); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows1.Close()
		var tooBigSlot bool
	LOOP_RS:
		for rows1.Next() {
			if err = rows1.Scan(&slot, &rowsCnt); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
			if rowsCnt > maxRowsCnt && i != len(tryIntervals)-1 {
				tooBigSlot = true
				break LOOP_RS
			}
			slots = append(slots, slot)
		}
		if !tooBigSlot {
			break
		}
	}
	return
}

func export(host, table string, slots []time.Time) {
	var err error
	for i := 0; i < len(slots); i++ {
		var slotBeg, slotEnd time.Time
		slotBeg = slots[i]
		if i != len(slots)-1 {
			slotEnd = slots[i+1]
		} else {
			if slotEnd, err = time.Parse(time.RFC3339, cmdOps.DtEnd); err != nil {
				panic(fmt.Sprintf("BUG: failed to parse %s, layout %s", cmdOps.DtEnd, time.RFC3339))
			}
		}
		exportSlot(host, table, i, slotBeg, slotEnd)
	}
	return
}

func exportSlot(host, table string, seq int, slotBeg, slotEnd time.Time) {
	colName := pattInfo[table][0]
	colType := pattInfo[table][1]
	wg.Add(1)
	globalPool.Submit(func() {
		defer wg.Done()
		hdfsTbl := "hdfs_" + table + "_" + slotBeg.Format("20060102150405")
		fp := filepath.Join(cmdOps.HdfsDir, table+"_"+host+"_"+slotBeg.Format("20060102150405")+".parquet")
		queries := []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s", hdfsTbl),
			fmt.Sprintf("CREATE TABLE %s AS %s ENGINE=HDFS('hdfs://%s%s', 'Parquet')", hdfsTbl, table, cmdOps.HdfsAddr, fp),
			fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE `%s`>=%s AND `%s`<%s", hdfsTbl, table, colName, formatTimestamp(slotBeg, colType), colName, formatTimestamp(slotEnd, colType)),
			fmt.Sprintf("DROP TABLE %s", hdfsTbl),
		}
		db := chConns[host]
		for _, query := range queries {
			if atomic.LoadInt32(&cntErrors) != 0 {
				return
			}
			log.Infof("host %s, table %s, slot %d, query: %s", host, table, seq, query)
			if _, err := db.Exec(query); err != nil {
				log.Errorf("host %s: got error %+v", host, err)
				atomic.AddInt32(&cntErrors, 1)
				return
			}
		}
		log.Infof("host %s, table %s, slot %d, export done", host, table, seq)
	})
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

	if err = getSortingInfo(); err != nil {
		log.Fatalf("got error %+v", err)
	}

	globalPool = common.NewWorkerPool(cmdOps.Parallelism, len(chConns))
	dir := cmdOps.DtBegin + "_" + cmdOps.DtEnd
	hdfsDir = filepath.Join(cmdOps.HdfsDir, dir)
	ops := hdfs.ClientOptions{
		Addresses: []string{cmdOps.HdfsAddr},
		User:      cmdOps.HdfsUser,
	}
	var hc *hdfs.Client
	if hc, err = hdfs.NewClient(ops); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatalf("got error %+v", err)
	}
	if err = hc.RemoveAll(hdfsDir); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatalf("got error %+v", err)
	}
	if err = hc.MkdirAll(hdfsDir, 0777); err != nil {
		err = errors.Wrapf(err, "")
		log.Fatalf("got error %+v", err)
	}
	log.Infof("cleared hdfs directory %s", hdfsDir)

	t0 := time.Now()
	for i := 0; i < len(chHosts); i++ {
		host := chHosts[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var slots []time.Time
			for _, table := range chTables {
				if slots, err = getSlots(host, table); err != nil {
					log.Fatalf("host %s: got error %+v", host, err)
				}
				export(host, table, slots)
			}
		}()
	}
	wg.Wait()
	if atomic.LoadInt32(&cntErrors) != 0 {
		log.Errorf("export failed")
	} else {
		du := uint64(time.Since(t0).Seconds())
		size := atomic.LoadUint64(&estSize)
		msg := fmt.Sprintf("exported %d bytes in %d seconds", size, du)
		if du != 0 {
			msg += fmt.Sprintf(", %d bytes/s", size/du)
		}
		log.Infof(msg)
	}
	return
}
