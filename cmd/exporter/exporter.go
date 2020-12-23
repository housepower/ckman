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
	"gitlab.eoitek.net/EOI/ckman/common"
)

// export data of given time range to HDFS

type CmdOptions struct {
	ShowVer     bool
	ChHosts     string
	ChPort      int
	ChUser      string
	ChPassword  string
	ChTables    string
	TsBegin     string
	TsEnd       string
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
	chTables       map[string]string
	tsBegin        time.Time
	tsEnd          time.Time
	hdfsDir        string
	tryIntervals   = []string{"1 year", "1 month", "1 week", "1 day", "4 hour", "1 hour"}
	ckConns        map[string]*sql.DB
	globalPool     *common.WorkerPool
	wg             sync.WaitGroup
	cntErrors      int32
	estSize        uint64
)

func initCmdOptions() {
	// 1. Set options to default value.
	cmdOps = CmdOptions{
		ShowVer:     false,
		ChPort:      9000,
		TsBegin:     "1970-01-01T00:00:00Z",
		MaxFileSize: 1e10, //10GB, Parquet files need be small, nearly equal size
		Parallelism: 4,    // >=4 is able to exhaust HDFS cluster(3 DataNodes with HDDs) write bandwidth 150MB/s
	}

	// 2. Replace options with the corresponding env variable if present.
	common.EnvBoolVar(&cmdOps.ShowVer, "v")
	common.EnvStringVar(&cmdOps.ChHosts, "ch-hosts")
	common.EnvIntVar(&cmdOps.ChPort, "ch-port")
	common.EnvStringVar(&cmdOps.ChUser, "ch-user")
	common.EnvStringVar(&cmdOps.ChPassword, "ch-password")
	common.EnvStringVar(&cmdOps.ChTables, "ch-tables")
	common.EnvStringVar(&cmdOps.TsBegin, "ts-begin")
	common.EnvStringVar(&cmdOps.TsEnd, "ts-end")
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
	flag.StringVar(&cmdOps.ChTables, "ch-tables", cmdOps.ChTables, "a list of comma-separated table:timestamp_column")
	flag.StringVar(&cmdOps.TsBegin, "ts-begin", cmdOps.TsBegin, "timestamp begin in ISO8601 format, for example 1970-01-01T00:00:00Z")
	flag.StringVar(&cmdOps.TsEnd, "ts-end", cmdOps.TsEnd, "timestamp end in ISO8601 format")
	flag.IntVar(&cmdOps.MaxFileSize, "max-file-size", cmdOps.MaxFileSize, "max parquet file size")

	flag.StringVar(&cmdOps.HdfsAddr, "hdfs-addr", cmdOps.HdfsAddr, "hdfs_name_node_ip:port")
	flag.StringVar(&cmdOps.HdfsUser, "hdfs-user", cmdOps.HdfsUser, "hdfs user")
	flag.StringVar(&cmdOps.HdfsDir, "hdfs-dir", cmdOps.HdfsDir, "hdfs dir, under which a subdirectory will be created according to the given timestamp range")
	flag.IntVar(&cmdOps.Parallelism, "parallelism", cmdOps.Parallelism, "how many time slots are allowed to export to HDFS at the same time")
	flag.Parse()
}

func initConns() (err error) {
	ckConns = make(map[string]*sql.DB)
	for _, host := range chHosts {
		if len(host) == 0 {
			continue
		}
		var db *sql.DB
		dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
			host, cmdOps.ChPort, "default", cmdOps.ChUser, cmdOps.ChPassword)
		if db, err = sql.Open("clickhouse", dsn); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		ckConns[host] = db
		log.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

func selectUint64(host, query string) (res uint64, err error) {
	db := ckConns[host]
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

func convToChTs(ts time.Time) string {
	return ts.Format("2006-01-02 15:04:05")
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
	if compressed, err = selectUint64(host, fmt.Sprintf("SELECT sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='default' AND table='%s' AND active=1", table)); err != nil {
		return
	}
	sizePerRow = float64(compressed) / float64(rowsCnt)

	maxRowsCnt := uint64(float64(cmdOps.MaxFileSize) / sizePerRow)
	slots = make([]time.Time, 0)
	var slot time.Time
	db := ckConns[host]

	tsColumn := chTables[table]
	var totalRowsCnt uint64
	if totalRowsCnt, err = selectUint64(host, fmt.Sprintf("SELECT count() FROM %s WHERE `%s`>='%s' AND `%s`<'%s'", table, tsColumn, convToChTs(tsBegin), tsColumn, convToChTs(tsEnd))); err != nil {
		return
	}
	tblEstSize := totalRowsCnt * uint64(sizePerRow)
	log.Infof("host %s: totol rows to export: %d, estimated size (in bytes): %d", host, totalRowsCnt, tblEstSize)
	atomic.AddUint64(&estSize, tblEstSize)

	sqlTmpl3 := "SELECT toStartOfInterval(`%s`, INTERVAL %s) AS slot, count() FROM %s WHERE `%s`>='%s' AND `%s`<'%s' GROUP BY slot ORDER BY slot"
	for i, interval := range tryIntervals {
		slots = slots[:0]
		var rows1 *sql.Rows
		query1 := fmt.Sprintf(sqlTmpl3, tsColumn, interval, table, tsColumn, convToChTs(tsBegin), tsColumn, convToChTs(tsEnd))
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
			if slotEnd, err = time.Parse(time.RFC3339, cmdOps.TsEnd); err != nil {
				panic(fmt.Sprintf("BUG: failed to parse %s, layout %s", cmdOps.TsEnd, time.RFC3339))
			}
		}
		exportSlot(host, table, i, slotBeg, slotEnd)
	}
	return
}

func exportSlot(host, table string, seq int, slotBeg, slotEnd time.Time) {
	tsColumn := chTables[table]
	wg.Add(1)
	globalPool.Submit(func() {
		defer wg.Done()
		hdfsTbl := "hdfs_" + table + "_" + slotBeg.Format("20060102150405")
		fp := filepath.Join(cmdOps.HdfsDir, table+"_"+host+"_"+slotBeg.Format("20060102150405")+".parquet")
		queries := []string{
			fmt.Sprintf("DROP TABLE IF EXISTS %s", hdfsTbl),
			fmt.Sprintf("CREATE TABLE %s AS %s ENGINE=HDFS('hdfs://%s%s', 'Parquet')", hdfsTbl, table, cmdOps.HdfsAddr, fp),
			fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE `%s`>='%s' AND `%s`<'%s'", hdfsTbl, table, tsColumn, convToChTs(slotBeg), tsColumn, convToChTs(slotEnd)),
			fmt.Sprintf("DROP TABLE %s", hdfsTbl),
		}
		db := ckConns[host]
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
	chTables = make(map[string]string)
	for _, tableSpec := range strings.Split(cmdOps.ChTables, ",") {
		tokens := strings.SplitN(tableSpec, ":", 2)
		if len(tokens) != 2 {
			log.Fatalf("failed to parse table spec %s", tableSpec)
		}
		chTables[tokens[0]] = tokens[1]
	}

	if tsBegin, err = time.Parse(time.RFC3339, cmdOps.TsBegin); err != nil {
		log.Fatalf("failed to parse timestamp %s, layout %s", cmdOps.TsBegin, time.RFC3339)
	}
	if tsEnd, err = time.Parse(time.RFC3339, cmdOps.TsEnd); err != nil {
		log.Fatalf("failed to parse timestamp %s, layout %s", cmdOps.TsEnd, time.RFC3339)
	}

	globalPool = common.NewWorkerPool(cmdOps.Parallelism, len(ckConns))
	dir := tsBegin.Format("20060102150405") + "_" + tsEnd.Format("20060102150405")
	hdfsDir = filepath.Join(cmdOps.HdfsDir, dir)
	var hc *hdfs.Client
	if hc, err = hdfs.New(cmdOps.HdfsAddr); err != nil {
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

	t0 := time.Now()
	for i := 0; i < len(chHosts); i++ {
		host := chHosts[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var slots []time.Time
			for table := range chTables {
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
