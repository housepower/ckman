package clickhouse

import (
	"database/sql"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/colinmarc/hdfs/v2"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/pkg/errors"
)

const (
	HdfsUserDefault    string = "root"
	MaxFileSizeDefault int    = 1e10
	CKDatabaseDefault  string = "default"
	ParallelismDefault int    = 4

	DateLayout     string = "2006-01-02"
	DateTimeLayout string = "2006-01-02 15:04:05"
	SlotTimeFormat string = "20060102150405"
)

type ArchiveHDFS struct {
	Hosts       []string
	Port        int
	User        string
	Password    string
	Database    string
	Tables      []string
	Begin       string
	End         string
	MaxFileSize int
	HdfsAddr    string
	HdfsUser    string
	HdfsDir     string
	Parallelism int
}

var (
	tryDateIntervals     = []string{"1 year", "1 month", "1 week", "1 day"}
	tryDateTimeIntervals = []string{"1 year", "1 month", "1 week", "1 day", "4 hour", "1 hour"}
	pattInfo             map[string][]string // table -> Date/DateTime column, type
	wg                   sync.WaitGroup
	cntErrors            int32
	estSize              uint64
	hdfsDir              []string
)

func (a *ArchiveHDFS) FillArchiveDefault() {
	if a.HdfsUser == "" {
		a.HdfsUser = HdfsUserDefault
	}
	if a.MaxFileSize == 0 {
		a.MaxFileSize = MaxFileSizeDefault
	}
	if a.Database == "" {
		a.Database = CKDatabaseDefault
	}
	if a.Parallelism == 0 {
		a.Parallelism = ParallelismDefault
	}
}

func (a *ArchiveHDFS) InitConns() (err error) {
	for _, host := range a.Hosts {
		if len(host) == 0 {
			continue
		}
		_, err = common.ConnectClickHouse(host, a.Port, a.Database, a.User, a.Password)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

func (a *ArchiveHDFS) GetSortingInfo() (err error) {
	pattInfo = make(map[string][]string)
	for _, table := range a.Tables {
		var name, typ string
		for _, host := range a.Hosts {
			db := common.GetConnection(host)
			if db == nil {
				return fmt.Errorf("can't get connection: %s", host)
			}
			var rows *sql.Rows
			query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database='%s' AND table='%s' AND is_in_partition_key=1 AND type IN ('Date', 'DateTime')", a.Database, table)
			log.Logger.Infof("host %s: query: %s", host, query)
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

// https://www.slideshare.net/databricks/the-parquet-format-and-performance-optimization-opportunities
// P22 sorted data helps to predicate pushdown
// P25 avoid many small files
// P27 avoid few huge files - 1GB?
func (a *ArchiveHDFS) GetSlots(host, table string) (slots []time.Time, err error) {
	var sizePerRow float64
	var rowsCnt uint64
	var compressed uint64
	// get size-per-row
	if rowsCnt, err = a.SelectUint64(host, fmt.Sprintf("SELECT count() FROM %s", table)); err != nil {
		return
	}
	if rowsCnt == 0 {
		return
	}
	if compressed, err = a.SelectUint64(host, fmt.Sprintf("SELECT sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1", a.Database, table)); err != nil {
		return
	}
	sizePerRow = float64(compressed) / float64(rowsCnt)

	maxRowsCnt := uint64(float64(a.MaxFileSize) / sizePerRow)
	slots = make([]time.Time, 0)
	var slot time.Time
	db := common.GetConnection(host)
	if db == nil {
		log.Logger.Errorf("can't get connection:%s", host)
		return
	}

	colName := pattInfo[table][0]
	colType := pattInfo[table][1]
	var totalRowsCnt uint64
	if totalRowsCnt, err = a.SelectUint64(host, fmt.Sprintf("SELECT count() FROM %s WHERE `%s`>=%s AND `%s`<%s", table, colName, formatDate(a.Begin, colType), colName, formatDate(a.End, colType))); err != nil {
		return
	}
	tblEstSize := totalRowsCnt * uint64(sizePerRow)
	log.Logger.Infof("host %s: totol rows to export: %d, estimated size (in bytes): %d", host, totalRowsCnt, tblEstSize)
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
		query1 := fmt.Sprintf(sqlTmpl3, colName, interval, table, colName, formatDate(a.Begin, colType), colName, formatDate(a.End, colType))
		log.Logger.Infof("host %s: query: %s", host, query1)
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

func (a *ArchiveHDFS) Export(host, table string, slots []time.Time) {
	var err error
	for i := 0; i < len(slots); i++ {
		var slotBeg, slotEnd time.Time
		slotBeg = slots[i]
		if i != len(slots)-1 {
			slotEnd = slots[i+1]
		} else {
			if slotEnd, err = time.Parse(DateLayout, a.End); err != nil {
				panic(fmt.Sprintf("BUG: failed to parse %s, layout %s", a.End, DateLayout))
			}
		}
		a.ExportSlot(host, table, i, slotBeg, slotEnd)
	}
}

func (a *ArchiveHDFS) ExportSlot(host, table string, seq int, slotBeg, slotEnd time.Time) {
	colName := pattInfo[table][0]
	colType := pattInfo[table][1]
	var wg sync.WaitGroup
	wg.Add(1)
	_ = common.Pool.Submit(func() {
		defer wg.Done()
		hdfsTbl := "hdfs_" + table + "_" + slotBeg.Format(SlotTimeFormat)
		for _, dir := range hdfsDir {
			fp := filepath.Join(dir, host+"_"+slotBeg.Format(SlotTimeFormat)+".parquet")
			queries := []string{
				fmt.Sprintf("DROP TABLE IF EXISTS %s", hdfsTbl),
				fmt.Sprintf("CREATE TABLE %s AS %s ENGINE=HDFS('hdfs://%s%s', 'Parquet')", hdfsTbl, table, a.HdfsAddr, fp),
				fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE `%s`>=%s AND `%s`<%s", hdfsTbl, table, colName, formatTimestamp(slotBeg, colType), colName, formatTimestamp(slotEnd, colType)),
				fmt.Sprintf("DROP TABLE %s", hdfsTbl),
			}
			db := common.GetConnection(host)
			if db == nil {
				log.Logger.Errorf("can't get connection: %s", host)
				return
			}
			for _, query := range queries {
				if atomic.LoadInt32(&cntErrors) != 0 {
					return
				}
				log.Logger.Infof("host %s, table %s, slot %d, query: %s", host, table, seq, query)
				if _, err := db.Exec(query); err != nil {
					log.Logger.Errorf("host %s: got error %+v", host, err)
					atomic.AddInt32(&cntErrors, 1)
					return
				}
			}
			log.Logger.Infof("host %s, table %s, slot %d, export done", host, table, seq)
		}
	})
	wg.Wait()
}

func (a *ArchiveHDFS) ClearHDFS() error {
	var err error
	ops := hdfs.ClientOptions{
		Addresses: []string{a.HdfsAddr},
		User:      a.HdfsUser,
	}
	var hc *hdfs.Client
	if hc, err = hdfs.NewClient(ops); err != nil {
		err = errors.Wrapf(err, "")
		log.Logger.Errorf("got error %+v", err)
		return err
	}

	slotBeg, _ := time.Parse(DateLayout, a.Begin)
	slotEnd, _ := time.Parse(DateLayout, a.End)
	for _, table := range a.Tables {
		dir := path.Join(a.HdfsDir, table)
		hdfsDir = append(hdfsDir, dir)
		_ = hc.Mkdir(dir, 0777)
		var fileList []os.FileInfo
		if fileList, err = hc.ReadDir(dir); err != nil {
			err = errors.Wrapf(err, "")
			log.Logger.Errorf("got error %+v", err)
			return err
		}
		for _, file := range fileList {
			name := file.Name()
			if !strings.Contains(name, "parquet") {
				continue
			}
			slot := strings.Split(strings.TrimSuffix(name, ".parquet"), "_")[1]
			slotTime, err := time.Parse(SlotTimeFormat, slot)
			if err != nil {
				log.Logger.Errorf("parse time error: %+v", err)
				return err
			}
			if (slotTime.After(slotBeg) || slotTime.Equal(slotBeg)) && slotTime.Before(slotEnd) {
				filePath := path.Join(dir, name)
				if err = hc.Remove(filePath); err != nil {
					err = errors.Wrapf(err, "")
					log.Logger.Errorf("got error %+v", err)
					return err
				}
			}
		}
		log.Logger.Infof("cleared hdfs directory %s", dir)
	}

	return nil
}

func (a *ArchiveHDFS) ExportToHDFS() (err error) {
	t0 := time.Now()
	for i := 0; i < len(a.Hosts); i++ {
		host := a.Hosts[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var slots []time.Time
			for _, table := range a.Tables {
				if slots, err = a.GetSlots(host, table); err != nil {
					log.Logger.Errorf("host %s: got error %+v", host, err)
					return
				}
				a.Export(host, table, slots)
			}
		}()
	}

	wg.Wait()
	if atomic.LoadInt32(&cntErrors) != 0 {
		log.Logger.Errorf("export failed")
		return errors.Wrap(err, "")
	} else {
		du := uint64(time.Since(t0).Seconds())
		size := atomic.LoadUint64(&estSize)
		msg := fmt.Sprintf("exported %d bytes in %d seconds", size, du)
		if du != 0 {
			msg += fmt.Sprintf(", %d bytes/s", size/du)
		}
		log.Logger.Infof(msg)
	}
	return nil
}

func formatDate(dt, typ string) string {
	if typ == "DateTime" {
		return fmt.Sprintf("parseDateTimeBestEffort('%s')", dt)
	}
	return fmt.Sprintf("'%s'", dt)
}

func formatTimestamp(ts time.Time, typ string) string {
	if typ == "DateTime" {
		return fmt.Sprintf("'%s'", ts.Format(DateTimeLayout))
	}
	return fmt.Sprintf("'%s'", ts.Format(DateLayout))
}

func (a *ArchiveHDFS) SelectUint64(host, query string) (res uint64, err error) {
	db := common.GetConnection(host)
	if db == nil {
		log.Logger.Errorf("can't get connection:%s", host)
		return
	}
	var rows *sql.Rows
	log.Logger.Infof("host %s: query: %s", host, query)
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
