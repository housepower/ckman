package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

const (
	HdfsUserDefault    string = "root"
	MaxFileSizeDefault int    = 1e10

	DateLayout     string = "2006-01-02"
	DateTimeLayout string = "2006-01-02 15:04:05"
	SlotTimeFormat string = "20060102150405"
)

type Slot struct {
	Host    string
	Table   string
	SlotBeg time.Time
	SlotEnd time.Time
}

type ArchiveParams struct {
	Hosts            []string
	Port             int
	User             string
	Password         string
	Database         string
	Tables           []string
	Begin            string
	End              string
	Format           string
	Suffix           string
	MaxFileSize      int
	Cluster          string
	PattInfo         map[string][]string // table -> Date/DateTime column, type
	Dirs             []string
	TmpTables        []string
	Slots            []Slot
	EstSize          uint64
	SshUser          string
	SshPassword      string
	SshPort          int
	Needsudo         bool
	AuthenticateType int
}

var (
	tryDateIntervals     = []string{"1 year", "1 month", "1 week", "1 day"}
	tryDateTimeIntervals = []string{"1 year", "1 month", "1 week", "1 day", "4 hour", "1 hour"}
)

func NewArchiveParams(hosts []string, conf model.CKManClickHouseConfig, req model.ArchiveTableReq) ArchiveParams {
	params := ArchiveParams{
		Hosts:            hosts,
		Port:             conf.Port,
		User:             conf.User,
		Password:         conf.Password,
		Database:         req.Database,
		Tables:           req.Tables,
		Begin:            req.Begin,
		End:              req.End,
		Format:           req.Format,
		Suffix:           "." + strings.ToLower(req.Format),
		MaxFileSize:      req.MaxFileSize,
		Cluster:          conf.Cluster,
		SshUser:          conf.SshUser,
		SshPassword:      conf.SshPassword,
		SshPort:          conf.SshPort,
		AuthenticateType: conf.AuthenticateType,
		Needsudo:         conf.NeedSudo,
	}
	if params.MaxFileSize == 0 {
		params.MaxFileSize = MaxFileSizeDefault
	}
	if params.Database == "" {
		params.Database = model.ClickHouseDefaultDB
	}
	return params
}

func (p *ArchiveParams) InitConns() (err error) {
	for _, host := range p.Hosts {
		if len(host) == 0 {
			continue
		}
		_, err = common.ConnectClickHouse(host, p.Port, p.Database, p.User, p.Password)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

func (p *ArchiveParams) GetSortingInfo() (err error) {
	p.PattInfo = make(map[string][]string)
	for _, table := range p.Tables {
		var name, typ string
		for _, host := range p.Hosts {
			conn := common.GetConnection(host)
			if conn == nil {
				return fmt.Errorf("can't get connection: %s", host)
			}
			var rows driver.Rows
			// column type includes Date, Date32, DateTime, DateTime64, DateTime64(3), DateTime64(3, 'Asia/Istanbul'), ...
			query := fmt.Sprintf("SELECT name, type FROM system.columns WHERE database='%s' AND table='%s' AND is_in_partition_key=1 AND type like 'Date%%'", p.Database, table)
			log.Logger.Infof("host %s: query: %s", host, query)
			if rows, err = conn.Query(context.Background(), query); err != nil {
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
				p.PattInfo[table] = []string{name, typ}
			}
			if name != "" {
				break
			}
		}
	}
	return
}

func (p *ArchiveParams) GetAllSlots() error {
	var lastErr error
	var wg sync.WaitGroup
	for i := 0; i < len(p.Hosts); i++ {
		host := p.Hosts[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			var slots []time.Time
			for _, table := range p.Tables {
				if slots, err = p.GetSlots(host, table); err != nil {
					log.Logger.Errorf("host %s: got error %+v", host, err)
					lastErr = err
					return
				}
				for i := 0; i < len(slots); i++ {
					var slotBeg, slotEnd time.Time
					slotBeg = slots[i]
					if i != len(slots)-1 {
						slotEnd = slots[i+1]
					} else {
						if slotEnd, err = time.Parse(DateLayout, p.End); err != nil {
							log.Logger.Errorf("BUG: failed to parse %s, layout %s", p.End, DateLayout)
							lastErr = err
							return
						}
					}
					slot := Slot{
						Host:    host,
						Table:   table,
						SlotBeg: slotBeg,
						SlotEnd: slotEnd,
					}
					p.Slots = append(p.Slots, slot)
				}
			}
		}()
	}
	wg.Wait()
	return lastErr
}

// https://www.slideshare.net/databricks/the-parquet-format-and-performance-optimization-opportunities
// P22 sorted data helps to predicate pushdown
// P25 avoid many small files
// P27 avoid few huge files - 1GB?
func (p *ArchiveParams) GetSlots(host, table string) (slots []time.Time, err error) {
	var sizePerRow float64
	var rowsCnt uint64
	var compressed uint64
	// get size-per-row
	if rowsCnt, err = p.SelectUint64(host, fmt.Sprintf("SELECT count() FROM %s", table)); err != nil {
		return
	}
	if rowsCnt == 0 {
		return
	}
	if compressed, err = p.SelectUint64(host, fmt.Sprintf("SELECT sum(data_compressed_bytes) AS compressed FROM system.parts WHERE database='%s' AND table='%s' AND active=1", p.Database, table)); err != nil {
		return
	}
	sizePerRow = float64(compressed) / float64(rowsCnt)

	maxRowsCnt := uint64(float64(p.MaxFileSize) / sizePerRow)
	slots = make([]time.Time, 0)
	var slot time.Time
	conn := common.GetConnection(host)
	if conn == nil {
		log.Logger.Errorf("can't get connection:%s", host)
		return
	}

	colName := p.PattInfo[table][0]
	colType := p.PattInfo[table][1]
	var totalRowsCnt uint64
	if totalRowsCnt, err = p.SelectUint64(host, fmt.Sprintf("SELECT count() FROM %s WHERE `%s`>=%s AND `%s`<%s", table, colName, formatDate(p.Begin, colType), colName, formatDate(p.End, colType))); err != nil {
		return
	}
	tblEstSize := totalRowsCnt * uint64(sizePerRow)
	log.Logger.Infof("host %s: totol rows to export: %d, estimated size (in bytes): %d", host, totalRowsCnt, tblEstSize)
	atomic.AddUint64(&p.EstSize, tblEstSize)

	sqlTmpl3 := "SELECT toStartOfInterval(`%s`, INTERVAL %s) AS slot, count() FROM %s WHERE `%s`>=%s AND `%s`<%s GROUP BY slot ORDER BY slot"
	var tryIntervals []string
	if colType == "Date" {
		tryIntervals = tryDateIntervals
	} else {
		tryIntervals = tryDateTimeIntervals
	}
	for i, interval := range tryIntervals {
		slots = slots[:0]
		var rows1 driver.Rows
		query1 := fmt.Sprintf(sqlTmpl3, colName, interval, table, colName, formatDate(p.Begin, colType), colName, formatDate(p.End, colType))
		log.Logger.Infof("host %s: query: %s", host, query1)
		if rows1, err = conn.Query(context.Background(), query1); err != nil {
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

func (p *ArchiveParams) ExportSlot(host, table string, seq int, slotBeg, slotEnd time.Time, engines []string) error {
	colName := p.PattInfo[table][0]
	colType := p.PattInfo[table][1]
	var wg sync.WaitGroup
	var lastErr error
	wg.Add(1)
	_ = common.Pool.Submit(func() {
		defer wg.Done()
		tmpTbl := "archive_" + table + "_" + slotBeg.Format(SlotTimeFormat)
		p.TmpTables = append(p.TmpTables, tmpTbl)
		for _, engine := range engines {
			queries := []string{
				fmt.Sprintf("DROP TABLE IF EXISTS %s", tmpTbl),
				fmt.Sprintf("CREATE TABLE %s AS %s ENGINE=%s", tmpTbl, table, engine),
				fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE `%s`>=%s AND `%s`<%s", tmpTbl, table, colName, formatTimestamp(slotBeg, colType), colName, formatTimestamp(slotEnd, colType)),
			}
			conn := common.GetConnection(host)
			if conn == nil {
				lastErr = fmt.Errorf("can't get connection: %s", host)
				return
			}
			for _, query := range queries {
				log.Logger.Debugf("host %s, table %s, slot %d, query: %s", host, table, slotBeg, query)
				if err := conn.Exec(context.Background(), query); err != nil {
					lastErr = errors.Wrap(err, host)
					return
				}
			}
		}
	})
	wg.Wait()
	return lastErr
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

func (a *ArchiveParams) SelectUint64(host, query string) (res uint64, err error) {
	conn := common.GetConnection(host)
	if conn == nil {
		log.Logger.Errorf("can't get connection:%s", host)
		return
	}
	var rows driver.Rows
	log.Logger.Infof("host %s: query: %s", host, query)
	if rows, err = conn.Query(context.Background(), query); err != nil {
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

type Archive interface {
	Normalize(params ArchiveParams, req model.ArchiveTableReq) error
	Init() error // check and connect
	Clear() error
	Engine(fp string) string
	Export() error
	Done(fp string)
}

type ArchiveFactory interface {
	Create() Archive
}

func GetSuitableArchiveAdpt(target string) Archive {
	switch target {
	case model.ArchiveTargetHDFS:
		return HdfsFactory{}.Create()
	case model.ArchiveTargetLocal:
		return LocalFactory{}.Create()
	case model.ArchiveTargetS3:
		return S3Factory{}.Create()
	default:
		return LocalFactory{}.Create()
	}
}
