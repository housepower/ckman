package clickhouse

import (
	"fmt"

	"github.com/housepower/ckman/common"
	"github.com/housepower/ckman/log"
	"github.com/housepower/ckman/model"
	"github.com/pkg/errors"
)

type PurgerRange struct {
	Hosts    []string
	Database string
	Tables   []string
	Begin    string
	End      string
	ConnOpt  model.ConnetOption
}

func NewPurgerRange(hosts []string, database string, begin string, end string, opt model.ConnetOption) *PurgerRange {
	return &PurgerRange{
		Hosts:    hosts,
		Database: database,
		Begin:    begin,
		End:      end,
		ConnOpt:  opt,
	}
}

func (p *PurgerRange) InitConns() (err error) {
	for _, host := range p.Hosts {
		if len(host) == 0 {
			continue
		}
		_, err = common.ConnectClickHouse(host, p.Database, p.ConnOpt)
		if err != nil {
			return
		}
		log.Logger.Infof("initialized clickhouse connection to %s", host)
	}
	return
}

// purgeTable purges specified time range
func (p *PurgerRange) PurgeTable(table string) (err error) {
	var dateExpr []string
	// ensure the table is partitioned by a Date/DateTime column
	for _, host := range p.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			return fmt.Errorf("can't get connection:%s", host)
		}
		var rows *common.Rows
		query := fmt.Sprintf("SELECT count(), max(max_date)!='1970-01-01', max(toDate(max_time))!='1970-01-01' FROM system.parts WHERE database='%s' AND table='%s'", p.Database, table)
		log.Logger.Infof("host %s: query: %s", host, query)
		if rows, err = conn.Query(query); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		defer rows.Close()
		rows.Next()
		var i1 uint64
		var i2, i3 uint8
		if err = rows.Scan(&i1, &i2, &i3); err != nil {
			err = errors.Wrapf(err, "")
			return
		}
		if i1 == 0 {
			continue
		} else if i2 == 0 && i3 == 0 {
			err = errors.Errorf("table %s.%s is not partitioned by a Date/DateTime column", p.Database, table)
			return
		} else if i2 == 1 {
			dateExpr = []string{"min_date", "max_date"}
		} else {
			dateExpr = []string{"toDate(min_time)", "toDate(max_time)"}
		}
		break
	}
	if len(dateExpr) != 2 {
		log.Logger.Infof("table %s.%s doesn't exist, or is empty", p.Database, table)
		return
	}

	// ensure no partition runs across the time range boundary
	for _, host := range p.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			log.Logger.Errorf("can't get connection: %s", host)
			return
		}
		var rows *common.Rows
		query := fmt.Sprintf("SELECT partition FROM (SELECT partition, countIf(%s>='%s' AND %s<'%s') AS c1, countIf(%s<'%s' OR %s>='%s') AS c2 FROM system.parts WHERE database='%s' AND table='%s' GROUP BY partition HAVING c1!=0 AND c2!=0)", dateExpr[0], p.Begin, dateExpr[1], p.End, dateExpr[0], p.Begin, dateExpr[1], p.End, p.Database, table)
		log.Logger.Infof("host %s: query: %s", host, query)
		if rows, err = conn.Query(query); err != nil {
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
			err = errors.Errorf("table %s.%s partition %s runs across the time range boundary", p.Database, table, patt)
			return
		}
	}

	// purge partitions
	for _, host := range p.Hosts {
		conn := common.GetConnection(host)
		if conn == nil {
			log.Logger.Errorf("can't get connection: %s", host)
			return
		}
		var rows *common.Rows
		query := fmt.Sprintf("SELECT DISTINCT partition FROM system.parts WHERE database='%s' AND table='%s' AND %s>='%s' AND %s<'%s' ORDER BY partition;", p.Database, table, dateExpr[0], p.Begin, dateExpr[1], p.End)
		log.Logger.Infof("host %s: query: %s", host, query)
		if rows, err = conn.Query(query); err != nil {
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
			query := fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION '%s'", p.Database, table, patt)
			log.Logger.Infof("host %s: query: %s", host, query)
			if err = conn.Exec(query); err != nil {
				err = errors.Wrapf(err, "")
				return
			}
		}
	}
	return
}
