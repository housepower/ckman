package common

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ColumnType struct {
	protocol clickhouse.Protocol
	ctp1     *sql.ColumnType
	ctp2     driver.ColumnType
}

func (c *ColumnType) ScanType() reflect.Type {
	if c.protocol == clickhouse.HTTP {
		return c.ctp1.ScanType()
	} else {
		return c.ctp2.ScanType()
	}
}

type Rows struct {
	protocol clickhouse.Protocol
	rs1      *sql.Rows
	rs2      driver.Rows
}

func (r *Rows) Close() error {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Close()
	} else {
		return r.rs2.Close()
	}
}

func (r *Rows) Columns() ([]string, error) {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Columns()
	} else {
		return r.rs2.Columns(), nil
	}
}

func (r *Rows) Next() bool {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Next()
	} else {
		return r.rs2.Next()
	}
}

func (r *Rows) Scan(dest ...any) error {
	if r.protocol == clickhouse.HTTP {
		return r.rs1.Scan(dest...)
	} else {
		return r.rs2.Scan(dest...)
	}
}

func (r *Rows) ColumnTypes() ([]*ColumnType, error) {
	var ctps []*ColumnType
	var err error
	if r.protocol == clickhouse.HTTP {
		ctp1s, err1 := r.rs1.ColumnTypes()
		err = err1
		for _, ctp1 := range ctp1s {
			ctps = append(ctps, &ColumnType{
				protocol: r.protocol,
				ctp1:     ctp1,
			})
		}
	} else {
		ctps2 := r.rs2.ColumnTypes()
		for _, ctp2 := range ctps2 {
			ctps = append(ctps, &ColumnType{
				protocol: r.protocol,
				ctp2:     ctp2,
			})
		}
	}
	return ctps, err
}

type Conn struct {
	protocol clickhouse.Protocol
	c        driver.Conn
	db       *sql.DB
	ctx      context.Context
}

func (c *Conn) Query(query string, args ...any) (*Rows, error) {
	var rs Rows
	rs.protocol = c.protocol
	if c.protocol == clickhouse.HTTP {
		rows, err := c.db.Query(query, args...)
		if err != nil {
			return &rs, err
		} else {
			rs.rs1 = rows
		}
	} else {
		rows, err := c.c.Query(c.ctx, query, args...)
		if err != nil {
			return &rs, err
		} else {
			rs.rs2 = rows
		}
	}
	return &rs, nil
}

func (c *Conn) Exec(query string, args ...any) error {
	if c.protocol == clickhouse.HTTP {
		_, err := c.db.Exec(query, args...)
		return err
	} else {
		return c.c.Exec(c.ctx, query, args...)
	}
}

func (c *Conn) Ping() error {
	if c.protocol == clickhouse.HTTP {
		return c.db.Ping()
	} else {
		return c.c.Ping(c.ctx)
	}
}

func (c *Conn) AsyncInsert(query string, wait bool) error {
	if c.protocol == clickhouse.HTTP {
		return errors.New("DO NOT SUPPORT THIS FUNCTION")
	} else {
		return c.c.AsyncInsert(c.ctx, query, wait)
	}
}

func (c *Conn) Close() error {
	if c.protocol == clickhouse.HTTP {
		return c.db.Close()
	} else {
		return c.c.Close()
	}
}
